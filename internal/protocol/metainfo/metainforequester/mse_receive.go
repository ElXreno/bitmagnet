package metainforequester

import (
	"bufio"
	"bytes"
	"crypto/rc4"  //nolint:gosec
	"crypto/sha1" //nolint:gosec
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

const (
	mseKeyLen        = 96
	mseMaxPad        = 512
	mseCryptoPlain   = 1
	mseCryptoRC4     = 2
	mseDiscardedKeys = 1024
)

var (
	msePrime, _ = new(big.Int).SetString(
		"FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DD"+
			"EF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A63A36210000000000090563", 16)
	mseGenerator = big.NewInt(2)

	errMSE = errors.New("message stream encryption handshake failed")
)

// MSEKey is all an encrypted handshake reveals about its info hash.
type MSEKey [sha1.Size]byte

func MSEKeyOf(hash protocol.ID) MSEKey {
	return mseHash([]byte("req2"), hash[:])
}

// MSELookup finds the info hash of an encrypted handshake.
type MSELookup func(MSEKey) (protocol.ID, bool)

// KeysLookup searches a short list of hashes one by one.
func KeysLookup(keys func() []protocol.ID) MSELookup {
	return func(wanted MSEKey) (protocol.ID, bool) {
		for _, hash := range keys() {
			if MSEKeyOf(hash) == wanted {
				return hash, true
			}
		}

		return protocol.ID{}, false
	}
}

func mseHash(parts ...[]byte) (sum [sha1.Size]byte) {
	h := sha1.New() //nolint:gosec
	for _, part := range parts {
		_, _ = h.Write(part)
	}

	h.Sum(sum[:0])

	return sum
}

func mseCipher(label string, secret []byte, hash protocol.ID) *rc4.Cipher {
	key := mseHash([]byte(label), secret, hash[:])
	cipher, _ := rc4.NewCipher(key[:]) //nolint:gosec

	var discarded [mseDiscardedKeys]byte

	cipher.XORKeyStream(discarded[:], discarded[:])

	return cipher
}

type mseReader struct {
	cipher *rc4.Cipher
	r      io.Reader
}

func (m mseReader) Read(p []byte) (int, error) {
	n, err := m.r.Read(p)
	m.cipher.XORKeyStream(p[:n], p[:n])

	return n, err
}

type mseWriter struct {
	cipher *rc4.Cipher
	w      io.Writer
}

func (m mseWriter) Write(p []byte) (int, error) {
	encrypted := make([]byte, len(p))
	m.cipher.XORKeyStream(encrypted, p)

	return m.w.Write(encrypted)
}

// mseReceive answers an encrypted handshake and finds its info hash with one lookup.
func mseReceive(rw io.ReadWriter, lookup MSELookup) (io.ReadWriter, protocol.ID, error) {
	r := bufio.NewReaderSize(rw, mseKeyLen+mseMaxPad+2*sha1.Size)

	var theirs [mseKeyLen]byte
	if _, err := io.ReadFull(r, theirs[:]); err != nil {
		return nil, protocol.ID{}, fmt.Errorf("%w: public key: %w", errMSE, err)
	}

	peer := new(big.Int).SetBytes(theirs[:])
	if peer.Cmp(big.NewInt(1)) <= 0 || peer.Cmp(new(big.Int).Sub(msePrime, big.NewInt(1))) >= 0 {
		return nil, protocol.ID{}, fmt.Errorf("%w: degenerate public key", errMSE)
	}

	keys, err := sharedMSEKeys.get()
	if err != nil {
		return nil, protocol.ID{}, fmt.Errorf("%w: %w", errMSE, err)
	}

	var secret [mseKeyLen]byte

	new(big.Int).Exp(peer, keys.private, msePrime).FillBytes(secret[:])

	if _, err := rw.Write(keys.public[:]); err != nil {
		return nil, protocol.ID{}, fmt.Errorf("%w: %w", errMSE, err)
	}

	marker := mseHash([]byte("req1"), secret[:])
	window := make([]byte, 0, mseMaxPad+sha1.Size)

	for !bytes.HasSuffix(window, marker[:]) {
		if len(window) == cap(window) {
			return nil, protocol.ID{}, fmt.Errorf("%w: no synchronisation", errMSE)
		}

		b, err := r.ReadByte()
		if err != nil {
			return nil, protocol.ID{}, fmt.Errorf("%w: synchronisation: %w", errMSE, err)
		}

		window = append(window, b)
	}

	var key MSEKey
	if _, err := io.ReadFull(r, key[:]); err != nil {
		return nil, protocol.ID{}, fmt.Errorf("%w: key: %w", errMSE, err)
	}

	mask := mseHash([]byte("req3"), secret[:])
	for i := range key {
		key[i] ^= mask[i]
	}

	hash, known := lookup(key)
	if !known {
		return nil, protocol.ID{}, ErrUnknownInfoHash
	}

	incoming := mseReader{cipher: mseCipher("keyA", secret[:], hash), r: r}

	var header [8 + 4 + 2]byte
	if _, err := io.ReadFull(incoming, header[:]); err != nil {
		return nil, protocol.ID{}, fmt.Errorf("%w: header: %w", errMSE, err)
	}

	if [8]byte(header[:8]) != [8]byte{} {
		return nil, protocol.ID{}, fmt.Errorf("%w: wrong verification constant", errMSE)
	}

	provided := binary.BigEndian.Uint32(header[8:12])

	padding := binary.BigEndian.Uint16(header[12:14])
	if padding > mseMaxPad {
		return nil, protocol.ID{}, fmt.Errorf("%w: padding of %d bytes", errMSE, padding)
	}

	if _, err := io.CopyN(io.Discard, incoming, int64(padding)); err != nil {
		return nil, protocol.ID{}, fmt.Errorf("%w: padding: %w", errMSE, err)
	}

	var initialLen [2]byte
	if _, err := io.ReadFull(incoming, initialLen[:]); err != nil {
		return nil, protocol.ID{}, fmt.Errorf("%w: initial payload length: %w", errMSE, err)
	}

	initial := make([]byte, binary.BigEndian.Uint16(initialLen[:]))
	if _, err := io.ReadFull(incoming, initial); err != nil {
		return nil, protocol.ID{}, fmt.Errorf("%w: initial payload: %w", errMSE, err)
	}

	var chosen uint32

	switch {
	case provided&mseCryptoRC4 != 0:
		chosen = mseCryptoRC4
	case provided&mseCryptoPlain != 0:
		chosen = mseCryptoPlain
	default:
		return nil, protocol.ID{}, fmt.Errorf("%w: no supported crypto method in %#x", errMSE, provided)
	}

	outgoing := mseWriter{cipher: mseCipher("keyB", secret[:], hash), w: rw}

	var reply [8 + 4 + 2]byte

	binary.BigEndian.PutUint32(reply[8:12], chosen)

	if _, err := outgoing.Write(reply[:]); err != nil {
		return nil, protocol.ID{}, fmt.Errorf("%w: %w", errMSE, err)
	}

	if chosen == mseCryptoPlain {
		return readWriter{io.MultiReader(bytes.NewReader(initial), r), rw}, hash, nil
	}

	return readWriter{io.MultiReader(bytes.NewReader(initial), incoming), outgoing}, hash, nil
}
