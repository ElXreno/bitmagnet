package metainforequester

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/netip"
	"os"
	"syscall"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/mse"
	"github.com/anacrolix/torrent/peer_protocol"
	"github.com/anacrolix/utp"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/clientid"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/prometheus/client_golang/prometheus"
)

type Requester interface {
	Request(context.Context, protocol.ID, netip.AddrPort) (Response, error)
}

type requester struct {
	clientID protocol.ID
	timeout  time.Duration
	dialer   *net.Dialer
	utp      *utp.Socket
}

// PeerError is a fetch that failed after the peer said who it is.
type PeerError struct {
	PeerID protocol.ID
	Err    error
}

func (e *PeerError) Error() string {
	return e.Err.Error()
}

func (e *PeerError) Unwrap() error {
	return e.Err
}

// ErrClosedNetworkPeer marks a failed fetch from a peer of a network that does not serve meta info.
var ErrClosedNetworkPeer = errors.New("peer of a closed network")

// ErrSelfPeer marks a handshake that carried our own peer id.
var ErrSelfPeer = errors.New("peer is ourselves")

type utpFallbackKey struct{}

// WithUTPFallback lets requests made with the returned context retry over uTP when the TCP dial times out.
func WithUTPFallback(ctx context.Context) context.Context {
	return context.WithValue(ctx, utpFallbackKey{}, true)
}

const utpDialTimeout = 2 * time.Second

type ExtensionBit uint

// https://www.bittorrent.org/beps/bep_0004.html
// https://wiki.theory.org/BitTorrentSpecification.html#Reserved_Bytes
const (
	// ExtensionBitDht http://www.bittorrent.org/beps/bep_0005.html
	ExtensionBitDht = 0
	// ExtensionBitFast http://www.bittorrent.org/beps/bep_0006.html
	ExtensionBitFast = 2
	// ExtensionBitV2 "Hybrid torrent legacy to v2 upgrade"
	ExtensionBitV2                           = 7
	ExtensionBitAzureusExtensionNegotiation1 = 16
	ExtensionBitAzureusExtensionNegotiation2 = 17
	// ExtensionBitLtep LibTorrent Extension Protocol, http://www.bittorrent.org/beps/bep_0010.html
	ExtensionBitLtep = 20
	// ExtensionBitLocationAwareProtocol https://wiki.theory.org/BitTorrent_Location-aware_Protocol_1
	ExtensionBitLocationAwareProtocol = 43
	// ExtensionBitAzureusMessagingProtocol https://www.bittorrent.org/beps/bep_0004.html
	ExtensionBitAzureusMessagingProtocol = 63
)

type PeerExtensionBits [8]byte

func NewPeerExtensionBits(bits ...ExtensionBit) (ret PeerExtensionBits) {
	for _, b := range bits {
		ret = ret.WithBit(b, true)
	}

	return
}

func (pex PeerExtensionBits) WithBit(bit ExtensionBit, on bool) PeerExtensionBits {
	if on {
		pex[7-bit/8] |= 1 << (bit % 8)
	} else {
		pex[7-bit/8] &^= 1 << (bit % 8)
	}

	return pex
}

func (pex PeerExtensionBits) GetBit(bit ExtensionBit) bool {
	return pex[7-bit/8]&(1<<(bit%8)) != 0
}

type HandshakeInfo struct {
	PeerID            protocol.ID
	PeerExtensionBits PeerExtensionBits
}

type Response struct {
	HandshakeInfo
	Info      metainfo.Info
	Identity  metainfo.Identity
	Encrypted bool
	Transport string
}

var errPlainHandshakeClosed = errors.New("peer closed the connection during the plaintext handshake")

func (r requester) Request(ctx context.Context, infoHash protocol.ID, addr netip.AddrPort) (Response, error) {
	res, err := r.request(ctx, infoHash, addr, false)
	if err == nil || !errors.Is(err, errPlainHandshakeClosed) {
		return res, err
	}

	res, mseErr := r.request(ctx, infoHash, addr, true)
	if mseErr != nil {
		return Response{}, fmt.Errorf("mse retry: %w", mseErr)
	}

	res.Encrypted = true

	return res, nil
}

func (r requester) request(
	ctx context.Context,
	infoHash protocol.ID,
	addr netip.AddrPort,
	encrypted bool,
) (Response, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	var conn net.Conn

	transport := "tcp"

	tcpConn, connErr := r.connect(timeoutCtx, addr)

	switch {
	case connErr == nil:
		conn = tcpConn
	case r.utp != nil && ctx.Value(utpFallbackKey{}) != nil && addr.Addr().Is4() && isDialTimeout(connErr):
		utpCtx, utpCancel := context.WithTimeout(timeoutCtx, utpDialTimeout)
		utpConn, utpErr := r.utp.DialContext(utpCtx, "udp4", addr.String())

		utpCancel()

		if utpErr != nil {
			return Response{}, fmt.Errorf("utp dial: %w", utpErr)
		}

		if deadline, ok := timeoutCtx.Deadline(); ok {
			_ = utpConn.SetDeadline(deadline)
		}

		conn = utpConn
		transport = "utp"
	default:
		return Response{}, connErr
	}

	defer func() {
		_ = conn.Close()
	}()

	var rw io.ReadWriter = conn

	if encrypted {
		encRw, _, mseErr := mse.InitiateHandshake(conn, infoHash[:], nil, mse.AllSupportedCrypto)
		if mseErr != nil {
			return Response{}, fmt.Errorf("mse handshake: %w", mseErr)
		}

		rw = encRw
	}

	hsInfo, btHandshakeErr := btHandshake(rw, infoHash, r.clientID)
	if btHandshakeErr != nil {
		if !encrypted && (errors.Is(btHandshakeErr, io.EOF) || errors.Is(btHandshakeErr, io.ErrUnexpectedEOF) ||
			errors.Is(btHandshakeErr, syscall.ECONNRESET)) {
			return Response{}, errors.Join(errPlainHandshakeClosed, btHandshakeErr)
		}

		return Response{}, btHandshakeErr
	}

	if hsInfo.PeerID == r.clientID {
		return Response{}, ErrSelfPeer
	}

	peerClient := clientid.Peer([20]byte(hsInfo.PeerID))
	peerVersion := ""
	outcome := func(stage string) {
		peerClientTotal.With(prometheus.Labels{"client": peerClient, "stage": stage}).Inc()
		dumpPeerID(addr, infoHash, hsInfo, peerClient, stage, peerVersion)
	}
	failed := func(stage string, err error) (Response, error) {
		outcome(stage)

		err = &PeerError{PeerID: hsInfo.PeerID, Err: err}

		if clientid.ClosedNetwork(peerClient) {
			err = errors.Join(ErrClosedNetworkPeer, err)
		}

		return Response{}, err
	}

	peerClientTotal.With(prometheus.Labels{"client": peerClient, "stage": "handshake"}).Inc()

	handshake, exHandshakeErr := readExHandshake(rw)
	if exHandshakeErr != nil {
		return failed("no_ext_handshake", exHandshakeErr)
	}

	peerVersion = handshake.V

	metadataSize, utMetadata, offerErr := handshake.metadata()
	if offerErr != nil {
		if errors.Is(offerErr, errNoMetadataSize) {
			return failed("no_metadata_size", offerErr)
		}

		return failed("no_ut_metadata", offerErr)
	}

	if requestAllPiecesErr := requestAllPieces(rw, metadataSize, utMetadata); requestAllPiecesErr != nil {
		return failed("pieces_failed", requestAllPiecesErr)
	}

	pieces, readAllPiecesErr := readAllPieces(rw, metadataSize)
	if readAllPiecesErr != nil {
		return failed("pieces_failed", readAllPiecesErr)
	}

	if bencodeUnsafe(pieces, incomingMaxDepth) {
		return failed("bad_metainfo", errIncomingBencode)
	}

	parsed, identity, parseErr := metainfo.ParseMetaInfo(infoHash, pieces)
	if parseErr != nil {
		return failed("bad_metainfo", parseErr)
	}

	outcome("metainfo")

	return Response{
		HandshakeInfo: hsInfo,
		Info:          parsed,
		Identity:      identity,
		Transport:     transport,
	}, nil
}

func isDialTimeout(err error) bool {
	var netErr net.Error

	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout())
}

func (r requester) connect(ctx context.Context, addr netip.AddrPort) (conn *net.TCPConn, err error) {
	network := "tcp4"
	if addr.Addr().Is6() && !addr.Addr().Is4In6() {
		network = "tcp6"
	}

	c, dialErr := r.dialer.DialContext(ctx, network, addr.String())
	if dialErr != nil {
		err = dialErr
		return
	}

	tcpConn := c.(*net.TCPConn)
	closeConn := func() {
		_ = tcpConn.Close()
	}
	// If sec == 0, operating system discards any unsent or unacknowledged data [after Close() has been called].
	if setLingerErr := tcpConn.SetLinger(0); setLingerErr != nil {
		err = setLingerErr

		closeConn()

		return
	}

	deadline, ok := ctx.Deadline()
	if ok {
		if setDeadlineErr := tcpConn.SetDeadline(deadline); setDeadlineErr != nil {
			err = setDeadlineErr

			closeConn()

			return
		}
	}

	return tcpConn, nil
}

var pipelinedHandshake = os.Getenv("BITMAGNET_PIPELINED_HANDSHAKE") == "1"

const extendedHandshake = "\x00\x00\x00\x1a\x14\x00d1:md11:ut_metadatai1eee"

var myExBits = NewPeerExtensionBits(ExtensionBitDht, ExtensionBitLtep)

func btHandshake(rw io.ReadWriter, infoHash protocol.ID, clientID protocol.ID) (HandshakeInfo, error) {
	handshakeBytes := make([]byte, 0, 68+len(extendedHandshake))
	handshakeBytes = append(handshakeBytes, peer_protocol.Protocol...)
	handshakeBytes = append(handshakeBytes, myExBits[:]...)
	handshakeBytes = append(handshakeBytes, infoHash[:]...)
	handshakeBytes = append(handshakeBytes, clientID[:]...)
	if pipelinedHandshake {
		handshakeBytes = append(handshakeBytes, extendedHandshake...)
	}

	if n, hsErr := rw.Write(handshakeBytes); hsErr != nil {
		return HandshakeInfo{}, hsErr
	} else if n != len(handshakeBytes) {
		return HandshakeInfo{}, io.ErrShortWrite
	}

	handshakeResponse := make([]byte, 68)
	if n, hsErr := io.ReadFull(rw, handshakeResponse); hsErr != nil {
		return HandshakeInfo{},
			fmt.Errorf("failed to read all handshake bytes (%d): %w / %s", n, hsErr, infoHash.String())
	}

	if !bytes.HasPrefix(handshakeResponse, []byte(peer_protocol.Protocol)) {
		return HandshakeInfo{}, errors.New("invalid handshake response received")
	}

	var peerExBits PeerExtensionBits

	copy(peerExBits[:], handshakeResponse[20:28])

	if !peerExBits.GetBit(ExtensionBitLtep) {
		return HandshakeInfo{}, errors.New("peer does not support the extension protocol")
	}

	var resHash protocol.ID

	copy(resHash[:], handshakeResponse[28:48])

	if resHash != infoHash {
		return HandshakeInfo{}, errors.New("infohash mismatch")
	}

	var resPeerID protocol.ID

	copy(resPeerID[:], handshakeResponse[48:68])

	return HandshakeInfo{
		PeerID:            resPeerID,
		PeerExtensionBits: peerExBits,
	}, nil
}

type rootDict struct {
	M            mDict  `bencode:"m"`
	MetadataSize int    `bencode:"metadata_size"`
	V            string `bencode:"v,ignore_unmarshal_type_error"`
}

var (
	errNoMetadataSize = errors.New("metadata too big or its size is less than or equal zero")
	errNoUTMetadata   = errors.New("ut_metadata is not an uint8")
)

func (d *rootDict) metadata() (metadataSize uint, utMetadata uint8, err error) {
	if 0 >= d.MetadataSize || d.MetadataSize >= maxMetadataSize {
		return 0, 0, errNoMetadataSize
	}

	if 0 >= d.M.UTMetadata || d.M.UTMetadata >= 255 {
		return 0, 0, errNoUTMetadata
	}

	return uint(d.MetadataSize), uint8(d.M.UTMetadata), nil
}

type mDict struct {
	UTMetadata  int `bencode:"ut_metadata"`
	UTPex       int `bencode:"ut_pex"`
	UTHolepunch int `bencode:"ut_holepunch"`
}

type extDict struct {
	MsgType int `bencode:"msg_type"`
	Piece   int `bencode:"piece"`
}

const (
	maxMetadataSize   = 10 * 1024 * 1024
	metadataPieceSize = 16 * 1024
)

func readExHandshake(rw io.ReadWriter) (*rootDict, error) {
	if !pipelinedHandshake {
		if _, writeErr := rw.Write([]byte(extendedHandshake)); writeErr != nil {
			return nil, writeErr
		}
	}

	rRootDict, readErr := readExHandshakeDict(rw)
	if readErr != nil {
		return nil, readErr
	}

	countPeerExtensions("", rRootDict)

	return rRootDict, nil
}

func readExHandshakeDict(r io.Reader) (*rootDict, error) {
	rExMessage, readErr := readExMessage(r)
	if readErr != nil {
		return nil, readErr
	}
	// Extension Handshake has the Extension Message id = 0x00
	if rExMessage[1] != 0 {
		return nil, errors.New("first extension message is not an extension handshake")
	}

	if bencodeUnsafe(rExMessage[2:], incomingMaxDepth) {
		return nil, errIncomingBencode
	}

	rRootDict := new(rootDict)
	if unmarshalErr := bencode.Unmarshal(rExMessage[2:], rRootDict); unmarshalErr != nil {
		return nil, unmarshalErr
	}

	return rRootDict, nil
}

func requestAllPieces(w io.Writer, metadataSize uint, utMetadata uint8) error {
	nPieces := int(math.Ceil(float64(metadataSize) / math.Pow(2, 14)))
	for piece := range nPieces {
		extDictDump, err := bencode.Marshal(extDict{
			MsgType: 0,
			Piece:   piece,
		})
		if err != nil { // ASSERT
			panic(err)
		}

		if _, writeErr := fmt.Fprintf(w,
			"%s\x14%s%s",
			uintToBigEndian4(uint(2+len(extDictDump))),
			[]byte{utMetadata},
			extDictDump); writeErr != nil {
			return writeErr
		}
	}

	return nil
}

func uintToBigEndian4(i uint) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(i))

	return b
}

func readAllPieces(r io.Reader, metadataSize uint) ([]byte, error) {
	var metadataBytes []byte

	receivedSize := uint(0)
	for receivedSize < metadataSize {
		rUmMessage, err := readUmMessage(r)
		if err != nil {
			return nil, err
		}
		// run TestDecoder() function in leech_test.go in case you have any doubts.
		if bencodeUnsafe(rUmMessage[2:], incomingMaxDepth) {
			return nil, errIncomingBencode
		}

		rMessageBuf := bytes.NewBuffer(rUmMessage[2:])
		rExtDict := new(extDict)

		if decodeErr := bencode.NewDecoder(rMessageBuf).Decode(rExtDict); decodeErr != nil {
			return nil, decodeErr
		}

		if rExtDict.MsgType == 2 { // reject
			return nil, errors.New("remote peer rejected sending metadataBytes")
		}

		if rExtDict.MsgType == 1 { // data
			// Get the unread bytes!
			metadataPiece := rMessageBuf.Bytes()
			// BEP 9 explicitly states:
			//   > If the piece is the last piece of the metadata, it may be less than 16kiB. If
			//   > it is not the last piece of the metadata, it MUST be 16kiB.
			//
			// Hence...
			//   ... if the length of metadataPiece is more than 16kiB, we err.
			if len(metadataPiece) > 16*1024 {
				return nil, errors.New("metadataPiece > 16kiB")
			}

			receivedSize += uint(len(metadataPiece))
			// ... if the length of @metadataPiece is less than 16kiB AND metadataBytes is NOT
			// complete then we err.
			if len(metadataPiece) < 16*1024 && receivedSize != metadataSize {
				return nil, errors.New("metadataPiece < 16 kiB but incomplete")
			}

			if receivedSize > metadataSize {
				return nil, errors.New("receivedSize > metadataSize")
			}

			piece := rExtDict.Piece
			if piece < 0 || uint(piece) >= (metadataSize+metadataPieceSize-1)/metadataPieceSize {
				return nil, errors.New("piece index out of range")
			}

			start := uint(piece) * metadataPieceSize

			end := start + uint(len(metadataPiece))
			if end > metadataSize {
				return nil, errors.New("piece exceeds the metadata size")
			}

			metadataBytes = grownTo(metadataBytes, end)
			copy(metadataBytes[start:], metadataPiece)
		}
	}

	return grownTo(metadataBytes, metadataSize), nil
}

// grownTo zero-extends b to at least size bytes, so a meta info takes memory as its pieces arrive.
func grownTo(b []byte, size uint) []byte {
	if uint(len(b)) >= size {
		return b
	}

	return append(b, make([]byte, size-uint(len(b)))...)
}

func readExMessage(r io.Reader) ([]byte, error) {
	for {
		rMessage, err := readMessage(r)
		if err != nil {
			return nil, err
		}
		// Every extension message has at least 2 bytes.
		if len(rMessage) < 2 {
			continue
		}
		// We are interested only in extension messages, whose first byte is always 20
		if rMessage[0] == byte(peer_protocol.Extended) {
			return rMessage, nil
		}
	}
}

// readUmMessage returns an ut_metadata extension message, sans the first 4 bytes indicating its
// length.
//
// It will IGNORE all non-"ut_metadata extension" messages!
func readUmMessage(r io.Reader) ([]byte, error) {
	for {
		rExMessage, err := readExMessage(r)
		if err != nil {
			return nil, err
		}

		if rExMessage[1] == 0x01 {
			return rExMessage, nil
		}
	}
}

// readMessage returns a BitTorrent message, sans the first 4 bytes indicating its length.
func readMessage(r io.Reader) ([]byte, error) {
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBytes); err != nil {
		return nil, err
	}

	length := uint(binary.BigEndian.Uint32(lengthBytes))

	// Some malicious/faulty peers say that they are sending a very long
	// message, and hence causing us to run out of memory.
	// This is a crude check that does not let it happen (i.e. boundary can probably be
	// tightened a lot more.)
	if length > maxMetadataSize {
		return nil, errors.New("message is longer than max allowed metadata size")
	}

	messageBytes := make([]byte, length)
	if _, err := io.ReadFull(r, messageBytes); err != nil {
		return nil, err
	}

	return messageBytes, nil
}
