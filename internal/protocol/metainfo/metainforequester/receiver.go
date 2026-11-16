package metainforequester

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/anacrolix/torrent/peer_protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/clientid"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ErrUnknownInfoHash   = errors.New("incoming peer wants an info hash that is not registered")
	ErrIncomingHandshake = errors.New("incoming handshake failed")
	ErrPeerHasNoMetaInfo = errors.New("incoming peer has no meta info")
	ErrIncomingBudget    = errors.New("incoming meta info memory budget is spent")
	ErrFetchedElsewhere  = errors.New("another connection finished this meta info")

	errIncomingMessageTooLong = errors.New("incoming message is longer than max allowed")
	errIncomingBencode        = errors.New("incoming bencode is nested too deep or claims a string it does not hold")
)

const (
	IncomingTimeout = 15 * time.Second

	incomingMaxExtended = metadataPieceSize + 1024
	incomingMaxMessage  = 1 << 20
	incomingMaxDepth    = 32
	// a peer that connects and says nothing must not hold a slot for the whole fetch timeout
	incomingHandshakeTimeout = 3 * time.Second
	incomingBudget           = 256 << 20
	// the protocol string of the peers that call themselves -DL in place of "BitTorrent protocol"
	dialectProtocol = "\x13\x49\x02\x0f\x57\x0d\x73\x66\x0a\x16\x7d\x26\x49\x57\x11\x00\x00\x00\x00\x00"
	incomingParses  = 2
)

type Receiver struct {
	clientID  protocol.ID
	timeout   time.Duration
	handshake time.Duration
	pending   *atomic.Int64
	parsing   chan struct{}
	dialect   bool
	budget    int64
	partials  *partialStore
}

func incomingBudgetFromEnv() int64 {
	if megabytes, err := strconv.Atoi(os.Getenv("BITMAGNET_INBOUND_BUDGET_MB")); err == nil && megabytes > 0 {
		return int64(megabytes) << 20
	}

	return incomingBudget
}

func piecesFailure(err error) string {
	var netErr net.Error

	switch message := err.Error(); {
	case errors.Is(err, ErrIncomingBudget):
		return "budget"
	case errors.Is(err, os.ErrDeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		return "timeout"
	case errors.Is(err, syscall.ECONNRESET):
		return "reset"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "eof"
	case strings.Contains(message, "rejected"):
		return "rejected"
	case strings.Contains(message, "wrong length"):
		return "wrong_length"
	case strings.Contains(message, "duplicate"):
		return "duplicate"
	case strings.Contains(message, "out of range"):
		return "out_of_range"
	case strings.Contains(message, "longer than"):
		return "too_long"
	default:
		return "malformed"
	}
}

func incomingHandshakeFromEnv() time.Duration {
	if ms, err := strconv.Atoi(os.Getenv("BITMAGNET_INBOUND_HANDSHAKE_MS")); err == nil && ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}

	return incomingHandshakeTimeout
}

func NewReceiver(clientID protocol.ID, timeout time.Duration) Receiver {
	r := Receiver{
		clientID:  clientID,
		timeout:   timeout,
		handshake: incomingHandshakeFromEnv(),
		dialect:   os.Getenv("BITMAGNET_INBOUND_DIALECT") == "on",
		budget:    incomingBudgetFromEnv(),
		pending:   new(atomic.Int64),
		parsing:   make(chan struct{}, incomingParses),
	}

	if os.Getenv("BITMAGNET_INBOUND_RESUME") == "on" {
		r.partials = newPartialStore(r.pending, r.budget)
	}

	return r
}

// the file tree decoder copies a subtree once per level above it, so few meta infos are parsed at a time
func (r Receiver) parse(infoHash protocol.ID, encoded []byte) (metainfo.Info, metainfo.Identity, error) {
	r.parsing <- struct{}{}

	defer func() {
		<-r.parsing
	}()

	if bencodeUnsafe(encoded, incomingMaxDepth) {
		return metainfo.Info{}, metainfo.Identity{}, errIncomingBencode
	}

	return metainfo.ParseMetaInfo(infoHash, encoded)
}

func (r Receiver) PeerID() protocol.ID {
	return r.clientID
}

type Incoming struct {
	InfoHash  protocol.ID
	Encrypted bool
	receiver  Receiver
	conn      net.Conn
	rw        io.ReadWriter
	addr      netip.AddrPort
	bits      PeerExtensionBits
	protocol  string
	// Dialect marks a peer that greeted with another protocol string and is answered in kind
	Dialect bool
}

type readWriter struct {
	io.Reader
	io.Writer
}

func (r Receiver) Accept(conn net.Conn, lookup MSELookup) (*Incoming, error) {
	if err := conn.SetDeadline(time.Now().Add(min(r.timeout, r.handshake))); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrIncomingHandshake, err)
	}

	const protocolLen = len(peer_protocol.Protocol)

	var head [protocolLen + 8 + 20]byte

	if _, err := io.ReadFull(conn, head[:protocolLen]); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrIncomingHandshake, err)
	}

	in := &Incoming{receiver: r, conn: conn, rw: conn, protocol: peer_protocol.Protocol}
	rest := head[protocolLen:]

	var encryptedFor *protocol.ID

	if r.dialect && string(head[:protocolLen]) == dialectProtocol {
		in.protocol = dialectProtocol
		in.Dialect = true
	}

	if string(head[:protocolLen]) != in.protocol {
		replayed := readWriter{io.MultiReader(bytes.NewReader(head[:protocolLen]), conn), conn}

		decrypted, keyed, err := mseReceive(replayed, lookup)
		if errors.Is(err, ErrUnknownInfoHash) {
			return nil, err
		}

		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrIncomingHandshake, err)
		}

		in.rw = decrypted
		in.Encrypted = true
		encryptedFor = &keyed
		rest = head[:]
	}

	if _, err := io.ReadFull(in.rw, rest); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrIncomingHandshake, err)
	}

	if string(head[:protocolLen]) != in.protocol {
		return nil, fmt.Errorf("%w: not a BitTorrent handshake", ErrIncomingHandshake)
	}

	copy(in.bits[:], head[protocolLen:protocolLen+8])
	copy(in.InfoHash[:], head[protocolLen+8:])

	if encryptedFor != nil && *encryptedFor != in.InfoHash {
		return nil, fmt.Errorf("%w: handshake for another info hash than its encryption", ErrIncomingHandshake)
	}

	if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		in.addr = tcpAddr.AddrPort()
	}

	return in, nil
}

// resume asks only for the pieces no earlier connection brought and adds its own to the shared store
func (in *Incoming) resume(messages io.Reader, metadataSize uint, utMetadata uint8) ([]byte, error) {
	store := in.receiver.partials

	missing, err := store.missing(in.InfoHash, metadataSize)
	if err != nil {
		return nil, err
	}

	if err := requestPieces(in.rw, missing, utMetadata); err != nil {
		return nil, err
	}

	wanted := make(map[uint]struct{}, len(missing))
	for _, index := range missing {
		wanted[index] = struct{}{}
	}

	own := 0

	for len(wanted) > 0 {
		index, piece, err := readPiece(messages, metadataSize)
		if err != nil {
			return nil, err
		}

		if _, ok := wanted[index]; !ok {
			return nil, errors.New("duplicate metadata piece")
		}

		delete(wanted, index)

		own += len(piece)

		whole, err := store.put(in.InfoHash, metadataSize, index, piece)
		if err != nil {
			return nil, err
		}

		if whole != nil {
			peerBytesTotal.WithLabelValues("resumed").Add(float64(len(whole) - own))

			return whole, nil
		}
	}

	return nil, ErrIncomingBudget
}

func (in *Incoming) Fetch() (Response, error) {
	if err := in.conn.SetDeadline(time.Now().Add(in.receiver.timeout)); err != nil {
		return Response{}, fmt.Errorf("%w: %w", ErrIncomingHandshake, err)
	}

	if !in.bits.GetBit(ExtensionBitLtep) {
		return Response{}, fmt.Errorf("%w: peer does not support the extension protocol", ErrIncomingHandshake)
	}

	reply := make([]byte, 0, 68+len(extendedHandshake))
	reply = append(reply, in.protocol...)
	reply = append(reply, myExBits[:]...)
	reply = append(reply, in.InfoHash[:]...)
	reply = append(reply, in.receiver.clientID[:]...)
	reply = append(reply, extendedHandshake...)

	if _, err := in.rw.Write(reply); err != nil {
		return Response{}, fmt.Errorf("%w: %w", ErrIncomingHandshake, err)
	}

	hsInfo := HandshakeInfo{PeerExtensionBits: in.bits}

	if _, err := io.ReadFull(in.rw, hsInfo.PeerID[:]); err != nil {
		return Response{}, fmt.Errorf("%w: %w", ErrIncomingHandshake, err)
	}

	if hsInfo.PeerID == in.receiver.clientID {
		return Response{}, ErrSelfPeer
	}

	peerClient := clientid.Peer([20]byte(hsInfo.PeerID))
	peerVersion := ""
	counted := &countingReader{r: in.rw}
	outcome := func(stage string) {
		peerClientTotal.With(prometheus.Labels{"client": peerClient, "stage": "inbound_" + stage}).Inc()
		peerBytesTotal.WithLabelValues(stage).Add(float64(counted.n))
		dumpPeerID(in.addr, in.InfoHash, hsInfo, peerClient, "inbound_"+stage, peerVersion)
	}

	peerClientTotal.With(prometheus.Labels{"client": peerClient, "stage": "inbound_handshake"}).Inc()

	if clientid.ClosedNetwork(peerClient) {
		outcome("closed_network")

		return Response{}, ErrClosedNetworkPeer
	}

	messages := &incomingMessages{r: counted}

	handshake, handshakeErr := readExHandshakeDict(messages)
	if handshakeErr != nil {
		outcome("no_ext_handshake")

		return Response{}, handshakeErr
	}

	countPeerExtensions("inbound_", handshake)

	peerVersion = handshake.V

	metadataSize, utMetadata, offerErr := handshake.metadata()
	if offerErr != nil {
		if errors.Is(offerErr, errNoMetadataSize) {
			outcome("no_metadata_size")

			return Response{}, errors.Join(ErrPeerHasNoMetaInfo, offerErr)
		}

		outcome("no_ut_metadata")

		return Response{}, offerErr
	}

	reserved := int64(0)

	defer func() { in.receiver.pending.Add(-reserved) }()

	reserve := func(size uint) error {
		reserved += int64(size)

		if in.receiver.pending.Add(int64(size)) > in.receiver.budget {
			return ErrIncomingBudget
		}

		return nil
	}

	var (
		pieces    []byte
		piecesErr error
	)

	started := time.Now()

	if in.receiver.partials != nil {
		pieces, piecesErr = in.resume(messages, metadataSize, utMetadata)
	} else if piecesErr = requestAllPieces(in.rw, metadataSize, utMetadata); piecesErr == nil {
		pieces, piecesErr = readAllPieces(messages, metadataSize, reserve)
	}

	if errors.Is(piecesErr, ErrFetchedElsewhere) {
		outcome("fetched_elsewhere")

		return Response{}, piecesErr
	}

	if piecesErr != nil {
		if peerIDDumpPath != "" {
			peerVersion = fmt.Sprintf("%.30s size=%d bytes=%d ms=%d %v",
				peerVersion, metadataSize, counted.n, time.Since(started).Milliseconds(), piecesErr)
		}

		outcome("pieces_failed")

		reason := piecesFailure(piecesErr)
		piecesFailedTotal.WithLabelValues(reason, "connections").Inc()
		piecesFailedTotal.WithLabelValues(reason, "bytes").Add(float64(counted.n))

		return Response{}, piecesErr
	}

	if in.receiver.partials != nil {
		reserved += int64(len(pieces))
		in.receiver.pending.Add(int64(len(pieces)))
	}

	parsed, identity, parseErr := in.receiver.parse(in.InfoHash, pieces)
	if parseErr != nil {
		if in.receiver.partials != nil {
			in.receiver.partials.retry(in.InfoHash)
		}

		outcome("bad_metainfo")

		return Response{}, parseErr
	}

	outcome("metainfo")

	return Response{
		HandshakeInfo: hsInfo,
		Info:          parsed,
		Identity:      identity,
		Encrypted:     in.Encrypted,
		Transport:     "tcp",
	}, nil
}

// only bounded extension messages reach the parsers, the rest is skipped unbuffered
type incomingMessages struct {
	r     io.Reader
	frame []byte
}

func (m *incomingMessages) Read(p []byte) (int, error) {
	for len(m.frame) == 0 {
		var head [5]byte

		if _, err := io.ReadFull(m.r, head[:4]); err != nil {
			return 0, err
		}

		length := binary.BigEndian.Uint32(head[:4])
		if length == 0 {
			continue
		}

		if length > incomingMaxMessage {
			return 0, errIncomingMessageTooLong
		}

		if _, err := io.ReadFull(m.r, head[4:]); err != nil {
			return 0, err
		}

		if head[4] != byte(peer_protocol.Extended) {
			if _, err := io.CopyN(io.Discard, m.r, int64(length)-1); err != nil {
				return 0, err
			}

			continue
		}

		if length > incomingMaxExtended {
			return 0, errIncomingMessageTooLong
		}

		frame := make([]byte, 4+length)
		copy(frame, head[:])

		if _, err := io.ReadFull(m.r, frame[5:]); err != nil {
			return 0, err
		}

		if length > 2 && bencodeUnsafe(frame[6:], incomingMaxDepth) {
			return 0, errIncomingBencode
		}

		m.frame = frame
	}

	n := copy(p, m.frame)
	m.frame = m.frame[n:]

	return n, nil
}

// the decoder recurses per nesting level and allocates what a string length claims
func bencodeUnsafe(b []byte, limit int) bool {
	depth := 0

	for i := 0; i < len(b); {
		switch c := b[i]; {
		case c == 'd' || c == 'l':
			depth++
			i++

			if depth > limit {
				return true
			}
		case c == 'e':
			depth--
			i++
		case c == 'i':
			end := bytes.IndexByte(b[i:], 'e')
			if end < 0 {
				return false
			}

			i += end + 1
		case c >= '0' && c <= '9':
			colon := i
			for colon < len(b) && b[colon] >= '0' && b[colon] <= '9' {
				colon++
			}

			if colon == len(b) || b[colon] != ':' {
				return false
			}

			length, err := strconv.ParseInt(string(b[i:colon]), 10, 64)
			if err != nil {
				return false
			}

			i = colon + 1

			if length > int64(len(b)-i) {
				return true
			}

			i += int(length)
		default:
			return false
		}

		if depth <= 0 {
			return false
		}
	}

	return false
}
