package metainforequester

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent/mse"
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

	errIncomingMessageTooLong = errors.New("incoming message is longer than max allowed")
	errIncomingBencode        = errors.New("incoming bencode is nested too deep or claims a string it does not hold")
)

const (
	IncomingTimeout = 15 * time.Second

	incomingMaxExtended = metadataPieceSize + 1024
	incomingMaxMessage  = 1 << 20
	incomingMaxDepth    = 32
	incomingBudget      = 256 << 20
	incomingParses      = 2
)

type Receiver struct {
	clientID protocol.ID
	timeout  time.Duration
	pending  *atomic.Int64
	parsing  chan struct{}
}

func NewReceiver(clientID protocol.ID, timeout time.Duration) Receiver {
	return Receiver{
		clientID: clientID,
		timeout:  timeout,
		pending:  new(atomic.Int64),
		parsing:  make(chan struct{}, incomingParses),
	}
}

// the file tree decoder copies a subtree once per level above it, so few meta infos are parsed at a time
func (r Receiver) parse(infoHash protocol.ID, encoded []byte) (metainfo.Info, error) {
	r.parsing <- struct{}{}

	defer func() {
		<-r.parsing
	}()

	if bencodeUnsafe(encoded, incomingMaxDepth) {
		return metainfo.Info{}, errIncomingBencode
	}

	return metainfo.ParseMetaInfoBytes(infoHash, encoded)
}

func (r Receiver) PeerID() protocol.ID {
	return r.clientID
}

type Incoming struct {
	InfoHash  protocol.ID
	Encrypted bool
	receiver  Receiver
	rw        io.ReadWriter
	addr      netip.AddrPort
	bits      PeerExtensionBits
}

type readWriter struct {
	io.Reader
	io.Writer
}

func (r Receiver) Accept(conn net.Conn, keys func() []protocol.ID) (*Incoming, error) {
	if err := conn.SetDeadline(time.Now().Add(r.timeout)); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrIncomingHandshake, err)
	}

	const protocolLen = len(peer_protocol.Protocol)

	var head [protocolLen + 8 + 20]byte

	if _, err := io.ReadFull(conn, head[:protocolLen]); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrIncomingHandshake, err)
	}

	in := &Incoming{receiver: r, rw: conn}
	rest := head[protocolLen:]

	if string(head[:protocolLen]) != peer_protocol.Protocol {
		replayed := readWriter{io.MultiReader(bytes.NewReader(head[:protocolLen]), conn), conn}

		decrypted, _, err := mse.ReceiveHandshake(
			context.Background(),
			replayed,
			func(yield func([]byte) bool) {
				for _, key := range keys() {
					if !yield(key[:]) {
						return
					}
				}
			},
			mse.DefaultCryptoSelector,
		)
		if errors.Is(err, mse.ErrNoSecretKeyMatch) {
			return nil, ErrUnknownInfoHash
		}

		if err != nil {
			return nil, fmt.Errorf("%w: mse: %w", ErrIncomingHandshake, err)
		}

		in.rw = decrypted
		in.Encrypted = true
		rest = head[:]
	}

	if _, err := io.ReadFull(in.rw, rest); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrIncomingHandshake, err)
	}

	if string(head[:protocolLen]) != peer_protocol.Protocol {
		return nil, fmt.Errorf("%w: not a BitTorrent handshake", ErrIncomingHandshake)
	}

	copy(in.bits[:], head[protocolLen:protocolLen+8])
	copy(in.InfoHash[:], head[protocolLen+8:])

	if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		in.addr = tcpAddr.AddrPort()
	}

	return in, nil
}

func (in *Incoming) Fetch() (Response, error) {
	if !in.bits.GetBit(ExtensionBitLtep) {
		return Response{}, fmt.Errorf("%w: peer does not support the extension protocol", ErrIncomingHandshake)
	}

	reply := make([]byte, 0, 68+len(extendedHandshake))
	reply = append(reply, peer_protocol.Protocol...)
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

	peerClient := clientid.Peer([20]byte(hsInfo.PeerID))
	peerVersion := ""
	outcome := func(stage string) {
		peerClientTotal.With(prometheus.Labels{"client": peerClient, "stage": "inbound_" + stage}).Inc()
		dumpPeerID(in.addr, in.InfoHash, hsInfo, peerClient, "inbound_"+stage, peerVersion)
	}

	peerClientTotal.With(prometheus.Labels{"client": peerClient, "stage": "inbound_handshake"}).Inc()

	if clientid.ClosedNetwork(peerClient) {
		outcome("closed_network")

		return Response{}, ErrClosedNetworkPeer
	}

	messages := &incomingMessages{r: in.rw}

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

	defer in.receiver.pending.Add(-int64(metadataSize))

	if in.receiver.pending.Add(int64(metadataSize)) > incomingBudget {
		outcome("pieces_failed")

		return Response{}, ErrIncomingBudget
	}

	if requestErr := requestAllPieces(in.rw, metadataSize, utMetadata); requestErr != nil {
		outcome("pieces_failed")

		return Response{}, requestErr
	}

	pieces, piecesErr := readAllPieces(messages, metadataSize)
	if piecesErr != nil {
		outcome("pieces_failed")

		return Response{}, piecesErr
	}

	parsed, parseErr := in.receiver.parse(in.InfoHash, pieces)
	if parseErr != nil {
		outcome("bad_metainfo")

		return Response{}, parseErr
	}

	outcome("metainfo")

	return Response{
		HandshakeInfo: hsInfo,
		Info:          parsed,
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
