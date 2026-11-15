// Package peertest is a scripted peer that dials in and serves meta info.
package peertest

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/mse"
)

const (
	protocolName = "\x13BitTorrent protocol"
	pieceSize    = 16 * 1024
	utMetadata   = 3
)

var PeerID = [20]byte([]byte("-qB4650-peertestpeer"))

type Peer struct {
	InfoHash     [20]byte
	Metadata     []byte
	Encrypted    bool
	NoExtensions bool
	Prelude      []byte
}

type offered struct {
	UTMetadata int `bencode:"ut_metadata"`
}

type extendedHandshake struct {
	M offered `bencode:"m"`
}

type metadataMessage struct {
	MsgType int `bencode:"msg_type"`
	Piece   int `bencode:"piece"`
}

type Seen struct {
	Reserved [8]byte
	PeerID   [20]byte
	Requests int
}

func Frame(body ...byte) []byte {
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(body))), body...)
}

func (p Peer) Dial(addr string) (Seen, error) {
	var seen Seen

	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(context.Background(), "tcp", addr)
	if err != nil {
		return seen, err
	}

	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	var rw io.ReadWriter = conn

	if p.Encrypted {
		rw, _, err = mse.InitiateHandshake(conn, p.InfoHash[:], nil, mse.AllSupportedCrypto)
		if err != nil {
			return seen, fmt.Errorf("mse: %w", err)
		}
	}

	var reserved [8]byte
	if !p.NoExtensions {
		reserved[5] = 0x10
	}

	handshake := append([]byte(protocolName), reserved[:]...)
	handshake = append(handshake, p.InfoHash[:]...)
	handshake = append(handshake, PeerID[:]...)

	if _, err = rw.Write(handshake); err != nil {
		return seen, err
	}

	reply := make([]byte, 68)
	if _, err = io.ReadFull(rw, reply); err != nil {
		return seen, fmt.Errorf("handshake reply: %w", err)
	}

	if string(reply[:20]) != protocolName || [20]byte(reply[28:48]) != p.InfoHash {
		return seen, errors.New("handshake reply is not for our info hash")
	}

	copy(seen.Reserved[:], reply[20:28])
	copy(seen.PeerID[:], reply[48:68])

	offer := fmt.Sprintf("d1:md11:ut_metadatai%dee", utMetadata)
	if p.Metadata != nil {
		offer += fmt.Sprintf("13:metadata_sizei%de", len(p.Metadata))
	}

	offer += "1:v8:peerteste"

	if _, err = rw.Write(append(p.Prelude, Frame(append([]byte{20, 0}, offer...)...)...)); err != nil {
		return seen, err
	}

	remoteUTMetadata := byte(0)

	for {
		body, readErr := readFrame(rw)
		if errors.Is(readErr, io.EOF) {
			return seen, nil
		}

		if readErr != nil {
			return seen, readErr
		}

		if len(body) < 2 || body[0] != 20 {
			continue
		}

		if body[1] == 0 {
			var theirs extendedHandshake

			if err = bencode.Unmarshal(body[2:], &theirs); err != nil {
				return seen, fmt.Errorf("their extended handshake: %w", err)
			}

			remoteUTMetadata = byte(theirs.M.UTMetadata)

			continue
		}

		if body[1] != utMetadata {
			continue
		}

		var request metadataMessage

		if err = bencode.Unmarshal(body[2:], &request); err != nil {
			return seen, fmt.Errorf("their ut_metadata message: %w", err)
		}

		if request.MsgType != 0 {
			continue
		}

		seen.Requests++

		start := request.Piece * pieceSize
		if request.Piece < 0 || start >= len(p.Metadata) {
			return seen, fmt.Errorf("request for piece %d of %d bytes", request.Piece, len(p.Metadata))
		}

		data := p.Metadata[start:min(start+pieceSize, len(p.Metadata))]
		header := fmt.Sprintf("d8:msg_typei1e5:piecei%de10:total_sizei%dee", request.Piece, len(p.Metadata))
		answer := append([]byte{20, remoteUTMetadata}, header...)

		if _, err = rw.Write(Frame(append(answer, data...)...)); err != nil {
			return seen, err
		}
	}
}

func readFrame(r io.Reader) ([]byte, error) {
	var length [4]byte

	if _, err := io.ReadFull(r, length[:]); err != nil {
		return nil, err
	}

	body := make([]byte, binary.BigEndian.Uint32(length[:]))
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}

	return body, nil
}
