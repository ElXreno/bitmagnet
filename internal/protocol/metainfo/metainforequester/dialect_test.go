package metainforequester

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

func TestDialectHandshakeIsAnsweredInKind(t *testing.T) {
	hash := protocol.ID{7}

	greet := func(t *testing.T, env string) (*Incoming, net.Conn, error) {
		t.Helper()
		t.Setenv("BITMAGNET_INBOUND_DIALECT", env)

		client, server := net.Pipe()

		t.Cleanup(func() {
			_ = client.Close()
			_ = server.Close()
		})

		go func() {
			greeting := append([]byte(dialectProtocol), myExBits[:]...)
			_, _ = client.Write(append(greeting, hash[:]...))

			if env != "on" {
				_ = client.Close()
			}
		}()

		in, err := NewReceiver(protocol.RandomPeerID(), time.Second).Accept(server, KeysLookup(func() []protocol.ID { return nil }))

		return in, client, err
	}

	if _, _, err := greet(t, "off"); !errors.Is(err, ErrIncomingHandshake) {
		t.Fatalf("with the knob off the greeting is no handshake, got %v", err)
	}

	in, client, err := greet(t, "on")
	if err != nil || !in.Dialect || in.InfoHash != hash || in.Encrypted {
		t.Fatalf("in %+v, err %v", in, err)
	}

	go func() { _, _ = in.Fetch() }()

	answer := make([]byte, len(dialectProtocol))
	if _, err := io.ReadFull(client, answer); err != nil || string(answer) != dialectProtocol {
		t.Fatalf("the answer must open with the same protocol string, got %x, err %v", answer, err)
	}
}

func TestOutgoingHandshakeInTheDialect(t *testing.T) {
	t.Parallel()

	hash, peerID := protocol.ID{9}, protocol.ID{1, 2, 3}

	client, server := net.Pipe()

	defer client.Close()
	defer server.Close()

	go func() {
		greeting := make([]byte, 68)
		if _, err := io.ReadFull(server, greeting); err != nil || string(greeting[:20]) != dialectProtocol {
			return
		}

		answer := append([]byte(dialectProtocol), myExBits[:]...)
		answer = append(answer, hash[:]...)
		_, _ = server.Write(append(answer, peerID[:]...))
	}()

	info, err := btHandshake(client, dialectProtocol, hash, protocol.RandomPeerID())
	if err != nil || info.PeerID != peerID {
		t.Fatalf("info %+v, err %v", info, err)
	}

	if !handshakeClosed(io.ErrUnexpectedEOF) || handshakeClosed(errors.New("timeout")) {
		t.Fatal("only a closed handshake leads to the dialect retry")
	}
}
