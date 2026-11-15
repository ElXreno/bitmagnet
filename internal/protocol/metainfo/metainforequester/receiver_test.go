package metainforequester

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/anacrolix/torrent/bencode"
	mi "github.com/anacrolix/torrent/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester/peertest"
)

func testMetadata(t *testing.T, name string) ([]byte, protocol.ID) {
	t.Helper()

	encoded, err := bencode.Marshal(metainfo.Info{
		Name:        name,
		PieceLength: 16384,
		Length:      16384 * 1000,
		Pieces:      bytes.Repeat([]byte{0xab}, 20*1000),
	})
	if err != nil {
		t.Fatal(err)
	}

	return encoded, protocol.ID(mi.HashBytes(encoded))
}

func listenLoopback(t *testing.T) net.Listener {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = listener.Close() })

	return listener
}

type received struct {
	in    *Incoming
	res   Response
	err   error
	seen  peertest.Seen
	fake  error
	spent int64
}

func receive(t *testing.T, peer peertest.Peer, keys []protocol.ID) received {
	t.Helper()

	return receiveWith(t, NewReceiver(protocol.RandomPeerID(), 5*time.Second), peer, keys)
}

func receiveWith(t *testing.T, receiver Receiver, peer peertest.Peer, keys []protocol.ID) received {
	t.Helper()

	listener := listenLoopback(t)

	type dialed struct {
		seen peertest.Seen
		err  error
	}

	done := make(chan dialed, 1)

	go func() {
		seen, dialErr := peer.Dial(listener.Addr().String())
		done <- dialed{seen, dialErr}
	}()

	conn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}

	var result received

	result.in, result.err = receiver.Accept(conn, func() []protocol.ID { return keys })
	if result.err == nil {
		result.res, result.err = result.in.Fetch()
	}

	_ = conn.Close()

	fake := <-done
	result.seen, result.fake = fake.seen, fake.err
	result.spent = receiver.pending.Load()

	return result
}

func TestReceivePlaintextAndMSE(t *testing.T) {
	t.Parallel()

	metadata, hash := testMetadata(t, "incoming")
	keys := []protocol.ID{{1}, {2}, hash, {3}}

	for name, encrypted := range map[string]bool{"plaintext": false, "mse": true} {
		receiver := NewReceiver(protocol.RandomPeerID(), 5*time.Second)
		peer := peertest.Peer{InfoHash: hash, Metadata: metadata, Encrypted: encrypted}
		got := receiveWith(t, receiver, peer, keys)

		if got.err != nil || got.fake != nil {
			t.Fatalf("%s: receiver %v, peer %v", name, got.err, got.fake)
		}

		if got.in.InfoHash != hash || got.in.Encrypted != encrypted || got.res.Encrypted != encrypted {
			t.Fatalf("%s: info hash %s, encrypted %v/%v", name, got.in.InfoHash, got.in.Encrypted, got.res.Encrypted)
		}

		if got.res.Info.Name != "incoming" || got.res.Info.PieceLength != 16384 || got.res.Transport != "tcp" {
			t.Fatalf("%s: got %q over %q", name, got.res.Info.Name, got.res.Transport)
		}

		if got.res.PeerID != protocol.ID(peertest.PeerID) {
			t.Fatalf("%s: peer id %q", name, got.res.PeerID[:])
		}

		if got.seen.PeerID != [20]byte(receiver.PeerID()) || got.seen.Reserved != [8]byte(myExBits) {
			t.Fatalf("%s: the peer saw id %q and bits %x", name, got.seen.PeerID[:], got.seen.Reserved)
		}

		if got.seen.Requests != 2 || got.spent != 0 {
			t.Fatalf("%s: %d piece requests, %d bytes still reserved", name, got.seen.Requests, got.spent)
		}
	}
}

func TestReceiveUnknownInfoHash(t *testing.T) {
	t.Parallel()

	metadata, hash := testMetadata(t, "unknown")

	got := receive(t, peertest.Peer{InfoHash: hash, Metadata: metadata, Encrypted: true}, []protocol.ID{{1}, {2}})
	if !errors.Is(got.err, ErrUnknownInfoHash) {
		t.Fatalf("mse with a secret that is not registered: want ErrUnknownInfoHash, got %v", got.err)
	}

	got = receive(t, peertest.Peer{InfoHash: hash, Metadata: metadata}, nil)
	if got.err != nil || got.in.InfoHash != hash {
		t.Fatalf("plaintext leaves the decision to the caller, got %v", got.err)
	}
}

func TestReceivePeerWithoutMetaInfo(t *testing.T) {
	t.Parallel()

	_, hash := testMetadata(t, "leecher")

	got := receive(t, peertest.Peer{InfoHash: hash}, nil)
	if !errors.Is(got.err, ErrPeerHasNoMetaInfo) {
		t.Fatalf("want ErrPeerHasNoMetaInfo, got %v", got.err)
	}

	if got.seen.Requests != 0 {
		t.Fatalf("a peer without meta info was asked for %d pieces", got.seen.Requests)
	}
}

func TestReceiveBoundsWhatThePeerSends(t *testing.T) {
	t.Parallel()

	metadata, hash := testMetadata(t, "bounds")
	bitfield := peertest.Frame(append([]byte{5}, make([]byte, 200*1024)...)...)

	got := receive(t, peertest.Peer{InfoHash: hash, Metadata: metadata, Prelude: bitfield}, nil)
	if got.err != nil || got.res.Info.Name != "bounds" {
		t.Fatalf("a large bitfield before the extended handshake must be skipped, got %v", got.err)
	}

	for name, prelude := range map[string][]byte{
		"length 4 GiB":             {0xff, 0xff, 0xff, 0xff},
		"length over the cap":      {0x00, 0x10, 0x00, 0x01, 5},
		"extended message of 1MiB": {0x00, 0x10, 0x00, 0x00, 20, 0},
	} {
		got = receive(t, peertest.Peer{InfoHash: hash, Metadata: metadata, Prelude: prelude}, nil)
		if !errors.Is(got.err, errIncomingMessageTooLong) {
			t.Errorf("%s: want errIncomingMessageTooLong, got %v", name, got.err)
		}

		if got.spent != 0 {
			t.Errorf("%s: %d bytes still reserved", name, got.spent)
		}
	}
}

func TestReceiveRejectsMetadataOfAnotherTorrent(t *testing.T) {
	t.Parallel()

	_, hash := testMetadata(t, "wanted")
	other, _ := testMetadata(t, "forged")

	got := receive(t, peertest.Peer{InfoHash: hash, Metadata: other}, nil)
	if got.err == nil {
		t.Fatalf("meta info that does not hash to the info hash was accepted as %q", got.res.Info.Name)
	}

	if got.seen.Requests != 2 {
		t.Fatalf("want the forged pieces to be fetched and then refused, got %d requests", got.seen.Requests)
	}
}

func TestReceiveHandshakeFailures(t *testing.T) {
	t.Parallel()

	metadata, hash := testMetadata(t, "handshake")

	got := receive(t, peertest.Peer{InfoHash: hash, Metadata: metadata, NoExtensions: true}, nil)
	if !errors.Is(got.err, ErrIncomingHandshake) {
		t.Fatalf("peer without the extension protocol: want ErrIncomingHandshake, got %v", got.err)
	}

	for name, sent := range map[string]string{
		"http probe":        "GET / HTTP/1.1\r\nHost: example\r\n\r\n",
		"short and closed":  "\x13Bit",
		"nothing and close": "",
		"truncated":         "\x13BitTorrent protocol\x00\x00\x00\x00\x00\x10\x00\x00half",
	} {
		_, err := acceptRaw(t, NewReceiver(protocol.RandomPeerID(), 5*time.Second), sent, true)
		if !errors.Is(err, ErrIncomingHandshake) {
			t.Errorf("%s: want ErrIncomingHandshake, got %v", name, err)
		}
	}

	started := time.Now()

	_, err := acceptRaw(t, NewReceiver(protocol.RandomPeerID(), 200*time.Millisecond), "\x13Bit", false)
	if !errors.Is(err, ErrIncomingHandshake) || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("silent peer: want a handshake timeout, got %v", err)
	}

	if waited := time.Since(started); waited > 3*time.Second {
		t.Fatalf("silent peer held the connection for %s", waited)
	}
}

func acceptRaw(t *testing.T, receiver Receiver, sent string, closeAfter bool) (*Incoming, error) {
	t.Helper()

	listener := listenLoopback(t)

	client, err := (&net.Dialer{}).DialContext(context.Background(), "tcp4", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	defer client.Close()

	if _, err = client.Write([]byte(sent)); err != nil {
		t.Fatal(err)
	}

	if closeAfter {
		_ = client.(*net.TCPConn).CloseWrite()
	}

	conn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}

	defer conn.Close()

	return receiver.Accept(conn, func() []protocol.ID { return nil })
}

func TestBencodeUnsafe(t *testing.T) {
	t.Parallel()

	nested := func(depth int) []byte {
		value := append([]byte("d1:x"), bytes.Repeat([]byte("l"), depth)...)
		value = append(value, bytes.Repeat([]byte("e"), depth)...)

		return append(value, 'e')
	}

	metadata, _ := testMetadata(t, "plain")

	for name, test := range map[string]struct {
		value  []byte
		unsafe bool
	}{
		"meta info":                     {metadata, false},
		"extended handshake":            {[]byte("d1:md11:ut_metadatai3ee13:metadata_sizei1000e1:v4:teste"), false},
		"nested to the limit":           {nested(incomingMaxDepth - 1), false},
		"nested past the limit":         {nested(incomingMaxDepth), true},
		"string longer than the value":  {[]byte("d1:v134217727:xe"), true},
		"string that ends at the value": {[]byte("d1:v2:xye"), false},
		"raw bytes after the value":     {append([]byte("d5:piecei0ee"), bytes.Repeat([]byte("l"), 5000)...), false},
		"not bencode":                   {[]byte("hello"), false},
		"number without an end":         {[]byte("d1:xi5"), false},
		"empty":                         {nil, false},
	} {
		if got := bencodeUnsafe(test.value, incomingMaxDepth); got != test.unsafe {
			t.Errorf("%s: unsafe %v, want %v", name, got, test.unsafe)
		}
	}

	hostile := append([]byte("d4:name1:x12:piece lengthi16384e1:z"), nested(3000)...)
	hostile = append(hostile, 'e')
	hash := protocol.ID(mi.HashBytes(hostile))

	got := receive(t, peertest.Peer{InfoHash: hash, Metadata: hostile}, nil)
	if !errors.Is(got.err, errIncomingBencode) {
		t.Fatalf("meta info nested 3000 deep with a matching hash: want errIncomingBencode, got %v", got.err)
	}

	for name, dict := range map[string]string{
		"nested handshake":      string(nested(3000)),
		"handshake with a huge": "d1:v134217727:x",
	} {
		prelude := peertest.Frame(append([]byte{20, 0}, dict...)...)

		got = receive(t, peertest.Peer{InfoHash: hash, Metadata: hostile, Prelude: prelude}, nil)
		if !errors.Is(got.err, errIncomingBencode) {
			t.Errorf("%s: want errIncomingBencode, got %v", name, got.err)
		}
	}
}

func TestReceiveBudget(t *testing.T) {
	t.Parallel()

	metadata, hash := testMetadata(t, "budget")
	receiver := NewReceiver(protocol.RandomPeerID(), 5*time.Second)
	receiver.pending.Store(incomingBudget - 10)

	got := receiveWith(t, receiver, peertest.Peer{InfoHash: hash, Metadata: metadata}, nil)
	if !errors.Is(got.err, ErrIncomingBudget) {
		t.Fatalf("want ErrIncomingBudget, got %v", got.err)
	}

	if got.seen.Requests != 0 || got.spent != incomingBudget-10 {
		t.Fatalf("%d piece requests, %d bytes reserved", got.seen.Requests, got.spent)
	}
}

func TestIncomingMessages(t *testing.T) {
	t.Parallel()

	extended := message(0, "d1:md11:ut_metadatai3eee", nil)

	stream := append([]byte{0, 0, 0, 0}, peertest.Frame(1)...)
	stream = append(stream, peertest.Frame(append([]byte{5}, make([]byte, 70000)...)...)...)
	stream = append(stream, extended...)
	stream = append(stream, 0, 0, 0, 0)

	got, err := io.ReadAll(&incomingMessages{r: bytes.NewReader(stream)})
	if err != nil || !bytes.Equal(got, extended) {
		t.Fatalf("want only the extension message, got %d bytes and %v", len(got), err)
	}

	largest := message(1, "", make([]byte, incomingMaxExtended-2))

	got, err = io.ReadAll(&incomingMessages{r: bytes.NewReader(largest)})
	if err != nil || !bytes.Equal(got, largest) {
		t.Fatalf("an extension message of the largest size must pass, got %d bytes and %v", len(got), err)
	}

	for name, stream := range map[string][]byte{
		"extension message over the cap": message(1, "", make([]byte, incomingMaxExtended-1)),
		"other message over the cap":     peertest.Frame(append([]byte{5}, make([]byte, incomingMaxMessage)...)...),
	} {
		_, err = io.ReadAll(&incomingMessages{r: bytes.NewReader(stream)})
		if !errors.Is(err, errIncomingMessageTooLong) {
			t.Errorf("%s: want errIncomingMessageTooLong, got %v", name, err)
		}
	}

	_, err = io.ReadAll(&incomingMessages{r: bytes.NewReader(extended[:len(extended)-3])})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("a truncated message: want io.ErrUnexpectedEOF, got %v", err)
	}
}
