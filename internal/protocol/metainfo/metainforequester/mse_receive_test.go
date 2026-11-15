package metainforequester

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/anacrolix/torrent/mse"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

func mseIndexOf(hashes ...protocol.ID) MSELookup {
	index := make(map[MSEKey]protocol.ID, len(hashes))
	for _, hash := range hashes {
		index[MSEKeyOf(hash)] = hash
	}

	return func(key MSEKey) (protocol.ID, bool) {
		hash, ok := index[key]

		return hash, ok
	}
}

func TestMSEReceiveAgainstTheLibraryInitiator(t *testing.T) {
	t.Parallel()

	wanted := protocol.ID{7, 7, 7}
	other := protocol.ID{9}

	for name, test := range map[string]struct {
		provides mse.CryptoMethod
		initial  string
	}{
		"rc4":                     {mse.CryptoMethodRC4, ""},
		"plaintext":               {mse.CryptoMethodPlaintext, ""},
		"both":                    {mse.AllSupportedCrypto, ""},
		"rc4 with a payload":      {mse.CryptoMethodRC4, "early bytes"},
		"plaintext with payload":  {mse.CryptoMethodPlaintext, "early bytes"},
		"both with a big payload": {mse.AllSupportedCrypto, string(bytes.Repeat([]byte{'x'}, 5000))},
	} {
		client, server := net.Pipe()
		deadline := time.Now().Add(5 * time.Second)
		_ = client.SetDeadline(deadline)
		_ = server.SetDeadline(deadline)

		type initiated struct {
			rw  io.ReadWriter
			err error
		}

		done := make(chan initiated, 1)

		go func() {
			rw, _, err := mse.InitiateHandshake(client, wanted[:], []byte(test.initial), test.provides)
			done <- initiated{rw, err}
		}()

		rw, hash, err := mseReceive(server, mseIndexOf(other, wanted))
		if err != nil {
			t.Fatalf("%s: receive: %v", name, err)
		}

		if hash != wanted {
			t.Fatalf("%s: got the hash %s", name, hash)
		}

		early := make([]byte, len(test.initial))
		if _, err := io.ReadFull(rw, early); err != nil || string(early) != test.initial {
			t.Fatalf("%s: initial payload: %v", name, err)
		}

		initiator := <-done
		if initiator.err != nil {
			t.Fatalf("%s: initiate: %v", name, initiator.err)
		}

		go func() {
			_, _ = initiator.rw.Write([]byte("from the initiator"))
		}()

		got := make([]byte, len("from the initiator"))
		if _, err := io.ReadFull(rw, got); err != nil || string(got) != "from the initiator" {
			t.Fatalf("%s: stream to the receiver: %q %v", name, got, err)
		}

		go func() {
			_, _ = rw.Write([]byte("from the receiver"))
		}()

		got = make([]byte, len("from the receiver"))
		if _, err := io.ReadFull(initiator.rw, got); err != nil || string(got) != "from the receiver" {
			t.Fatalf("%s: stream to the initiator: %q %v", name, got, err)
		}

		_ = client.Close()
		_ = server.Close()
	}
}

func TestMSEReceiveRefusals(t *testing.T) {
	t.Parallel()

	client, server := net.Pipe()
	deadline := time.Now().Add(5 * time.Second)
	_ = client.SetDeadline(deadline)
	_ = server.SetDeadline(deadline)

	go func() {
		_, _, _ = mse.InitiateHandshake(client, []byte("some other torrent!!"), nil, mse.AllSupportedCrypto)
		_ = client.Close()
	}()

	if _, _, err := mseReceive(server, mseIndexOf(protocol.ID{1})); !errors.Is(err, ErrUnknownInfoHash) {
		t.Fatalf("unknown key: got %v", err)
	}

	_ = server.Close()

	for name, stream := range map[string][]byte{
		"empty":           nil,
		"short key":       bytes.Repeat([]byte{1}, 50),
		"key and garbage": bytes.Repeat([]byte{0xff}, 2000),
		"zero key":        make([]byte, 96+600),
	} {
		rw := struct {
			io.Reader
			io.Writer
		}{bytes.NewReader(stream), io.Discard}

		if _, _, err := mseReceive(rw, mseIndexOf(protocol.ID{1})); !errors.Is(err, errMSE) {
			t.Errorf("%s: got %v", name, err)
		}
	}
}
