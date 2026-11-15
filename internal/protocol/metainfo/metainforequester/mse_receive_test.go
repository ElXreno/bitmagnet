package metainforequester

import (
	"bytes"
	"errors"
	"io"
	"math/big"
	"net"
	"sync"
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

func BenchmarkMSEReceive(b *testing.B) {
	var theirs [mseKeyLen]byte

	new(big.Int).Exp(mseGenerator, big.NewInt(0x1234567890abcdef), msePrime).FillBytes(theirs[:])

	lookup := mseIndexOf(protocol.ID{1})

	b.ResetTimer()

	for range b.N {
		rw := struct {
			io.Reader
			io.Writer
		}{bytes.NewReader(theirs[:]), io.Discard}

		_, _, _ = mseReceive(rw, lookup)
	}
}

func TestMSEReceiveRefusesDegenerateKeys(t *testing.T) {
	t.Parallel()

	one := big.NewInt(1)
	primeMinusOne := new(big.Int).Sub(msePrime, one)

	for name, key := range map[string]*big.Int{
		"zero":         big.NewInt(0),
		"one":          one,
		"p minus one":  primeMinusOne,
		"p":            msePrime,
		"above p":      new(big.Int).Add(msePrime, big.NewInt(5)),
		"all bits set": new(big.Int).Sub(new(big.Int).Lsh(one, 8*mseKeyLen), one),
	} {
		var stream [mseKeyLen]byte

		key.FillBytes(stream[:])

		var written bytes.Buffer

		rw := struct {
			io.Reader
			io.Writer
		}{bytes.NewReader(stream[:]), &written}

		if _, _, err := mseReceive(rw, mseIndexOf(protocol.ID{1})); !errors.Is(err, errMSE) {
			t.Errorf("%s: got %v", name, err)
		}

		if written.Len() != 0 {
			t.Errorf("%s: answered a degenerate key with %d bytes", name, written.Len())
		}
	}

	var stream [mseKeyLen]byte

	new(big.Int).Sub(msePrime, big.NewInt(2)).FillBytes(stream[:])

	var written bytes.Buffer

	rw := struct {
		io.Reader
		io.Writer
	}{bytes.NewReader(stream[:]), &written}

	_, _, _ = mseReceive(rw, mseIndexOf(protocol.ID{1}))

	if written.Len() != mseKeyLen {
		t.Errorf("p minus two: answered with %d bytes", written.Len())
	}
}

func TestMSEKeysRotate(t *testing.T) {
	t.Parallel()

	now := time.Unix(1000, 0)
	keys := &mseKeys{now: func() time.Time { return now }, lifetime: time.Minute}

	first, err := keys.get()
	if err != nil {
		t.Fatal(err)
	}

	now = now.Add(59 * time.Second)

	same, _ := keys.get()
	if same != first {
		t.Fatal("the key pair changed before its lifetime ended")
	}

	now = now.Add(time.Second)

	next, _ := keys.get()
	if next == first || next.private.Cmp(first.private) == 0 || next.public == first.public {
		t.Fatal("the key pair was not replaced after its lifetime")
	}

	want := new(big.Int).Exp(mseGenerator, next.private, msePrime)
	if new(big.Int).SetBytes(next.public[:]).Cmp(want) != 0 {
		t.Fatal("public key does not match the private one")
	}
}

func TestMSEKeysConcurrent(t *testing.T) {
	t.Parallel()

	keys := &mseKeys{now: time.Now, lifetime: time.Minute}
	pairs := make(chan *mseKeyPair, 32)

	var wg sync.WaitGroup

	for range cap(pairs) {
		wg.Add(1)

		go func() {
			defer wg.Done()

			pair, _ := keys.get()
			pairs <- pair
		}()
	}

	wg.Wait()
	close(pairs)

	first := <-pairs
	for pair := range pairs {
		if pair != first {
			t.Fatal("concurrent callers got different key pairs")
		}
	}
}
