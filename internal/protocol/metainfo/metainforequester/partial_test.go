package metainforequester

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/anacrolix/torrent/bencode"
	mi "github.com/anacrolix/torrent/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester/peertest"
)

func largeMetadata(t *testing.T, name string) ([]byte, protocol.ID) {
	t.Helper()

	encoded, err := bencode.Marshal(mi.Info{
		Name:        name,
		PieceLength: 16384,
		Length:      16384 * 5000,
		Pieces:      bytes.Repeat([]byte{0xab}, 20*5000),
	})
	if err != nil {
		t.Fatal(err)
	}

	return encoded, protocol.ID(mi.HashBytes(encoded))
}

func resumingReceiver() Receiver {
	receiver := NewReceiver(protocol.RandomPeerID(), 5*time.Second)
	receiver.partials = newPartialStore(receiver.pending, receiver.budget)

	return receiver
}

func TestResumeAsksTheNextPeerOnlyForTheRest(t *testing.T) {
	t.Parallel()

	metadata, hash := largeMetadata(t, "resume")
	pieces := int(pieceCount(uint(len(metadata))))
	receiver := resumingReceiver()

	first := receiveWith(t, receiver, peertest.Peer{InfoHash: hash, Metadata: metadata, Pieces: 3}, nil)
	if first.err == nil || first.spent != 3*metadataPieceSize {
		t.Fatalf("want a failed fetch that leaves three pieces, got %v and %d bytes", first.err, first.spent)
	}

	second := receiveWith(t, receiver, peertest.Peer{InfoHash: hash, Metadata: metadata}, nil)
	if second.err != nil || second.res.Info.Name != "resume" {
		t.Fatalf("the second peer must complete the meta info: %v", second.err)
	}

	if second.seen.Requests != pieces-3 || second.spent != 0 {
		t.Fatalf("want %d requests and nothing held, got %d and %d bytes", pieces-3, second.seen.Requests, second.spent)
	}

	third := receiveWith(t, receiver, peertest.Peer{InfoHash: hash, Metadata: metadata}, nil)
	if !errors.Is(third.err, ErrFetchedElsewhere) || third.seen.Requests != 0 {
		t.Fatalf("want ErrFetchedElsewhere without a request, got %v after %d", third.err, third.seen.Requests)
	}
}

func TestResumeDropsPiecesThatFailTheHash(t *testing.T) {
	t.Parallel()

	metadata, hash := largeMetadata(t, "poisoned")
	size := uint(len(metadata))
	receiver := resumingReceiver()

	if _, err := receiver.partials.missing(hash, size); err != nil {
		t.Fatal(err)
	}

	if _, err := receiver.partials.put(hash, size, 0, make([]byte, metadataPieceSize)); err != nil {
		t.Fatal(err)
	}

	poisoned := receiveWith(t, receiver, peertest.Peer{InfoHash: hash, Metadata: metadata}, nil)
	if poisoned.err == nil || poisoned.spent != 0 {
		t.Fatalf("want the forged piece to fail the check and be dropped, got %v and %d bytes", poisoned.err, poisoned.spent)
	}

	clean := receiveWith(t, receiver, peertest.Peer{InfoHash: hash, Metadata: metadata}, nil)
	if clean.err != nil || clean.seen.Requests != int(pieceCount(size)) {
		t.Fatalf("want a full fetch after the drop, got %v after %d requests", clean.err, clean.seen.Requests)
	}
}

func TestPartialStore(t *testing.T) {
	t.Parallel()

	receiver := resumingReceiver()
	store := receiver.partials
	store.budget = 3 * metadataPieceSize

	now := time.Unix(1_700_000_000, 0)
	store.now = func() time.Time { return now }

	piece := make([]byte, metadataPieceSize)
	size := uint(4 * metadataPieceSize)
	hashes := []protocol.ID{protocol.RandomPeerID(), protocol.RandomPeerID(), protocol.RandomPeerID()}

	for _, hash := range hashes[:2] {
		if _, err := store.missing(hash, size); err != nil {
			t.Fatal(err)
		}

		now = now.Add(time.Second)
	}

	for _, step := range []struct {
		hash  protocol.ID
		index uint
	}{{hashes[0], 0}, {hashes[1], 0}, {hashes[0], 1}} {
		if _, err := store.put(step.hash, size, step.index, piece); err != nil {
			t.Fatal(err)
		}

		now = now.Add(time.Second)
	}

	if _, err := store.put(hashes[0], size, 2, piece); err != nil {
		t.Fatal(err)
	}

	if _, err := store.put(hashes[1], size, 1, piece); !errors.Is(err, ErrIncomingBudget) {
		t.Fatalf("the least recently fed meta info must be evicted, got %v", err)
	}

	missing, err := store.missing(hashes[0], size)
	if err != nil || len(missing) != 1 || missing[0] != 3 {
		t.Fatalf("want only piece 3 missing, got %v %v", missing, err)
	}

	other, err := store.missing(hashes[0], size+1)
	if err != nil || len(other) != 5 {
		t.Fatalf("another claimed size must start on its own, got %v %v", other, err)
	}

	whole, err := store.put(hashes[0], size, 3, piece)
	if err != nil || uint(len(whole)) != size {
		t.Fatalf("the last piece must hand out the whole, got %d bytes %v", len(whole), err)
	}

	if _, err := store.put(hashes[0], size, 3, piece); !errors.Is(err, ErrFetchedElsewhere) {
		t.Fatalf("a late piece of a finished meta info: %v", err)
	}

	store.retry(hashes[0])

	if _, err := store.missing(hashes[2], size); err != nil {
		t.Fatal(err)
	}

	if _, err := store.put(hashes[2], size, 0, piece); err != nil {
		t.Fatal(err)
	}

	now = now.Add(partialIdle + time.Minute)

	if _, err := store.missing(hashes[0], size); err != nil {
		t.Fatal(err)
	}

	if held := receiver.pending.Load(); held != 0 {
		t.Fatalf("idle pieces must be swept, %d bytes held", held)
	}
}
