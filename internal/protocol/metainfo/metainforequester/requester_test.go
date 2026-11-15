package metainforequester

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"
)

func message(id byte, payload string, data []byte) []byte {
	body := append([]byte{20, id}, payload...)
	body = append(body, data...)

	return append(binary.BigEndian.AppendUint32(nil, uint32(len(body))), body...)
}

func piece(index int, data []byte) []byte {
	return message(1, fmt.Sprintf("d8:msg_typei1e5:piecei%dee", index), data)
}

func TestReadAllPieces(t *testing.T) {
	t.Parallel()

	full := bytes.Repeat([]byte{7}, metadataPieceSize)
	tail := bytes.Repeat([]byte{9}, 100)

	want := append(append([]byte{}, full...), tail...)

	got, err := readAllPieces(bytes.NewReader(append(piece(1, tail), piece(0, full)...)), metadataPieceSize+100)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("pieces that arrive out of order must be assembled in order, got %d bytes, %v", len(got), err)
	}

	got, err = readAllPieces(bytes.NewReader(append(piece(0, full), piece(1, tail)...)), metadataPieceSize+100)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, want) {
		t.Fatal("pieces are not assembled in order")
	}

	for name, test := range map[string]struct {
		stream []byte
		size   uint
	}{
		"index beyond the metadata": {piece(5, tail), 100},
		"negative index":            {piece(-1, tail), 100},
		"huge index":                {piece(1<<40, tail), 100},
		"full piece past the end":   {piece(1, full), metadataPieceSize + 100},
		"short piece before the last": {
			piece(0, tail), metadataPieceSize + 100,
		},
		"last piece too long": {
			piece(1, append(tail, 1)), metadataPieceSize + 100,
		},
		"duplicate piece": {
			append(piece(0, full), piece(0, full)...), 2*metadataPieceSize + 100,
		},
		"duplicate piece hiding a hole": {
			append(append(piece(0, full), piece(0, full)...), piece(2, tail)...), 2*metadataPieceSize + 100,
		},
	} {
		if _, err := readAllPieces(bytes.NewReader(test.stream), test.size); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestReadAllPiecesFollowsTheBytesReceived(t *testing.T) {
	declared := uint(maxMetadataSize - 1)
	last := (declared+metadataPieceSize-1)/metadataPieceSize - 1
	tail := bytes.Repeat([]byte{1}, int(declared-last*metadataPieceSize))
	stream := piece(int(last), tail)

	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)

	_, err := readAllPieces(bytes.NewReader(stream), declared)

	runtime.ReadMemStats(&after)

	if err == nil {
		t.Fatal("a stream that ends after one piece must fail")
	}

	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 4*metadataPieceSize {
		t.Fatalf("one piece of a %d byte declaration allocated %d bytes", declared, allocated)
	}
}

func TestReadAllPiecesSkipsLargeMessages(t *testing.T) {
	t.Parallel()

	full := bytes.Repeat([]byte{7}, metadataPieceSize)
	tail := bytes.Repeat([]byte{9}, 100)
	bitfield := binary.BigEndian.AppendUint32(nil, 1+3<<20)
	bitfield = append(append(bitfield, 5), make([]byte, 3<<20)...)

	stream := bytes.Join([][]byte{bitfield, piece(0, full), bitfield, piece(1, tail)}, nil)

	got, err := readAllPieces(bytes.NewReader(stream), metadataPieceSize+100)
	if err != nil || !bytes.Equal(got, append(append([]byte{}, full...), tail...)) {
		t.Fatalf("large messages around the pieces must be skipped, got %d bytes, %v", len(got), err)
	}

	handshake := message(0, "d1:md11:ut_metadatai3ee13:metadata_sizei1000ee", nil)

	parsed, err := readExHandshake(discardWriter{bytes.NewReader(append(append([]byte{}, bitfield...), handshake...))})
	if err != nil {
		t.Fatalf("a large bitfield before the extended handshake must be skipped: %v", err)
	}

	if size, _, _ := parsed.metadata(); size != 1000 {
		t.Fatalf("metadata size %d", size)
	}
}

func TestReadMessageLimit(t *testing.T) {
	t.Parallel()

	long := binary.BigEndian.AppendUint32(nil, maxMetadataSize+1)
	if _, err := readMessage(bytes.NewReader(long), maxUmMessage); err == nil {
		t.Fatal("a message beyond the metadata maximum must fail")
	}

	oversizeData := append(message(1, "d8:msg_typei1e5:piecei0ee", nil), make([]byte, maxUmMessage)...)
	binary.BigEndian.PutUint32(oversizeData, uint32(len(oversizeData)-4))

	full := bytes.Repeat([]byte{7}, metadataPieceSize)
	followed := append(append([]byte{}, oversizeData...), piece(0, full)...)

	if _, err := readMessage(bytes.NewReader(followed), maxUmMessage); err == nil {
		t.Fatal("a ut_metadata message beyond the limit must fail")
	}

	if _, err := readAllPieces(bytes.NewReader(followed), metadataPieceSize); err == nil {
		t.Fatal("an oversize piece must fail the transfer")
	}

	truncated := append(binary.BigEndian.AppendUint32(nil, 5000), 1, 2, 3)
	if _, err := readMessage(bytes.NewReader(truncated), 100); err == nil {
		t.Fatal("a skipped message cut short must fail")
	}

	small := append(binary.BigEndian.AppendUint32(nil, 3), 20, 1, 2)

	got, err := readMessage(bytes.NewReader(append(append(binary.BigEndian.AppendUint32(nil, 200), make([]byte, 200)...), small...)), 100)
	if err != nil || !bytes.Equal(got, []byte{20, 1, 2}) {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestHostileBencodeIsRefusedBeforeDecoding(t *testing.T) {
	t.Parallel()

	huge := "d1:v134217727:xe"
	deep := strings.Repeat("l", incomingMaxDepth+1) + strings.Repeat("e", incomingMaxDepth+1)

	for name, dict := range map[string]string{"string longer than the message": huge, "nesting": "d1:x" + deep + "e"} {
		if _, err := readExHandshake(discardWriter{bytes.NewReader(message(0, dict, nil))}); !errors.Is(err, errIncomingBencode) {
			t.Errorf("extended handshake, %s: got %v", name, err)
		}

		if _, err := readAllPieces(bytes.NewReader(message(1, dict, nil)), 100); !errors.Is(err, errIncomingBencode) {
			t.Errorf("ut_metadata message, %s: got %v", name, err)
		}
	}
}

type discardWriter struct {
	io.Reader
}

func (discardWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func TestReadExHandshake(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		dict    string
		version string
		size    uint
		id      uint8
		err     error
	}{
		"complete":         {"d1:md11:ut_metadatai3ee13:metadata_sizei1000e1:v17:qBittorrent/5.0.1e", "qBittorrent/5.0.1", 1000, 3, nil},
		"version not text": {"d1:md11:ut_metadatai3ee13:metadata_sizei1000e1:vi5ee", "", 1000, 3, nil},
		"no metadata size": {"d1:md11:ut_metadatai3ee1:v2:XLe", "XL", 0, 0, errNoMetadataSize},
		"no ut_metadata":   {"d1:md6:ut_pexi1ee13:metadata_sizei1000ee", "", 0, 0, errNoUTMetadata},
	} {
		handshake, err := readExHandshake(discardWriter{bytes.NewReader(message(0, test.dict, nil))})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		if handshake.V != test.version {
			t.Errorf("%s: version %q", name, handshake.V)
		}

		size, id, err := handshake.metadata()
		if !errors.Is(err, test.err) || size != test.size || id != test.id {
			t.Errorf("%s: got %d, %d, %v", name, size, id, err)
		}
	}
}
