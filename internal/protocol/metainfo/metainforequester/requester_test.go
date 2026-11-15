package metainforequester

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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

	got, err := readAllPieces(bytes.NewReader(append(piece(1, tail), piece(0, full)...)), metadataPieceSize+100)
	if err == nil {
		t.Fatalf("a short piece before the last one must fail, got %d bytes", len(got))
	}

	got, err = readAllPieces(bytes.NewReader(append(piece(0, full), piece(1, tail)...)), metadataPieceSize+100)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, append(append([]byte{}, full...), tail...)) {
		t.Fatal("pieces are not assembled in order")
	}

	for name, stream := range map[string][]byte{
		"index beyond the metadata": piece(5, tail),
		"negative index":            piece(-1, tail),
		"huge index":                piece(1<<40, tail),
		"full piece past the end":   piece(1, full),
	} {
		size := uint(100)
		if name == "full piece past the end" {
			size = metadataPieceSize + 100
		}

		if _, err := readAllPieces(bytes.NewReader(stream), size); err == nil {
			t.Errorf("%s: want an error", name)
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
