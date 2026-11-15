package dhtfx

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNodeIDIsWrittenOnlyWhenSaved(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "dht_node_id")

	id, stored := loadNodeID(path)
	if stored {
		t.Fatal("a missing file is not a stored id")
	}

	if _, err := os.Stat(path); err == nil {
		t.Fatal("loading must not create the file")
	}

	saveNodeID(path, id)

	again, stored := loadNodeID(path)
	if !stored || again != id {
		t.Fatalf("want the saved id back, got %s stored=%v", again, stored)
	}
}
