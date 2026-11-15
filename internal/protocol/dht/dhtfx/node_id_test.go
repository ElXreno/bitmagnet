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

func TestNodeIDReplacesTheFileThroughATempFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "dht_node_id")

	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	id, _ := loadNodeID(path)
	saveNodeID(path, id)

	again, stored := loadNodeID(path)
	if !stored || again != id {
		t.Fatalf("want the saved id back, got %s stored=%v", again, stored)
	}

	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 {
		t.Fatalf("want only the id file in the directory, got %v, %v", entries, err)
	}

	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("want mode 0600, got %v, %v", info, err)
	}

	if err := writeFileAtomic(filepath.Join(dir, "missing", "dht_node_id"), []byte("x")); err == nil {
		t.Fatal("a missing directory must fail")
	}
}
