package dhtcrawler

import (
	"os"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

func TestFailedHashesSurviveRestartAndExpire(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	clock := time.Unix(1_700_000_000, 0)

	f, err := newFailedHashes(dir, 3*time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	f.now = func() time.Time { return clock }

	failed, other := protocol.ID{1, 2, 3}, protocol.ID{9, 9, 9}

	if f.test(failed) {
		t.Fatal("an empty memory must not know any hash")
	}

	f.add(failed)

	if !f.test(failed) || f.test(other) {
		t.Fatal("want exactly the added hash")
	}

	if err := f.save(); err != nil {
		t.Fatal(err)
	}

	// newFailedHashes reads the real clock, so load with a memory long enough to cover the fixed test time.
	reloaded, err := newFailedHashes(dir, time.Since(clock)+3*time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	reloaded.now = func() time.Time { return clock.Add(time.Hour) }
	reloaded.keep = 3

	if !reloaded.test(failed) || reloaded.test(other) {
		t.Fatal("the memory must survive a restart")
	}

	reloaded.now = func() time.Time { return clock.Add(4 * time.Hour) }

	if reloaded.test(failed) {
		t.Fatal("the memory must expire")
	}

	if err := reloaded.save(); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 0 {
		t.Fatalf("expired hours must be removed from disk, found %d files", len(entries))
	}
}

func TestFailedHashesDisabled(t *testing.T) {
	t.Parallel()

	f, err := newFailedHashes(t.TempDir(), 0)
	if err != nil || f != nil {
		t.Fatalf("a zero lifetime disables the memory, got %v %v", f, err)
	}

	f.add(protocol.ID{1})

	if f.test(protocol.ID{1}) || f.save() != nil {
		t.Fatal("a disabled memory must be inert")
	}
}
