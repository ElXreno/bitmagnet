package dhtcrawler

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

func TestFailedHashesSurviveRestartAndExpire(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	clock := time.Unix(1_700_000_000, 0)

	f, err := newFailedHashes(dir, 3*time.Hour, time.Hour, 1000)
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
	reloaded, err := newFailedHashes(dir, time.Since(clock)+3*time.Hour, time.Hour, 1000)
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

	f, err := newFailedHashes(t.TempDir(), 0, time.Hour, 1000)
	if err != nil || f != nil {
		t.Fatalf("a zero lifetime disables the memory, got %v %v", f, err)
	}

	f.add(protocol.ID{1})

	if f.test(protocol.ID{1}) || f.save() != nil {
		t.Fatal("a disabled memory must be inert")
	}
}

func TestWriteFileAtomicKeepsTheOldFileOnFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "state")

	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	failure := errors.New("disk full")

	err := writeFileAtomic(path, func(w io.Writer) error {
		_, _ = w.Write([]byte("partial"))

		return failure
	})
	if !errors.Is(err, failure) {
		t.Fatalf("got %v", err)
	}

	if got, _ := os.ReadFile(path); string(got) != "old" {
		t.Fatalf("a failed write replaced the file with %q", got)
	}

	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Fatalf("a failed write left %d files", len(entries))
	}

	err = writeFileAtomic(path, func(w io.Writer) error {
		_, err := w.Write([]byte("new"))

		return err
	})
	if got, _ := os.ReadFile(path); err != nil || string(got) != "new" {
		t.Fatalf("got %q, %v", got, err)
	}

	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Fatalf("a write left %d files", len(entries))
	}
}

func TestFailedHashesAreReleasedOverTheHourWhenSmooth(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	clock := time.Unix(1_700_000_000, 0).Truncate(time.Hour).Add(10 * time.Minute)

	f, err := newFailedHashes(dir, 2*time.Hour, time.Hour, 1000, true)
	if err != nil {
		t.Fatal(err)
	}

	f.now = func() time.Time { return clock }

	early, late := protocol.ID{1}, protocol.ID{2}
	early[16], late[16] = 0x20, 0xe0

	f.add(early)
	f.add(late)

	for _, step := range []struct {
		after    time.Duration
		held     [2]bool
		released [2]bool
	}{
		{0, [2]bool{true, true}, [2]bool{false, false}},
		{110 * time.Minute, [2]bool{true, true}, [2]bool{false, false}},
		{20 * time.Minute, [2]bool{false, true}, [2]bool{true, false}},
		{40 * time.Minute, [2]bool{false, false}, [2]bool{true, true}},
		{59 * time.Minute, [2]bool{false, false}, [2]bool{true, true}},
		{time.Minute, [2]bool{false, false}, [2]bool{false, false}},
	} {
		clock = clock.Add(step.after)

		if err := f.save(); err != nil {
			t.Fatal(err)
		}

		// newFailedHashes reads the real clock, so load with a memory long enough to cover the fixed test time.
		reloaded, err := newFailedHashes(dir, time.Since(clock)+3*time.Hour, time.Hour, 1000, true)
		if err != nil {
			t.Fatal(err)
		}

		reloaded.now = f.now
		reloaded.keep = 2

		for _, memory := range []*failedHashes{f, reloaded} {
			if got := [2]bool{memory.test(early), memory.test(late)}; got != step.held {
				t.Fatalf("at %s held: want %v, got %v", clock.Format("15:04"), step.held, got)
			}

			released := [2]bool{
				!memory.test(early) && memory.released(early), !memory.test(late) && memory.released(late),
			}
			if released != step.released {
				t.Fatalf("at %s released: want %v, got %v", clock.Format("15:04"), step.released, released)
			}
		}
	}
}
