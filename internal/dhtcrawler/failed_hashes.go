package dhtcrawler

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	boom "github.com/tylertreat/BoomFilters"
)

const (
	failedHashesFalseRate  = 0.001
	failedHashesFilePrefix = "failed-"
	failedHashesFileSuffix = ".bloom"
)

// failedHashes remembers, across restarts, the info hashes whose meta info could not be fetched, so that the
// crawler does not spend packets on the same dead torrents again until the memory expires. It keeps one bloom
// filter per hour and forgets an hour as a whole.
type failedHashes struct {
	mu          sync.Mutex
	dir         string
	span        time.Duration
	perSpan     uint
	keep        int64
	generations map[int64]*boom.BloomFilter
	dirty       map[int64]bool
	now         func() time.Time
}

func newFailedHashes(dir string, ttl time.Duration, span time.Duration, perSpan uint) (*failedHashes, error) {
	if ttl <= 0 {
		return nil, nil //nolint:nilnil
	}

	f := &failedHashes{
		dir:         dir,
		span:        span,
		perSpan:     perSpan,
		keep:        int64(ttl / span),
		generations: make(map[int64]*boom.BloomFilter),
		dirty:       make(map[int64]bool),
		now:         time.Now,
	}

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	current := f.generation()

	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, failedHashesFilePrefix) || !strings.HasSuffix(name, failedHashesFileSuffix) {
			continue
		}

		generation, parseErr := strconv.ParseInt(
			strings.TrimSuffix(strings.TrimPrefix(name, failedHashesFilePrefix), failedHashesFileSuffix), 10, 64)
		if parseErr != nil {
			continue
		}

		path := filepath.Join(dir, name)

		if generation <= current-f.keep || generation > current {
			_ = os.Remove(path)

			continue
		}

		file, openErr := os.Open(path)
		if openErr != nil {
			continue
		}

		filter := boom.NewBloomFilter(f.perSpan, failedHashesFalseRate)
		_, readErr := filter.ReadFrom(file)
		_ = file.Close()

		if readErr == nil {
			f.generations[generation] = filter
		}
	}

	return f, nil
}

func (f *failedHashes) generation() int64 {
	return f.now().Unix() / int64(f.span/time.Second)
}

func (f *failedHashes) test(hash protocol.ID) bool {
	if f == nil {
		return false
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	oldest := f.generation() - f.keep

	for generation, filter := range f.generations {
		if generation > oldest && filter.Test(hash[:]) {
			return true
		}
	}

	return false
}

func (f *failedHashes) add(hash protocol.ID) {
	if f == nil {
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	generation := f.generation()

	filter, ok := f.generations[generation]
	if !ok {
		filter = boom.NewBloomFilter(f.perSpan, failedHashesFalseRate)
		f.generations[generation] = filter
	}

	filter.Add(hash[:])
	f.dirty[generation] = true
}

// save writes the hours that changed and drops the ones that expired.
func (f *failedHashes) save() error {
	if f == nil {
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	oldest := f.generation() - f.keep

	for generation := range f.generations {
		path := filepath.Join(f.dir, fmt.Sprintf("%s%d%s", failedHashesFilePrefix, generation, failedHashesFileSuffix))

		if generation <= oldest {
			delete(f.generations, generation)
			delete(f.dirty, generation)

			_ = os.Remove(path)

			continue
		}

		if !f.dirty[generation] {
			continue
		}

		file, err := os.CreateTemp(f.dir, "tmp-*")
		if err != nil {
			return err
		}

		_, writeErr := f.generations[generation].WriteTo(file)
		closeErr := file.Close()

		if writeErr != nil || closeErr != nil {
			_ = os.Remove(file.Name())

			if writeErr != nil {
				return writeErr
			}

			return closeErr
		}

		if err := os.Rename(file.Name(), path); err != nil {
			return err
		}

		delete(f.dirty, generation)
	}

	return nil
}
