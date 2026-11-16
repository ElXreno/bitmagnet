package dhtcrawler

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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
// filter per hour and forgets an hour as a whole, or, when smooth, hash by hash over the following hour.
type failedHashes struct {
	mu          sync.Mutex
	saving      sync.Mutex
	dir         string
	span        time.Duration
	perSpan     uint
	keep        int64
	smooth      bool
	generations map[int64]*boom.BloomFilter
	dirty       map[int64]bool
	now         func() time.Time
}

func newFailedHashes(
	dir string, ttl time.Duration, span time.Duration, perSpan uint, smooth ...bool,
) (*failedHashes, error) {
	if ttl <= 0 {
		return nil, nil //nolint:nilnil
	}

	f := &failedHashes{
		dir:         dir,
		span:        span,
		perSpan:     perSpan,
		keep:        int64(ttl / span),
		smooth:      len(smooth) > 0 && smooth[0],
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

		if generation <= current-f.keep-f.extra() || generation > current {
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

	if filter, ok := f.generations[oldest]; ok && f.smooth {
		return f.position(hash) >= f.phase() && filter.Test(hash[:])
	}

	return false
}

// extra is how many generations past their term stay loaded: the one being released and the one before it
func (f *failedHashes) extra() int64 {
	if f.smooth {
		return 2
	}

	return 0
}

// phase is the part of the current span that has passed, from 0 to 1
func (f *failedHashes) phase() float64 {
	span := int64(f.span / time.Second)

	return float64(f.now().Unix()%span) / float64(span)
}

// position spreads the hashes of a generation over the span in which it is released
func (*failedHashes) position(hash protocol.ID) float64 {
	return float64(binary.BigEndian.Uint32(hash[16:])) / (1 << 32)
}

// released says whether a hash that test no longer holds was given up on within the last two spans
func (f *failedHashes) released(hash protocol.ID) bool {
	if f == nil || !f.smooth {
		return false
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	oldest := f.generation() - f.keep

	for _, generation := range []int64{oldest, oldest - 1} {
		if filter, ok := f.generations[generation]; ok && filter.Test(hash[:]) {
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

	f.saving.Lock()
	defer f.saving.Unlock()

	type snapshot struct {
		generation int64
		path       string
		data       bytes.Buffer
	}

	var snapshots []*snapshot

	f.mu.Lock()

	oldest := f.generation() - f.keep - f.extra()

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

		taken := &snapshot{generation: generation, path: path}
		if _, err := f.generations[generation].WriteTo(&taken.data); err != nil {
			f.mu.Unlock()

			return err
		}

		delete(f.dirty, generation)

		snapshots = append(snapshots, taken)
	}

	f.mu.Unlock()

	for n, taken := range snapshots {
		if err := writeFileAtomic(taken.path, func(w io.Writer) error {
			_, err := taken.data.WriteTo(w)

			return err
		}); err != nil {
			f.mu.Lock()

			for _, unsaved := range snapshots[n:] {
				if _, ok := f.generations[unsaved.generation]; ok {
					f.dirty[unsaved.generation] = true
				}
			}

			f.mu.Unlock()

			return err
		}
	}

	return nil
}
