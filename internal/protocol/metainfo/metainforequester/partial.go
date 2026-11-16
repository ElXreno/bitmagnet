package metainforequester

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

const partialIdle = 10 * time.Minute

type partialKey struct {
	hash protocol.ID
	size uint
}

type partial struct {
	key     partialKey
	pieces  map[uint][]byte
	bytes   int64
	touched time.Time
	order   *list.Element
}

// partialStore keeps the pieces of a meta info between connections, so the next peer sends only the rest
type partialStore struct {
	mu      sync.Mutex
	parts   map[partialKey]*partial
	order   *list.List
	done    map[protocol.ID]time.Time
	pending *atomic.Int64
	budget  int64
	now     func() time.Time
	swept   time.Time
}

func newPartialStore(pending *atomic.Int64, budget int64) *partialStore {
	return &partialStore{
		parts:   make(map[partialKey]*partial),
		order:   list.New(),
		done:    make(map[protocol.ID]time.Time),
		pending: pending,
		budget:  budget,
		now:     time.Now,
	}
}

func pieceCount(size uint) uint {
	return (size + metadataPieceSize - 1) / metadataPieceSize
}

func (s *partialStore) forget(part *partial) {
	s.pending.Add(-part.bytes)
	s.order.Remove(part.order)
	delete(s.parts, part.key)
}

func (s *partialStore) touch(part *partial) {
	part.touched = s.now()
	s.order.MoveToBack(part.order)
}

// missing lists the pieces no connection brought yet
func (s *partialStore) missing(hash protocol.ID, size uint) ([]uint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sweep()

	if _, done := s.done[hash]; done {
		return nil, ErrFetchedElsewhere
	}

	key := partialKey{hash: hash, size: size}

	part, ok := s.parts[key]
	if !ok {
		part = &partial{key: key, pieces: make(map[uint][]byte)}
		part.order = s.order.PushBack(part)
		s.parts[key] = part
	}

	s.touch(part)

	missing := make([]uint, 0, pieceCount(size)-uint(len(part.pieces)))

	for index := range pieceCount(size) {
		if _, held := part.pieces[index]; !held {
			missing = append(missing, index)
		}
	}

	return missing, nil
}

// put stores a piece and hands out the whole meta info to the connection that completes it
func (s *partialStore) put(hash protocol.ID, size uint, index uint, piece []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	part, ok := s.parts[partialKey{hash: hash, size: size}]
	if !ok {
		if _, done := s.done[hash]; done {
			return nil, ErrFetchedElsewhere
		}

		return nil, ErrIncomingBudget
	}

	s.touch(part)

	if _, held := part.pieces[index]; !held {
		part.pieces[index] = piece
		part.bytes += int64(len(piece))

		if s.pending.Add(int64(len(piece))) > s.budget {
			s.evict(part)
		}
	}

	if uint(len(part.pieces)) < pieceCount(size) {
		return nil, nil
	}

	whole := make([]byte, 0, size)
	for index := range pieceCount(size) {
		whole = append(whole, part.pieces[index]...)
	}

	s.forget(part)
	s.done[hash] = s.now()

	return whole, nil
}

// retry lets a meta info that failed its check be collected again
func (s *partialStore) retry(hash protocol.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.done, hash)
}

func (s *partialStore) evict(keep *partial) {
	for s.pending.Load() > s.budget {
		oldest := s.order.Front()
		if oldest != nil && oldest.Value == any(keep) {
			oldest = oldest.Next()
		}

		if oldest == nil {
			return
		}

		part, _ := oldest.Value.(*partial)

		peerBytesTotal.WithLabelValues("partial_evicted").Add(float64(part.bytes))
		s.forget(part)
	}
}

func (s *partialStore) sweep() {
	now := s.now()
	if now.Sub(s.swept) < time.Minute {
		return
	}

	s.swept = now

	for oldest := s.order.Front(); oldest != nil; oldest = s.order.Front() {
		part, _ := oldest.Value.(*partial)
		if now.Sub(part.touched) <= partialIdle {
			break
		}

		peerBytesTotal.WithLabelValues("partial_idle").Add(float64(part.bytes))
		s.forget(part)
	}

	for hash, at := range s.done {
		if now.Sub(at) > partialIdle {
			delete(s.done, hash)
		}
	}
}
