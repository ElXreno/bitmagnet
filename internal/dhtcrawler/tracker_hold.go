package dhtcrawler

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/netip"
	"sort"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/tracker"
	"github.com/prometheus/client_golang/prometheus"
)

type joinVerdict int

const (
	joinNone joinVerdict = iota
	joinMember
	joinHeld
)

// one limiter token has always paid for started plus stopped
const announceTokenPackets = 2

type joinFunc func(s *trackerScraper, index int, hash protocol.ID, seeders uint32) joinVerdict

func (s *trackerScraper) enableHolds(peerID protocol.ID, port uint16, capacity int, join joinFunc) bool {
	if s == nil || !s.announceEnabled {
		return false
	}

	s.stops = make([]chan protocol.ID, len(s.hosts))
	s.extras = make([]chan protocol.ID, len(s.hosts))

	for i := range s.stops {
		s.stops[i] = make(chan protocol.ID, capacity)
		s.extras[i] = make(chan protocol.ID, trackerAnnounceQueue)
	}

	s.held = make(map[protocol.ID]uint32)
	s.holdParams = tracker.AnnounceParams{
		PeerID:  peerID,
		Key:     rand.Uint32(),
		Port:    port,
		NumWant: s.announceParams.NumWant,
	}
	s.join = join

	return true
}

func (s *trackerScraper) countHold(index int, result string) {
	s.metrics.packets.With(prometheus.Labels{"tracker": s.hosts[index], "result": result}).Inc()
}

func (s *trackerScraper) holding(hash protocol.ID) bool {
	s.holdMu.Lock()
	defer s.holdMu.Unlock()

	return s.held[hash] != 0
}

func (s *trackerScraper) markHeld(hash protocol.ID, index int) {
	s.holdMu.Lock()
	s.held[hash] |= 1 << index
	s.holdMu.Unlock()
}

// wantExtras queues a membership on up to limit more trackers that saw seeders, the idlest first
func (s *trackerScraper) wantExtras(hash protocol.ID, limit int) {
	if s == nil || s.extras == nil {
		return
	}

	info, ok := s.cache.Get(hash)
	if !ok {
		return
	}

	s.holdMu.Lock()
	started := s.held[hash]
	s.held[hash] = started
	s.holdMu.Unlock()

	var candidates []int

	for index := range s.extras {
		if info.seededBy&^started&(1<<index) != 0 {
			candidates = append(candidates, index)
		}
	}

	sort.SliceStable(candidates, func(a, b int) bool {
		return len(s.extras[candidates[a]]) < len(s.extras[candidates[b]])
	})

	for _, index := range candidates[:min(limit, len(candidates))] {
		select {
		case s.extras[index] <- hash:
		default:
			s.countHold(index, "extra_queue_full")
		}
	}
}

func (s *trackerScraper) nextExtra(index int) (protocol.ID, bool) {
	for {
		select {
		case hash := <-s.extras[index]:
			s.holdMu.Lock()

			started, wanted := s.held[hash]
			fresh := wanted && started&(1<<index) == 0

			if fresh {
				s.held[hash] = started | 1<<index
			}

			s.holdMu.Unlock()

			if fresh {
				return hash, true
			}

			s.countHold(index, "extra_stale")
		default:
			return protocol.ID{}, false
		}
	}
}

func (s *trackerScraper) release(hash protocol.ID) {
	if s == nil || s.stops == nil {
		return
	}

	s.holdMu.Lock()
	started := s.held[hash]
	delete(s.held, hash)
	s.holdMu.Unlock()

	for index := range s.stops {
		if started&(1<<index) != 0 {
			s.queueStop(index, hash)
		}
	}
}

func (s *trackerScraper) queueStop(index int, hash protocol.ID) {
	select {
	case s.stops[index] <- hash:
	default:
		s.countHold(index, "hold_stop_dropped")
	}
}

func (s *trackerScraper) spendToken(index int) []announceJob {
	var jobs []announceJob

	packets := 0

	for packets == 0 && !s.draining.Load() {
		job, ok := s.nextAnnounce(index)
		if !ok {
			break
		}

		verdict := joinNone
		if s.join != nil {
			verdict = s.join(s, index, job.hash, job.seeders)
		}

		switch verdict {
		case joinHeld:
			continue
		case joinMember:
			job.hold = true
			packets = 1
		case joinNone:
			packets = announceTokenPackets
		}

		jobs = append(jobs, job)
	}

	// an extra membership takes only a token that no normal announce is waiting for
	if packets == 0 && s.extras != nil && !s.draining.Load() {
		if hash, ok := s.nextExtra(index); ok {
			jobs = append(jobs, announceJob{hash: hash, hold: true, extra: true})
			packets = 1

			if s.onExtra != nil {
				s.onExtra(hash)
			}
		}
	}

	for ; packets < announceTokenPackets && s.stops != nil; packets++ {
		select {
		case hash := <-s.stops[index]:
			jobs = append(jobs, announceJob{hash: hash, stop: true})
		default:
			return jobs
		}
	}

	return jobs
}

func (s *trackerScraper) announceMember(ctx context.Context, index int, host string, job announceJob) {
	result, err := s.client.AnnounceStart(ctx, index, job.hash, s.holdParams)

	// the stop queued by release may have left before this start arrived
	if !s.holding(job.hash) {
		s.queueStop(index, job.hash)
	}

	switch {
	case err == nil:
		s.countHold(index, "hold_ok")

		if s.onHeld != nil {
			s.onHeld(job.hash)
		}
	case errors.Is(err, tracker.ErrTimeout):
		s.countHold(index, "hold_timeout")
	default:
		s.countHold(index, "hold_error")
	}

	s.announced(ctx, host, job.hash, job.seeders, result.Peers, err)
}

func (s *trackerScraper) announceExtra(ctx context.Context, index int, hash protocol.ID) {
	params := s.holdParams
	params.NumWant = 0

	_, err := s.client.AnnounceStart(ctx, index, hash, params)

	if !s.holding(hash) {
		s.queueStop(index, hash)
	}

	switch {
	case err == nil:
		s.countHold(index, "extra_ok")
	case errors.Is(err, tracker.ErrTimeout):
		s.countHold(index, "extra_timeout")
	default:
		s.countHold(index, "extra_error")
	}
}

func (s *trackerScraper) announced(
	ctx context.Context,
	host string,
	hash protocol.ID,
	seeders uint32,
	peers []netip.AddrPort,
	err error,
) {
	switch {
	case err == nil && len(peers) > 0:
		s.metrics.packets.With(prometheus.Labels{"tracker": host, "result": "announce_ok"}).Inc()

		if s.onPeers != nil {
			s.onPeers(ctx, hash, s.prefix+trackerSource(seeders), peers)
		}
	case err == nil:
		s.metrics.packets.With(prometheus.Labels{"tracker": host, "result": "announce_no_peers"}).Inc()
	case errors.Is(err, tracker.ErrTimeout):
		s.metrics.packets.With(prometheus.Labels{"tracker": host, "result": "announce_timeout"}).Inc()
	default:
		s.metrics.packets.With(prometheus.Labels{"tracker": host, "result": "announce_error"}).Inc()
	}
}

func (s *trackerScraper) announceStop(ctx context.Context, index int, hash protocol.ID) {
	if err := s.client.AnnounceStop(ctx, index, hash, s.holdParams); err != nil {
		s.countHold(index, "hold_stop_error")

		return
	}

	s.countHold(index, "hold_stop")
}
