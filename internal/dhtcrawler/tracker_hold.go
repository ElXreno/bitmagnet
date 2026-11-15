package dhtcrawler

import (
	"context"
	"errors"
	"math/rand/v2"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/tracker"
	"github.com/prometheus/client_golang/prometheus"
)

type holdJob struct {
	hash protocol.ID
	stop bool
}

type heldSwarm struct {
	started  uint32
	priority int
}

func (s *trackerScraper) enableHolds(peerID protocol.ID, port uint16) bool {
	if s == nil || !s.announceEnabled {
		return false
	}

	s.holdQueues = make([][announcePriorities]chan holdJob, len(s.hosts))

	for i := range s.holdQueues {
		for priority := range s.holdQueues[i] {
			s.holdQueues[i][priority] = make(chan holdJob, trackerAnnounceQueue)
		}
	}

	s.held = make(map[protocol.ID]heldSwarm)
	s.holdParams = tracker.AnnounceParams{PeerID: peerID, Key: rand.Uint32(), Port: port}

	return true
}

func (s *trackerScraper) holdableSeeders(hash protocol.ID) uint32 {
	if s == nil || s.holdQueues == nil {
		return 0
	}

	info, _ := s.cache.Get(hash)

	return info.seeders
}

func (s *trackerScraper) countHold(index int, result string) {
	s.metrics.packets.With(prometheus.Labels{"tracker": s.hosts[index], "result": result}).Inc()
}

func (s *trackerScraper) hold(hash protocol.ID) {
	if s == nil || s.holdQueues == nil {
		return
	}

	info, ok := s.cache.Get(hash)
	if !ok || info.seeders == 0 {
		return
	}

	priority := announcePriority(info.seeders)

	s.holdMu.Lock()

	if _, held := s.held[hash]; !held {
		s.held[hash] = heldSwarm{priority: priority}
	}

	s.holdMu.Unlock()

	for index := range s.holdQueues {
		if info.seededBy&(1<<index) == 0 {
			continue
		}

		select {
		case s.holdQueues[index][priority] <- holdJob{hash: hash}:
		default:
			s.countHold(index, "hold_queue_full")
		}
	}
}

func (s *trackerScraper) release(hash protocol.ID) {
	if s == nil || s.holdQueues == nil {
		return
	}

	s.holdMu.Lock()
	swarm, held := s.held[hash]
	delete(s.held, hash)
	s.holdMu.Unlock()

	if !held {
		return
	}

	for index := range s.holdQueues {
		if swarm.started&(1<<index) == 0 {
			continue
		}

		select {
		case s.holdQueues[index][swarm.priority] <- holdJob{hash: hash, stop: true}:
		default:
			s.countHold(index, "hold_stop_dropped")
		}
	}
}

func (s *trackerScraper) startHold(hash protocol.ID, index int) bool {
	s.holdMu.Lock()
	defer s.holdMu.Unlock()

	swarm, held := s.held[hash]
	if !held {
		return false
	}

	swarm.started |= 1 << index
	s.held[hash] = swarm

	return true
}

func (s *trackerScraper) nextHold(index int, priority int) (announceJob, bool) {
	if s.holdQueues == nil {
		return announceJob{}, false
	}

	for {
		select {
		case job := <-s.holdQueues[index][priority]:
			if job.stop || s.startHold(job.hash, index) {
				return announceJob{hash: job.hash, hold: !job.stop, stop: job.stop}, true
			}

			s.countHold(index, "hold_stale")
		default:
			return announceJob{}, false
		}
	}
}

func (s *trackerScraper) announceHold(ctx context.Context, index int, host string, hash protocol.ID) {
	_, err := s.client.AnnounceStart(ctx, index, hash, s.holdParams)

	switch {
	case err == nil:
		s.metrics.packets.With(prometheus.Labels{"tracker": host, "result": "hold_ok"}).Inc()

		if s.onHeld != nil {
			s.onHeld(hash)
		}
	case errors.Is(err, tracker.ErrTimeout):
		s.metrics.packets.With(prometheus.Labels{"tracker": host, "result": "hold_timeout"}).Inc()
	default:
		s.metrics.packets.With(prometheus.Labels{"tracker": host, "result": "hold_error"}).Inc()
	}
}

func (s *trackerScraper) announceStop(ctx context.Context, index int, host string, hash protocol.ID) {
	if err := s.client.AnnounceStop(ctx, index, hash, s.holdParams); err != nil {
		s.metrics.packets.With(prometheus.Labels{"tracker": host, "result": "hold_stop_error"}).Inc()

		return
	}

	s.metrics.packets.With(prometheus.Labels{"tracker": host, "result": "hold_stop"}).Inc()
}
