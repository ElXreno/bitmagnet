package dhtcrawler

import (
	"hash/fnv"
	"net/netip"
	"os"
	"sync"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

const (
	sampleScheduleFloor  = time.Minute
	sampleScheduleForget = 8 * time.Hour
)

type sampleVisit struct {
	fingerprint uint64
	next        int64
}

// sampleSchedule remembers the sample each node last showed: a node keeps one sample for its whole interval,
// so it is asked rarely until the sample changes and from then on once per interval
type sampleSchedule struct {
	mu     sync.Mutex
	probe  time.Duration
	visits map[netip.AddrPort]sampleVisit
	now    func() time.Time
	swept  time.Time
}

func sampleScheduleFromEnv() *sampleSchedule {
	if os.Getenv("BITMAGNET_SAMPLE_SCHEDULE") != "on" {
		return nil
	}

	return newSampleSchedule(time.Duration(envInt("BITMAGNET_SAMPLE_PROBE_SECONDS", 1800)) * time.Second)
}

func newSampleSchedule(probe time.Duration) *sampleSchedule {
	return &sampleSchedule{probe: probe, visits: make(map[netip.AddrPort]sampleVisit), now: time.Now}
}

func (s *sampleSchedule) due(addr netip.AddrPort) bool {
	if s == nil {
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	visit, ok := s.visits[addr]

	return !ok || visit.next <= s.now().Unix()
}

// record plans the next visit and tells what the answer was: first, same or changed
func (s *sampleSchedule) record(addr netip.AddrPort, samples []protocol.ID, interval time.Duration) string {
	if s == nil {
		return ""
	}

	digest := fnv.New64a()
	for _, sample := range samples {
		_, _ = digest.Write(sample[:])
	}

	fingerprint := digest.Sum64()
	interval = min(max(interval, sampleScheduleFloor), sampleScheduleForget)

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	s.sweep(now)

	before, known := s.visits[addr]
	wait, outcome := min(s.probe, interval), "schedule_first"

	switch {
	case known && before.fingerprint != fingerprint:
		wait, outcome = interval, "schedule_changed"
	case known:
		outcome = "schedule_same"
	}

	s.visits[addr] = sampleVisit{fingerprint: fingerprint, next: now.Add(wait).Unix()}

	return outcome
}

func (s *sampleSchedule) sweep(now time.Time) {
	if now.Sub(s.swept) < 10*time.Minute {
		return
	}

	s.swept = now
	oldest := now.Add(-sampleScheduleForget).Unix()

	for addr, visit := range s.visits {
		if visit.next < oldest {
			delete(s.visits, addr)
		}
	}
}

func (s *sampleSchedule) size() int {
	if s == nil {
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.visits)
}
