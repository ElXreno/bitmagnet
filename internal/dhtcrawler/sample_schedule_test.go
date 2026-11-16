package dhtcrawler

import (
	"net/netip"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

func TestSampleScheduleAsksRarelyUntilTheSampleChanges(t *testing.T) {
	t.Parallel()

	clock := time.Unix(1_700_000_000, 0)
	s := newSampleSchedule(30 * time.Minute)
	s.now = func() time.Time { return clock }

	node := netip.MustParseAddrPort("192.0.2.1:6881")
	first, second := []protocol.ID{{1}, {2}}, []protocol.ID{{3}, {4}}
	interval := 6 * time.Hour

	if !s.due(node) {
		t.Fatal("an unknown node is due")
	}

	for _, step := range []struct {
		after   time.Duration
		due     bool
		samples []protocol.ID
		outcome string
	}{
		{0, true, first, "schedule_first"},
		{29 * time.Minute, false, nil, ""},
		{time.Minute, true, first, "schedule_same"},
		{30 * time.Minute, true, second, "schedule_changed"},
		{6*time.Hour - time.Second, false, nil, ""},
		{time.Second, true, second, "schedule_same"},
		{30 * time.Minute, true, first, "schedule_changed"},
	} {
		clock = clock.Add(step.after)

		if got := s.due(node); got != step.due {
			t.Fatalf("at +%s due: want %v, got %v", clock.Sub(time.Unix(1_700_000_000, 0)), step.due, got)
		}

		if step.samples == nil {
			continue
		}

		if got := s.record(node, step.samples, interval); got != step.outcome {
			t.Fatalf("at +%s: want %s, got %s", clock.Sub(time.Unix(1_700_000_000, 0)), step.outcome, got)
		}
	}

	clock = clock.Add(sampleScheduleForget + 7*time.Hour)

	s.record(netip.MustParseAddrPort("192.0.2.2:6881"), first, interval)

	if s.size() != 1 {
		t.Fatalf("a node not seen for long must be forgotten, %d kept", s.size())
	}

	short := netip.MustParseAddrPort("192.0.2.3:6881")
	s.record(short, first, 10*time.Second)

	clock = clock.Add(59 * time.Second)

	if s.due(short) {
		t.Fatal("a node with a tiny interval is still left alone for a minute")
	}

	var off *sampleSchedule

	if !off.due(node) || off.record(node, first, interval) != "" {
		t.Fatal("without a schedule every node is due and nothing is recorded")
	}
}
