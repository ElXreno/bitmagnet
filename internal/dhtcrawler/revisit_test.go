package dhtcrawler

import (
	"net/netip"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

func TestRevisitPlan(t *testing.T) {
	t.Parallel()

	r := newRevisits()
	if r == nil {
		t.Skip("revisits are switched off in this environment")
	}

	node := ktable.NewNode(protocol.ID{1}, netip.MustParseAddrPort("192.0.2.1:6881"))

	if got := r.plan(node, 20, 20, 20); got != "done_drained" {
		t.Fatalf("a node that gave all it stores: got %s", got)
	}

	if got := r.plan(node, 500, 20, revisitMinFresh-1); got != "done_low_yield" {
		t.Fatalf("a visit with too few new hashes: got %s", got)
	}

	for visit := 1; visit < revisitMaxVisits; visit++ {
		if got := r.plan(node, 500, 20, 20); got != "scheduled" {
			t.Fatalf("visit %d: got %s", visit, got)
		}
	}

	if got := r.plan(node, 500, 20, 20); got != "done_max_visits" {
		t.Fatalf("the visit cap: got %s", got)
	}

	if len(r.visits) != 0 {
		t.Fatal("a finished node must not stay in the visit counter")
	}

	if queued := len(r.queue); queued != revisitMaxVisits-1 {
		t.Fatalf("want %d queued revisits, got %d", revisitMaxVisits-1, queued)
	}

	var off *revisits

	off.forget(protocol.ID{1})

	if got := off.plan(node, 500, 20, 20); got != "off" {
		t.Fatalf("a disabled planner: got %s", got)
	}
}
