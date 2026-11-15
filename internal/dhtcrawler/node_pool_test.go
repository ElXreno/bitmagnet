package dhtcrawler

import (
	"net/netip"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

func TestNodePoolClosest(t *testing.T) {
	t.Parallel()

	p := newNodePool()
	addr := netip.MustParseAddrPort("192.0.2.1:6881")

	if got := p.closest(protocol.ID{5}, 4); got != nil {
		t.Fatalf("empty pool: got %v", got)
	}

	for i := 1; i <= 20; i++ {
		p.add(protocol.ID{byte(i * 10)}, addr)
	}

	p.add(protocol.ID{100}, addr)
	p.add(protocol.ID{}, addr)
	p.rebuild()

	if p.size() != 20 {
		t.Fatalf("want 20 unique nodes, got %d", p.size())
	}

	got := p.closest(protocol.ID{101}, 4)
	if len(got) != 4 {
		t.Fatalf("want 4 nodes, got %d", len(got))
	}

	want := []byte{90, 100, 110, 120}
	for i, node := range got {
		if node.ID[0] != want[i] {
			t.Fatalf("node %d: want %d, got %d", i, want[i], node.ID[0])
		}
	}

	if low := p.closest(protocol.ID{0}, 4); len(low) != 4 || low[0].ID[0] != 10 {
		t.Fatalf("low edge: %v", low)
	}

	if high := p.closest(protocol.ID{255}, 4); len(high) != 4 || high[3].ID[0] != 200 {
		t.Fatalf("high edge: %v", high)
	}
}

func TestFartherThanKth(t *testing.T) {
	t.Parallel()

	responded := []protocol.ID{{1}, {2}, {3}}

	if fartherThanKth(protocol.ID{9}, responded[:2], 3) {
		t.Fatal("fewer than k answers: the lookup must go on")
	}

	if !fartherThanKth(protocol.ID{9}, responded, 3) {
		t.Fatal("k closer answers: the lookup is over")
	}

	if fartherThanKth(protocol.ID{2}, responded, 3) {
		t.Fatal("a candidate inside the k closest must still be asked")
	}
}
