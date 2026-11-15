package dhtcrawler

import (
	"bytes"
	"context"
	"net/netip"
	"sort"
	"sync"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
)

const (
	nodePoolSize            = 1 << 18
	nodePoolRebuildInterval = 3 * time.Second
)

// nodePool remembers nodes that recently answered a query, ordered by ID, so that a lookup can start from
// nodes that already share a long prefix with its target instead of the few hundred nodes of the routing table.
type nodePool struct {
	mu     sync.Mutex
	ring   []client.NodeInfo
	next   int
	full   bool
	sorted []client.NodeInfo
}

func newNodePool() *nodePool {
	return &nodePool{ring: make([]client.NodeInfo, nodePoolSize)}
}

func (p *nodePool) add(id protocol.ID, addr netip.AddrPort) {
	if id.IsZero() || !addr.IsValid() || addr.Port() == 0 {
		return
	}

	p.mu.Lock()
	p.ring[p.next] = client.NodeInfo{ID: id, Addr: addr}
	p.next++

	if p.next == len(p.ring) {
		p.next = 0
		p.full = true
	}

	p.mu.Unlock()
}

func (p *nodePool) rebuild() {
	p.mu.Lock()

	n := p.next
	if p.full {
		n = len(p.ring)
	}

	nodes := make([]client.NodeInfo, n)
	copy(nodes, p.ring[:n])
	p.mu.Unlock()

	sort.Slice(nodes, func(i, j int) bool {
		return bytes.Compare(nodes[i].ID[:], nodes[j].ID[:]) < 0
	})

	unique := nodes[:0]

	for i, node := range nodes {
		if i == 0 || node.ID != nodes[i-1].ID {
			unique = append(unique, node)
		}
	}

	p.mu.Lock()
	p.sorted = unique
	p.mu.Unlock()
}

func (p *nodePool) size() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.sorted)
}

func (p *nodePool) closest(target protocol.ID, n int) []client.NodeInfo {
	p.mu.Lock()
	sorted := p.sorted
	p.mu.Unlock()

	if len(sorted) == 0 {
		return nil
	}

	pos := sort.Search(len(sorted), func(i int) bool {
		return bytes.Compare(sorted[i].ID[:], target[:]) >= 0
	})

	from := max(0, pos-n/2)
	to := min(len(sorted), from+n)
	from = max(0, to-n)

	return sorted[from:to]
}

func (c *crawler) runNodePool(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(nodePoolRebuildInterval):
			c.nodePool.rebuild()
			c.stageGauge.With(prometheusLabels("node_pool", "queued")).Set(float64(c.nodePool.size()))
		}
	}
}
