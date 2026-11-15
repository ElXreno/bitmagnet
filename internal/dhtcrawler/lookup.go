package dhtcrawler

import (
	"bytes"
	"context"
	"errors"
	"net/netip"
	"os"
	"strconv"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/server"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	lookupEnoughPeers    = 8
	lookupMaxPeers       = 16
	lookupDeadline       = 10 * time.Second
	lookupQueryTimeout   = 2 * time.Second
	lookupMaxLimiterWait = 50 * time.Millisecond
	lookupPoolSeeds      = 16
	lookupClosestK       = 8
)

var (
	lookupAlpha           = envInt("BITMAGNET_LOOKUP_ALPHA", 6)
	lookupMaxQueries      = envInt("BITMAGNET_LOOKUP_MAX_QUERIES", 12)
	lookupExtraAfterPeers = envInt("BITMAGNET_LOOKUP_EXTRA_AFTER_PEERS", 3)
)

func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}

	return fallback
}

type lookupNode struct {
	id       protocol.ID
	distance protocol.ID
	addr     netip.AddrPort
	queried  bool
}

func xorDistance(a protocol.ID, b protocol.ID) (d protocol.ID) {
	for i := range a {
		d[i] = a[i] ^ b[i]
	}

	return
}

func (c *crawler) lookupPeers(
	ctx context.Context,
	source string,
	hash protocol.ID,
	seeds []client.NodeInfo,
) []netip.AddrPort {
	candidates := make(map[netip.AddrPort]*lookupNode)
	add := func(id protocol.ID, addr netip.AddrPort) {
		if !addr.IsValid() || addr.Port() == 0 {
			return
		}

		if _, ok := candidates[addr]; !ok {
			candidates[addr] = &lookupNode{id: id, distance: xorDistance(id, hash), addr: addr}
		}
	}

	for _, n := range seeds {
		add(n.ID, n.Addr)
	}

	for _, n := range c.nodePool.closest(hash, lookupPoolSeeds) {
		add(n.ID, n.Addr)
	}

	for _, n := range c.kTable.GetClosestNodes(hash) {
		add(n.ID(), n.Addr())
	}

	peers := make(map[netip.AddrPort]struct{})
	queries := 0
	inflight := 0
	afterFirstPeers := 0
	completed := 0
	limited := 0

	var responded []protocol.ID

	firstPeersAt := 0
	stop := false

	type lookupResult struct {
		res     client.GetPeersResult
		ok      bool
		limited bool
		node    *lookupNode
	}

	results := make(chan lookupResult, 4*lookupMaxQueries)

	lookupCtx, cancel := context.WithTimeout(server.WithMaxLimiterWait(ctx, lookupMaxLimiterWait), lookupDeadline)
	defer cancel()

	for {
		for !stop && inflight < lookupAlpha && queries < lookupMaxQueries {
			var best *lookupNode

			for _, n := range candidates {
				if !n.queried && (best == nil || bytes.Compare(n.distance[:], best.distance[:]) < 0) {
					best = n
				}
			}

			if best == nil || fartherThanKth(best.distance, responded, lookupClosestK) {
				break
			}

			best.queried = true
			inflight++
			queries++

			go func(node *lookupNode) {
				addr := node.addr
				queryCtx, queryCancel := context.WithTimeout(lookupCtx, lookupQueryTimeout)
				defer queryCancel()

				res, err := c.client.GetPeers(queryCtx, addr, hash)
				results <- lookupResult{res: res, ok: err == nil, limited: errors.Is(err, server.ErrLimited), node: node}
			}(best)
		}

		if inflight == 0 {
			break
		}

		r := <-results
		inflight--

		if r.limited {
			queries--
			limited++

			continue
		}

		completed++

		if len(peers) > 0 {
			afterFirstPeers++
		}

		if r.ok {
			responded = append(responded, r.node.distance)
			c.nodePool.add(r.res.ID, r.node.addr)

			for _, v := range r.res.Values {
				if len(peers) < lookupMaxPeers {
					peers[v] = struct{}{}
				}
			}

			for _, n := range r.res.Nodes {
				add(n.ID, n.Addr)

				select {
				case c.discoveredNodes.In() <- ktable.NewNode(n.ID, n.Addr):
				default:
				}
			}
		}

		if firstPeersAt == 0 && len(peers) > 0 {
			firstPeersAt = completed
		}

		if len(peers) >= lookupEnoughPeers || afterFirstPeers >= lookupExtraAfterPeers || lookupCtx.Err() != nil {
			break
		}
	}

	c.countHash(source, stageLookupQueries, queries)
	c.countHash(source, "lookup_limited", limited)

	if len(peers) > 0 {
		c.lookupQueries.With(prometheus.Labels{"source": source, "result": "found"}).Observe(float64(firstPeersAt))
	} else {
		c.lookupQueries.With(prometheus.Labels{"source": source, "result": "empty"}).Observe(float64(completed))
	}

	result := make([]netip.AddrPort, 0, len(peers))
	for p := range peers {
		result = append(result, p)
	}

	return result
}

// fartherThanKth reports whether distance is beyond the k-th closest node that has answered, which is where a
// Kademlia lookup ends: nothing closer is left to ask.
func fartherThanKth(distance protocol.ID, responded []protocol.ID, k int) bool {
	if len(responded) < k {
		return false
	}

	closer := 0

	for _, d := range responded {
		if bytes.Compare(d[:], distance[:]) < 0 {
			closer++
		}
	}

	return closer >= k
}
