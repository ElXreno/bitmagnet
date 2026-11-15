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

func envIsOn(name string) bool {
	return os.Getenv(name) != "off"
}

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

var lookupIPv6 = os.Getenv("BITMAGNET_LOOKUP_IPV6") != "off"

// lookupPeers runs one lookup per address family side by side: a node only knows peers of its own family.
func (c *crawler) lookupPeers(
	ctx context.Context,
	source string,
	hash protocol.ID,
	seeds []client.NodeInfo,
) []netip.AddrPort {
	if c.v6 == nil || !lookupIPv6 {
		return c.lookupFamilyPeers(ctx, c.v4, source, hash, seeds)
	}

	found6 := make(chan []netip.AddrPort, 1)

	go func() {
		found6 <- c.lookupFamilyPeers(ctx, c.v6, source, hash, seeds)
	}()

	peers := c.lookupFamilyPeers(ctx, c.v4, source, hash, seeds)
	peers6 := <-found6

	if len(peers6) > 0 {
		c.countHash(source, stageLookupFound+"6", 1)
		c.countHash(source, stagePeersSum+"6", len(peers6))

		if len(peers) == 0 {
			c.countHash(source, stageLookupFound+"6_only", 1)
		}
	}

	return append(peers, peers6...)
}

func (c *crawler) lookupFamilyPeers(
	ctx context.Context,
	f *nodeFamily,
	source string,
	hash protocol.ID,
	seeds []client.NodeInfo,
) []netip.AddrPort {
	return c.lookupFamily(ctx, f, source, hash, seeds, nil)
}

type lookupToken struct {
	addr     netip.AddrPort
	distance protocol.ID
	token    string
}

func (c *crawler) lookupFamily(
	ctx context.Context,
	f *nodeFamily,
	source string,
	hash protocol.ID,
	seeds []client.NodeInfo,
	tokens *[]lookupToken,
) []netip.AddrPort {
	candidates := make(map[netip.AddrPort]*lookupNode)
	add := func(id protocol.ID, addr netip.AddrPort) {
		if !addr.IsValid() || addr.Port() == 0 || c.family(addr.Addr()) != f {
			return
		}

		if _, ok := candidates[addr]; !ok {
			candidates[addr] = &lookupNode{id: id, distance: xorDistance(id, hash), addr: addr}
		}
	}

	for _, n := range seeds {
		add(n.ID, n.Addr)
	}

	for _, n := range f.nodePool.closest(hash, lookupPoolSeeds) {
		add(n.ID, n.Addr)
	}

	for _, n := range f.kTable.GetClosestNodes(hash) {
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
			f.nodePool.add(r.res.ID, r.node.addr)

			if tokens != nil && r.res.Token != "" {
				*tokens = append(*tokens, lookupToken{
					addr:     r.node.addr,
					distance: r.node.distance,
					token:    r.res.Token,
				})
			}

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

	c.countHash(source, stageLookupQueries+f.suffix, queries)
	c.countHash(source, "lookup_limited"+f.suffix, limited)

	if len(peers) > 0 {
		c.lookupQueries.With(prometheus.Labels{"source": source, "result": "found" + f.suffix}).
			Observe(float64(firstPeersAt))
	} else {
		c.lookupQueries.With(prometheus.Labels{"source": source, "result": "empty" + f.suffix}).
			Observe(float64(completed))
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
