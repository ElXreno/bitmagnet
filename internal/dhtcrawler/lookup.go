package dhtcrawler

import (
	"bytes"
	"context"
	"net/netip"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
)

const (
	lookupAlpha        = 6
	lookupDeadline     = 10 * time.Second
	lookupMaxQueries   = 24
	lookupEnoughPeers  = 8
	lookupMaxPeers     = 16
	lookupQueryTimeout = 2 * time.Second
)

type lookupNode struct {
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
			candidates[addr] = &lookupNode{distance: xorDistance(id, hash), addr: addr}
		}
	}

	for _, n := range seeds {
		add(n.ID, n.Addr)
	}

	for _, n := range c.kTable.GetClosestNodes(hash) {
		add(n.ID(), n.Addr())
	}

	peers := make(map[netip.AddrPort]struct{})
	queries := 0
	inflight := 0
	afterFirstPeers := 0
	stop := false

	type lookupResult struct {
		res client.GetPeersResult
		ok  bool
	}

	results := make(chan lookupResult, lookupMaxQueries)

	lookupCtx, cancel := context.WithTimeout(ctx, lookupDeadline)
	defer cancel()

	for {
		for !stop && inflight < lookupAlpha && queries < lookupMaxQueries {
			var best *lookupNode

			for _, n := range candidates {
				if !n.queried && (best == nil || bytes.Compare(n.distance[:], best.distance[:]) < 0) {
					best = n
				}
			}

			if best == nil {
				break
			}

			best.queried = true
			inflight++
			queries++

			go func(addr netip.AddrPort) {
				queryCtx, queryCancel := context.WithTimeout(lookupCtx, lookupQueryTimeout)
				defer queryCancel()

				res, err := c.client.GetPeers(queryCtx, addr, hash)
				results <- lookupResult{res: res, ok: err == nil}
			}(best.addr)
		}

		if inflight == 0 {
			break
		}

		r := <-results
		inflight--

		if len(peers) > 0 {
			afterFirstPeers++
		}

		if r.ok {
			for _, v := range r.res.Values {
				if len(peers) < lookupMaxPeers {
					peers[v] = struct{}{}
				}
			}

			for _, n := range r.res.Nodes {
				add(n.ID, n.Addr)
			}
		}

		if len(peers) >= lookupEnoughPeers || afterFirstPeers >= lookupAlpha || lookupCtx.Err() != nil {
			stop = true
		}
	}

	c.countHash(source, stageLookupQueries, queries)

	result := make([]netip.AddrPort, 0, len(peers))
	for p := range peers {
		result = append(result, p)
	}

	return result
}
