package dhtcrawler

import (
	"bytes"
	"context"
	"net/netip"
	"sync"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
)

const (
	lookupAlpha        = 4
	lookupMaxRounds    = 6
	lookupMaxQueries   = 24
	lookupEnoughPeers  = 8
	lookupMaxPeers     = 16
	lookupQueryTimeout = time.Second
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
	rounds := 0

	for rounds < lookupMaxRounds && queries < lookupMaxQueries && ctx.Err() == nil {
		next := make([]*lookupNode, 0, lookupAlpha)

		for len(next) < lookupAlpha {
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
			next = append(next, best)
		}

		if len(next) == 0 {
			break
		}

		results := make([]client.GetPeersResult, len(next))
		oks := make([]bool, len(next))

		var wg sync.WaitGroup

		for i, n := range next {
			wg.Add(1)

			go func() {
				defer wg.Done()

				queryCtx, cancel := context.WithTimeout(ctx, lookupQueryTimeout)
				defer cancel()

				res, err := c.client.GetPeers(queryCtx, n.addr, hash)
				results[i], oks[i] = res, err == nil
			}()
		}

		wg.Wait()

		rounds++
		queries += len(next)
		hadPeers := len(peers) > 0

		for i, res := range results {
			if !oks[i] {
				continue
			}

			for _, v := range res.Values {
				if len(peers) < lookupMaxPeers {
					peers[v] = struct{}{}
				}
			}

			for _, n := range res.Nodes {
				add(n.ID, n.Addr)
			}
		}

		if len(peers) >= lookupEnoughPeers || hadPeers {
			break
		}
	}

	c.countHash(source, stageLookupQueries, queries)
	c.countHash(source, stageLookupRounds, rounds)

	result := make([]netip.AddrPort, 0, len(peers))
	for p := range peers {
		result = append(result, p)
	}

	return result
}
