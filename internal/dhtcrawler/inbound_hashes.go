package dhtcrawler

import (
	"context"
	"net/netip"
)

func (c *crawler) runInboundHashes(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case h := <-c.inboundHashes:
			if c.inboundSeen.testAndAdd(h.InfoHash) {
				c.countHash(h.Query, stageDuplicate, 1)

				continue
			}

			c.countHash(h.Query, stageDiscovered, 1)

			select {
			case <-ctx.Done():
				return
			case c.inboundTriage.In() <- nodeHasPeersForHash{
				infoHash: h.InfoHash,
				node:     h.Node,
				source:   h.Query,
				peers:    []netip.AddrPort{h.Peer},
			}:
			}
		}
	}
}
