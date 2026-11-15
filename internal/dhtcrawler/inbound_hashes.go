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
			source := h.Query + c.family(h.Node.Addr()).suffix

			if c.inboundSeen.testAndAdd(h.InfoHash) {
				c.countHash(source, stageDuplicate, 1)

				continue
			}

			c.countHash(source, stageDiscovered, 1)

			select {
			case <-ctx.Done():
				return
			case c.inboundTriage.In() <- nodeHasPeersForHash{
				infoHash: h.InfoHash,
				node:     h.Node,
				source:   source,
				peers:    []netip.AddrPort{h.Peer},
			}:
			}
		}
	}
}
