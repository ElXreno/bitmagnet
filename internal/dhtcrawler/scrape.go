package dhtcrawler

import (
	"context"
	"fmt"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

func (c *crawler) runScrape(ctx context.Context) {
	_ = c.scrape.Run(ctx, func(req nodeHasPeersForHash) {
		pfh, pfhErr := c.requestScrape(ctx, req)
		if pfhErr != nil {
			c.countClass(req, "scrape_error", 1)

			return
		}

		c.countClass(req, "scrape_ok", 1)
		select {
		case <-ctx.Done():
		case c.persistSources.In() <- infoHashWithScrape{
			nodeHasPeersForHash: req,
			bfsd:                pfh.bfsd,
			bfpe:                pfh.bfpe,
		}:
		}
	})
}

// requestScrape requests a scrape from a node to find seeders/leechers for a given info hash;
// see https://www.bittorrent.org/beps/bep_0033.html
func (c *crawler) requestScrape(
	ctx context.Context,
	req nodeHasPeersForHash,
) (infoHashWithScrape, error) {
	kTable := c.family(req.node.Addr()).kTable

	res, err := c.client.GetPeersScrape(ctx, req.node, req.infoHash)
	if err != nil {
		kTable.BatchCommand(ktable.DropAddr{
			Addr:   req.node.Addr(),
			Reason: fmt.Errorf("failed to get peers from p: %w", err),
		})

		return infoHashWithScrape{}, err
	}

	kTable.BatchCommand(ktable.PutNode{
		ID:      res.ID,
		Addr:    req.node,
		Options: []ktable.NodeOption{ktable.NodeResponded()},
	})

	if len(res.Nodes) > 0 {
		for _, n := range res.Nodes {
			select {
			case c.discoveredNodes.In() <- ktable.NewNode(n.ID, n.Addr):
			default:
			}
		}
	}

	return infoHashWithScrape{
		nodeHasPeersForHash: req,
		bfsd:                res.BfSeeders,
		bfpe:                res.BfPeers,
	}, nil
}
