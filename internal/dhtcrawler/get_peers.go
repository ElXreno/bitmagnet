package dhtcrawler

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

var errNoPeers = errors.New("no peers found")

func (c *crawler) runGetPeers(ctx context.Context) {
	_ = c.getPeers.Run(ctx, func(req nodeHasPeersForHash) {
		if req.source != sourceSample {
			select {
			case <-ctx.Done():
			case c.lookupsInbound.In() <- lookupRequest{nodeHasPeersForHash: req}:
			}

			return
		}

		pfh, nodes, pfhErr := c.requestPeersForHash(ctx, req)
		if pfhErr != nil {
			if errors.Is(pfhErr, errNoPeers) {
				c.countHash(req.source, stageNoPeers, 1)
			} else {
				c.countHash(req.source, stageGetPeersError, 1)
			}

			if c.unseeded(req.infoHash) {
				c.countHash(req.source, "lookup_skipped", 1)
				c.trackerOutcome(req.infoHash, "lookup_skipped")

				return
			}

			req.source = sourceSampleLookup

			select {
			case c.lookups.In() <- lookupRequest{nodeHasPeersForHash: req, seeds: nodes}:
			default:
				c.countHash(req.source, stageLookupDropped, 1)
				c.trackerOutcome(req.infoHash, stageLookupDropped)
			}

			return
		}

		c.countHash(req.source, stagePeersFound, 1)
		c.countHash(req.source, stagePeersSum, len(pfh.peers))
		c.forwardPeers(ctx, req, pfh.peers)
	})
}

type lookupRequest struct {
	nodeHasPeersForHash
	seeds []client.NodeInfo
}

func (c *crawler) runLookups(ctx context.Context, ch concurrency.BufferedConcurrentChannel[lookupRequest]) {
	_ = ch.Run(ctx, func(req lookupRequest) {
		found := c.lookupPeers(ctx, req.source, req.infoHash, req.seeds)
		if len(found) == 0 {
			c.countHash(req.source, stageLookupEmpty, 1)
		} else {
			c.countHash(req.source, stageLookupFound, 1)
			c.countHash(req.source, stagePeersSum, len(found))
		}

		var peers []netip.AddrPort
		if req.source == dht.QAnnouncePeer {
			peers = append(append(peers, req.peers...), found...)
		} else {
			peers = append(append(peers, found...), req.peers...)
		}

		if len(peers) == 0 {
			c.trackerOutcome(req.infoHash, stageLookupEmpty)

			return
		}

		c.forwardPeers(ctx, req.nodeHasPeersForHash, peers)
	})
}

func (c *crawler) forwardPeers(ctx context.Context, req nodeHasPeersForHash, peers []netip.AddrPort) {
	hashPeers := make([]ktable.HashPeer, 0, len(peers))
	for _, p := range peers {
		hashPeers = append(hashPeers, ktable.HashPeer{
			Addr: p,
		})
	}

	c.kTable.BatchCommand(
		ktable.PutHash{ID: req.infoHash, Peers: hashPeers},
	)

	select {
	case <-ctx.Done():
	case c.requestMetaInfo.In() <- infoHashWithPeers{
		nodeHasPeersForHash: req,
		peers:               peers,
	}:
	}
}

func (c *crawler) requestPeersForHash(
	ctx context.Context,
	req nodeHasPeersForHash,
) (infoHashWithPeers, []client.NodeInfo, error) {
	res, err := c.client.GetPeers(ctx, req.node, req.infoHash)
	if err != nil {
		c.kTable.BatchCommand(ktable.DropAddr{
			Addr:   req.node.Addr(),
			Reason: fmt.Errorf("failed to get peers: %w", err),
		})

		return infoHashWithPeers{}, nil, err
	}

	c.kTable.BatchCommand(ktable.PutNode{
		ID:      res.ID,
		Addr:    req.node,
		Options: []ktable.NodeOption{ktable.NodeResponded()},
	})

	if len(res.Nodes) > 0 {
		// block the channel for up to a second in an attempt to add the nodes to the discoveredNodes channel
		cancelCtx, cancel := context.WithTimeout(ctx, time.Second)

		for _, n := range res.Nodes {
			select {
			case <-cancelCtx.Done():
				break
			case c.discoveredNodes.In() <- ktable.NewNode(n.ID, n.Addr):
				continue
			}
		}

		cancel()
	}

	if len(res.Values) < 1 {
		return infoHashWithPeers{}, res.Nodes, errNoPeers
	}

	return infoHashWithPeers{
		nodeHasPeersForHash: req,
		peers:               res.Values,
	}, nil, nil
}
