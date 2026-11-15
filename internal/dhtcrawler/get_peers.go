package dhtcrawler

import (
	"context"
	"errors"
	"fmt"
	"net/netip"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

var errNoPeers = errors.New("no peers found")

func (c *crawler) runGetPeers(ctx context.Context) {
	_ = c.getPeers.Run(ctx, func(req nodeHasPeersForHash) {
		if !isSampleSource(req.source) {
			select {
			case c.lookupsInbound.In() <- lookupRequest{nodeHasPeersForHash: req}:
			default:
				c.countHash(req.source, stageLookupDropped, 1)
			}

			return
		}

		pfh, nodes, pfhErr := c.requestPeersForHash(ctx, req)
		if pfhErr != nil {
			if errors.Is(pfhErr, errNoPeers) {
				c.countHash(req.source, stageNoPeers, 1)
				c.countClass(req, stageNoPeers, 1)
			} else {
				c.countHash(req.source, stageGetPeersError, 1)
				c.countClass(req, stageGetPeersError, 1)
			}

			if c.unseeded(req.infoHash) {
				c.countHash(req.source, "lookup_skipped", 1)
				c.trackerOutcome(req.infoHash, "lookup_skipped")
				c.giveUp(req.infoHash)

				return
			}

			req.source = sampleLookupSource(req.source)

			select {
			case c.lookups.In() <- lookupRequest{nodeHasPeersForHash: req, seeds: nodes}:
			default:
				c.countHash(req.source, stageLookupDropped, 1)
				c.trackerOutcome(req.infoHash, stageLookupDropped)
			}

			return
		}

		c.countHash(req.source, stagePeersFound, 1)
		c.countClass(req, stagePeersFound, 1)
		c.countHash(req.source, stagePeersSum, len(pfh.peers))
		c.forwardPeers(ctx, pfh.nodeHasPeersForHash, pfh.peers)
	})
}

type lookupRequest struct {
	nodeHasPeersForHash
	seeds []client.NodeInfo
}

func (c *crawler) runLookups(ctx context.Context, ch concurrency.BufferedConcurrentChannel[lookupRequest]) {
	_ = ch.Run(ctx, func(req lookupRequest) {
		var tokens *[]lookupToken
		if c.inbound.keepsTokens() {
			tokens = &req.tokens
		}

		found := c.lookupPeersWithTokens(ctx, req.source, req.infoHash, req.seeds, tokens)
		if len(found) == 0 {
			c.countHash(req.source, stageLookupEmpty, 1)
		} else {
			c.countHash(req.source, stageLookupFound, 1)
			c.countHash(req.source, stagePeersSum, len(found))
		}

		var peers []netip.AddrPort
		if isAnnounceSource(req.source) {
			peers = append(append(peers, req.peers...), found...)
		} else {
			peers = append(append(peers, found...), req.peers...)
		}

		if len(peers) == 0 {
			c.trackerOutcome(req.infoHash, stageLookupEmpty)
			c.giveUp(req.infoHash)

			return
		}

		c.forwardPeers(ctx, req.nodeHasPeersForHash, peers)
	})
}

func (c *crawler) forwardPeers(ctx context.Context, req nodeHasPeersForHash, peers []netip.AddrPort) {
	hashPeers := make(map[*nodeFamily][]ktable.HashPeer, 2)

	for _, p := range peers {
		f := c.family(p.Addr())
		hashPeers[f] = append(hashPeers[f], ktable.HashPeer{
			Addr: p,
		})
	}

	for f, familyPeers := range hashPeers {
		f.kTable.BatchCommand(
			ktable.PutHash{ID: req.infoHash, Peers: familyPeers},
		)
	}

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
	kTable := c.family(req.node.Addr()).kTable

	res, err := c.client.GetPeers(ctx, req.node, req.infoHash)
	if err != nil {
		kTable.BatchCommand(ktable.DropAddr{
			Addr:   req.node.Addr(),
			Reason: fmt.Errorf("failed to get peers: %w", err),
		})

		return infoHashWithPeers{}, nil, err
	}

	kTable.BatchCommand(ktable.PutNode{
		ID:      res.ID,
		Addr:    req.node,
		Options: []ktable.NodeOption{ktable.NodeResponded()},
	})

	if len(res.Nodes) > 0 {
		// block the channel for up to a second in an attempt to add the nodes to the discoveredNodes channel
		for _, n := range res.Nodes {
			select {
			case c.discoveredNodes.In() <- ktable.NewNode(n.ID, n.Addr):
			default:
			}
		}
	}

	if len(res.Values) < 1 {
		return infoHashWithPeers{}, res.Nodes, errNoPeers
	}

	if res.Token != "" && c.inbound.keepsTokens() {
		req.tokens = []lookupToken{{addr: req.node, distance: xorDistance(res.ID, req.infoHash), token: res.Token}}
	}

	return infoHashWithPeers{
		nodeHasPeersForHash: req,
		peers:               res.Values,
	}, nil, nil
}
