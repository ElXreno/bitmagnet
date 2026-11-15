package dhtcrawler

import (
	"context"
	"fmt"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

func (c *crawler) getNodesForSampleInfoHashes(ctx context.Context, f *nodeFamily) {
	for {
		peers := f.kTable.GetNodesForSampleInfoHashes(240)
		for _, p := range peers {
			select {
			case <-ctx.Done():
				return
			case f.nodesForSampleInfoHashes.In() <- p:
				continue
			}
		}

		<-time.After(time.Second)
	}
}

func (c *crawler) runSampleInfoHashes(ctx context.Context, f *nodeFamily) {
	source := f.sourceSample()

	_ = f.nodesForSampleInfoHashes.Run(ctx, func(n ktable.Node) {
		if !n.IsSampleInfoHashesCandidate() {
			return
		}

		res, err := c.client.SampleInfoHashes(ctx, n.Addr(), c.soughtNodeID.Get())
		if err != nil {
			f.kTable.BatchCommand(
				ktable.DropNode{ID: n.ID(), Reason: fmt.Errorf("sample_infohashes failed: %w", err)},
			)

			return
		}

		var discoveredHashes []nodeHasPeersForHash

		for _, s := range res.Samples {
			if !c.ignoreHashes.testAndAdd(s) {
				discoveredHashes = append(discoveredHashes, nodeHasPeersForHash{
					infoHash: s,
					node:     n.Addr(),
					source:   source,
				})
			}
		}

		c.countSampledNode(f, n.ID(), res.Num, res.Interval, len(res.Samples), len(discoveredHashes))
		c.countHash(source, stageDiscovered, len(discoveredHashes))
		c.countHash(source, stageDuplicate, len(res.Samples)-len(discoveredHashes))

		for _, h := range discoveredHashes {
			select {
			case <-ctx.Done():
				return
			case c.infoHashTriage.In() <- h:
				continue
			}
		}

		interval := res.Interval
		// most nodes request a 6 hour backoff time(!)
		// if we're still discovering info hashes from them then let's set a respectful interval instead
		if len(discoveredHashes) > 0 && interval > 300 {
			interval = 60
		}

		f.kTable.BatchCommand(ktable.PutNode{ID: n.ID(), Addr: n.Addr(), Options: []ktable.NodeOption{
			ktable.NodeResponded(),
			ktable.NodeBep51Support(true),
			ktable.NodeSampleInfoHashesRes(
				len(discoveredHashes),
				res.Num,
				time.Now().Add(time.Duration(interval)*time.Second),
			),
		}})

		if len(res.Nodes) > 0 {
			// block on the channel for up to a second trying to add sampled nodes to the discoveredNodes
			// channel
			go func() {
				timeoutCtx, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()

				for _, n := range res.Nodes {
					select {
					case <-timeoutCtx.Done():
						return
					case c.discoveredNodes.In() <- ktable.NewNode(n.ID, n.Addr):
						continue
					}
				}
			}()
		}
	})
}

// countSampledNode records how wide and how deep sampling reaches: whether the node was seen before in this
// run, how many info hashes it says it stores and how many new ones the visit produced.
func (c *crawler) countSampledNode(
	f *nodeFamily, id protocol.ID, stored int, interval int, samples int, fresh int,
) {
	visit := "first"
	if f.sampledNodes.testAndAdd(id) {
		visit = "repeat"
	}

	source := "sample_nodes" + f.suffix

	c.countHash(source, visit, 1)
	c.countHash(source, "new_from_"+visit, fresh)
	c.countHash(source, "samples_from_"+visit, samples)
	c.countHash(source, "stored_sum", stored)

	switch {
	case stored <= 20:
		c.countHash(source, "stored_le_20", 1)
	case stored <= 100:
		c.countHash(source, "stored_21_100", 1)
	case stored <= 1000:
		c.countHash(source, "stored_101_1000", 1)
	default:
		c.countHash(source, "stored_gt_1000", 1)
	}

	switch {
	case interval <= 60:
		c.countHash(source, "interval_le_60s", 1)
	case interval <= 3600:
		c.countHash(source, "interval_le_1h", 1)
	default:
		c.countHash(source, "interval_gt_1h", 1)
	}
}
