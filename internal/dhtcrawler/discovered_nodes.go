package dhtcrawler

import (
	"context"
	"net/netip"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/responder"
	"go.uber.org/fx"
)

type DiscoveredNodesParams struct {
	fx.In
	Config Config
}

type DiscoveredNodesResult struct {
	fx.Out
	DiscoveredNodes concurrency.BatchingChannel[ktable.Node] `name:"dht_discovered_nodes"`
	InboundHashes   chan responder.InboundHash               `name:"dht_inbound_hashes"`
}

// NewDiscoveredNodes creates the channel for discovered nodes.
// It receives nodes discovered by the crawler, as well as nodes from incoming requests to the DHT server.
// It is provided as a separate service to avoid a circular dependency with the DHT server.
func NewDiscoveredNodes(params DiscoveredNodesParams) DiscoveredNodesResult {
	return DiscoveredNodesResult{
		DiscoveredNodes: concurrency.NewBatchingChannel[ktable.Node](
			int(100*params.Config.ScalingFactor), 10, time.Second/100),
		InboundHashes: make(chan responder.InboundHash, int(100*params.Config.ScalingFactor)),
	}
}

func (c *crawler) runDiscoveredNodes(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ps := <-c.discoveredNodes.Out():
			addrs := make([]netip.Addr, 0, 1)

			m := make(map[string]ktable.Node, 1)
			for _, p := range ps {
				if _, ok := m[p.Addr().Addr().String()]; !ok {
					m[p.Addr().Addr().String()] = p
					addrs = append(addrs, p.Addr().Addr())
				}
			}

			// For any newly discovered node, we will block until it can be
			// sent to any one of the pipeline channels.
			for _, addr := range addrs {
				p := m[addr.String()]
				f := c.family(addr)

				if f.discovered != nil {
					select {
					case f.discovered <- p:
					default:
					}

					continue
				}

				if !f.dispatchDiscoveredNode(ctx, p) {
					return
				}
			}
		}
	}
}

func (f *nodeFamily) dispatchDiscoveredNode(ctx context.Context, p ktable.Node) bool {
	if f.ignoreNodes.test(p.ID()) {
		return true
	}

	select {
	case <-ctx.Done():
		return false
	case f.nodesForFindNode.In() <- p:
	case f.nodesForSampleInfoHashes.In() <- p:
		f.ignoreNodes.add(p.ID())
	case f.nodesForPing.In() <- p:
	}

	return true
}

func (c *crawler) runDiscoveredNodes6(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-c.v6.discovered:
			if !c.v6.dispatchDiscoveredNode(ctx, p) {
				return
			}
		}
	}
}
