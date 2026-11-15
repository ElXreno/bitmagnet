package dhtcrawler

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func (c *crawler) reportStages(ctx context.Context) {
	type stage interface {
		Len() int
		Active() int
	}

	stages := map[string]stage{
		"ping":              c.nodesForPing,
		"find_node":         c.nodesForFindNode,
		"sample_infohashes": c.nodesForSampleInfoHashes,
		"get_peers":         c.getPeers,
		"lookups":           c.lookups,
		"lookups_inbound":   c.lookupsInbound,
		"scrape":            c.scrape,
		"request_meta_info": c.requestMetaInfo,
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
			for name, s := range stages {
				c.stageGauge.With(prometheus.Labels{"stage": name, "kind": "queued"}).Set(float64(s.Len()))
				c.stageGauge.With(prometheus.Labels{"stage": name, "kind": "active"}).Set(float64(s.Active()))
			}

			c.stageGauge.With(prometheus.Labels{"stage": "inbound_hashes", "kind": "queued"}).
				Set(float64(len(c.inboundHashes)))
		}
	}
}
