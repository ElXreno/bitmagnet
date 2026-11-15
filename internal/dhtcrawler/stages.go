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
		"get_peers":         c.getPeers,
		"lookups":           c.lookups,
		"lookups_inbound":   c.lookupsInbound,
		"scrape":            c.scrape,
		"request_meta_info": c.requestMetaInfo,
	}

	for _, f := range c.families() {
		stages["ping"+f.suffix] = f.nodesForPing
		stages["find_node"+f.suffix] = f.nodesForFindNode
		stages["sample_infohashes"+f.suffix] = f.nodesForSampleInfoHashes
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

			if c.v6 != nil {
				c.stageGauge.With(prometheus.Labels{"stage": "discovered_nodes6", "kind": "queued"}).
					Set(float64(len(c.v6.discovered)))
			}
		}
	}
}
