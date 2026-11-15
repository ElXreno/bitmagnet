package ktable

import (
	"net/netip"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable/btree"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"
)

type Params struct {
	fx.In
	NodeID ID `name:"dht_node_id"`
}

type Result struct {
	fx.Out
	Table                 Table
	Table6                Table                `name:"dht_ktable_ipv6"`
	NodesCountGauge       prometheus.Collector `group:"prometheus_collectors"`
	NodesAddedCounter     prometheus.Collector `group:"prometheus_collectors"`
	NodesDroppedCounter   prometheus.Collector `group:"prometheus_collectors"`
	HashesCountGauge      prometheus.Collector `group:"prometheus_collectors"`
	HashesAddedCounter    prometheus.Collector `group:"prometheus_collectors"`
	HashesDroppedCounter  prometheus.Collector `group:"prometheus_collectors"`
	Nodes6CountGauge      prometheus.Collector `group:"prometheus_collectors"`
	Nodes6AddedCounter    prometheus.Collector `group:"prometheus_collectors"`
	Nodes6DroppedCounter  prometheus.Collector `group:"prometheus_collectors"`
	Hashes6CountGauge     prometheus.Collector `group:"prometheus_collectors"`
	Hashes6AddedCounter   prometheus.Collector `group:"prometheus_collectors"`
	Hashes6DroppedCounter prometheus.Collector `group:"prometheus_collectors"`
}

const (
	nodesK  = 320
	hashesK = 80
)

func New(p Params) Result {
	table4, nodesCollector, hashesCollector := newTable(p.NodeID, false, "")
	table6, nodes6Collector, hashes6Collector := newTable(p.NodeID, true, "6")

	return Result{
		Table:                 table4,
		Table6:                table6,
		NodesCountGauge:       nodesCollector.CountGauge,
		NodesAddedCounter:     nodesCollector.AddedCounter,
		NodesDroppedCounter:   nodesCollector.DroppedCounter,
		HashesCountGauge:      hashesCollector.CountGauge,
		HashesAddedCounter:    hashesCollector.AddedCounter,
		HashesDroppedCounter:  hashesCollector.DroppedCounter,
		Nodes6CountGauge:      nodes6Collector.CountGauge,
		Nodes6AddedCounter:    nodes6Collector.AddedCounter,
		Nodes6DroppedCounter:  nodes6Collector.DroppedCounter,
		Hashes6CountGauge:     hashes6Collector.CountGauge,
		Hashes6AddedCounter:   hashes6Collector.AddedCounter,
		Hashes6DroppedCounter: hashes6Collector.DroppedCounter,
	}
}

func newTable(
	nodeID ID,
	ipv6 bool,
	suffix string,
) (t *table, nodesCollector btree.PrometheusCollector, hashesCollector btree.PrometheusCollector) {
	rm := &reverseMap{addrs: make(map[string]*infoForAddr)}
	nodes := nodeKeyspace{
		keyspace: newKeyspace[netip.AddrPort, NodeOption, Node, *node](
			nodeID,
			nodesK,
			func(id ID, addr netip.AddrPort) *node {
				return &node{
					nodeBase: nodeBase{
						id:   id,
						addr: addr,
					},
					discoveredAt: time.Now(),
					reverseMap:   rm,
				}
			},
		),
	}
	nodesCollector = patchPrometheusCollector("nodes"+suffix, &nodes.keyspace)
	hashes := hashKeyspace{
		keyspace: newKeyspace[[]HashPeer, HashOption, Hash, *hash](
			nodeID,
			hashesK,
			func(id ID, peers []HashPeer) *hash {
				peersMap := make(map[string]HashPeer, len(peers))
				for _, p := range peers {
					peersMap[p.Addr.Addr().String()] = p
					rm.putAddrHashes(p.Addr.Addr(), id)
				}
				return &hash{
					id:           id,
					peers:        peersMap,
					discoveredAt: time.Now(),
					reverseMap:   rm,
				}
			},
		),
	}
	hashesCollector = patchPrometheusCollector("hashes"+suffix, &hashes.keyspace)

	return &table{
		origin:  nodeID,
		ipv6:    ipv6,
		nodesK:  nodesK,
		hashesK: hashesK,
		nodes:   nodes,
		hashes:  hashes,
		addrs:   rm,
	}, nodesCollector, hashesCollector
}

const (
	namespace = "bitmagnet"
	subsystem = "dht_ktable"
)

func patchPrometheusCollector[
	Input any,
	Option any,
	ItemPublic keyspaceItem,
	ItemPrivate keyspaceItemPrivate[Input, Option, ItemPublic],
](itemName string, ks *keyspace[Input, Option, ItemPublic, ItemPrivate]) btree.PrometheusCollector {
	collector := btree.PrometheusCollector{
		Btree: ks.btree,
		CountGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      itemName + "_count",
			Help:      "Number of " + itemName + " in routing table.",
		}),
		AddedCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      itemName + "_added",
			Help:      "Total number of " + itemName + " added to routing table.",
		}),
		DroppedCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      itemName + "_dropped",
			Help:      "Total number of " + itemName + " dropped from routing table.",
		}),
	}
	ks.btree = collector

	return collector
}
