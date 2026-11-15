package dhtcrawler

import (
	"context"
	"os"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/blocking"
	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/responder"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/server"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/banning"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	"github.com/bitmagnet-io/bitmagnet/internal/worker"
	"github.com/prometheus/client_golang/prometheus"
	boom "github.com/tylertreat/BoomFilters"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Params struct {
	fx.In
	Config            Config
	KTable            ktable.Table
	KTable6           ktable.Table `name:"dht_ktable_ipv6"`
	Client            lazy.Lazy[client.Client]
	MetainfoRequester metainforequester.Requester
	MetainfoReceiver  metainforequester.Receiver
	DhtServerConfig   server.Config
	BanningChecker    banning.Checker `name:"metainfo_banning_checker"`
	Search            lazy.Lazy[search.Search]
	Dao               lazy.Lazy[*dao.Query]
	BlockingManager   lazy.Lazy[blocking.Manager]
	DiscoveredNodes   concurrency.BatchingChannel[ktable.Node] `name:"dht_discovered_nodes"`
	InboundHashes     chan responder.InboundHash               `name:"dht_inbound_hashes"`
	Logger            *zap.SugaredLogger
}

type Result struct {
	fx.Out
	Worker worker.Worker `group:"workers"`

	DhtCrawlerActive *concurrency.AtomicValue[bool] `name:"dht_crawler_active"`

	PersistedTotal prometheus.Collector `group:"prometheus_collectors"`
	HashesTotal    prometheus.Collector `group:"prometheus_collectors"`
	DialsTotal     prometheus.Collector `group:"prometheus_collectors"`
	StageGauge     prometheus.Collector `group:"prometheus_collectors"`
	ProbeTotal     prometheus.Collector `group:"prometheus_collectors"`
	SwarmTotal     prometheus.Collector `group:"prometheus_collectors"`
	PeerDialTotal  prometheus.Collector `group:"prometheus_collectors"`
	LookupQueries  prometheus.Collector `group:"prometheus_collectors"`
	ClassTotal     prometheus.Collector `group:"prometheus_collectors"`
	TrackerPackets prometheus.Collector `group:"prometheus_collectors"`
	TrackerHashes  prometheus.Collector `group:"prometheus_collectors"`
	TrackerOutcome prometheus.Collector `group:"prometheus_collectors"`
	TrackerSent    prometheus.Collector `group:"prometheus_collectors"`
}

func New(params Params) Result {
	active := &concurrency.AtomicValue[bool]{}

	var c crawler

	persistedTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bitmagnet",
		Subsystem: "dht_crawler",
		Name:      "persisted_total",
		Help:      "A counter of persisted database entities.",
	}, []string{"entity"})
	hashesTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bitmagnet",
		Subsystem: "dht_crawler",
		Name:      "hashes_total",
		Help:      "A counter of info hashes by discovery source and pipeline stage.",
	}, []string{"source", "stage"})
	stageGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bitmagnet",
		Subsystem: "dht_crawler",
		Name:      "stage_load",
		Help:      "Queued items and busy workers of each pipeline stage.",
	}, []string{"stage", "kind"})
	classTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bitmagnet",
		Subsystem: "dht_crawler",
		Name:      "sample_class_total",
		Help:      "Sampling funnel by the kind of node that gave the info hashes.",
	}, []string{"family", "class", "stage"})
	lookupQueries := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "bitmagnet",
		Subsystem: "dht_crawler",
		Name:      "lookup_queries",
		Help:      "Answered get_peers queries of a lookup until the first peers arrived, or all of them when none did.",
		Buckets:   []float64{1, 2, 3, 4, 6, 8, 12, 16, 24},
	}, []string{"source", "result"})
	probeTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bitmagnet",
		Subsystem: "dht_crawler",
		Name:      "peer_probe_total",
		Help:      "Meta info dials by earlier failure of the address, DHT ping result and dial outcome.",
	}, []string{"prev", "ping", "dial"})
	swarmTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bitmagnet",
		Subsystem: "dht_crawler",
		Name:      "swarm_total",
		Help:      "Info hashes that reached the meta info stage, by the DHT versions of their peers and the outcome.",
	}, []string{"class", "outcome"})
	peerDialTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bitmagnet",
		Subsystem: "dht_crawler",
		Name:      "peer_dial_total",
		Help:      "Meta info dials by the DHT version class of the peer and the result.",
	}, []string{"node", "result"})
	dialsTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bitmagnet",
		Subsystem: "dht_crawler",
		Name:      "metainfo_dials_total",
		Help:      "A counter of meta info requests to peers by discovery source and result.",
	}, []string{"source", "result"})

	trackerMetrics := newTrackerMetrics()

	return Result{
		Worker: worker.NewWorker(
			"dht_crawler",
			fx.Hook{
				OnStart: func(context.Context) error {
					active.Set(true)
					scalingFactor := int(params.Config.ScalingFactor)
					cl, err := params.Client.Get()
					if err != nil {
						return err
					}
					query, err := params.Dao.Get()
					if err != nil {
						return err
					}
					blockingManager, err := params.BlockingManager.Get()
					if err != nil {
						return err
					}
					var failed, failedSeeded *failedHashes

					closedSwarms, err := newFailedHashes(
						"closed_swarm_hashes",
						time.Duration(envInt("BITMAGNET_CLOSED_SWARM_TTL_HOURS", 72))*time.Hour, 6*time.Hour, 6_000_000)
					if err != nil {
						return err
					}

					if os.Getenv("BITMAGNET_FAILED_MEMORY") != "off" {
						failed, err = newFailedHashes(
							"failed_hashes", time.Duration(envInt("BITMAGNET_FAILED_TTL_HOURS", 6))*time.Hour, time.Hour, 4_000_000)
						if err != nil {
							return err
						}

						failedSeeded, err = newFailedHashes(
							"failed_hashes_seeded",
							time.Duration(envInt("BITMAGNET_FAILED_SEEDED_TTL_MINUTES", 60))*time.Minute, 10*time.Minute, 500_000)
						if err != nil {
							return err
						}
					}

					trackers, err := newTrackerScraper(trackerMetrics, "udp4")
					if err != nil {
						return err
					}

					trackers6, err := newTrackerScraper(trackerMetrics, "udp6")
					if err != nil {
						params.Logger.Warnf("IPv6 trackers are unavailable: %s", err.Error())

						trackers6 = nil
					}

					sampleConcurrency := envInt("BITMAGNET_SAMPLE_CONCURRENCY", 40*scalingFactor)

					var v6 *nodeFamily
					if cl.IPv6() {
						v6 = newNodeFamily("6", params.KTable6, scalingFactor,
							envInt("BITMAGNET_SAMPLE_CONCURRENCY6", sampleConcurrency))
						v6.discovered = make(chan ktable.Node, 100*scalingFactor)
					}

					c = crawler{
						v4:                           newNodeFamily("", params.KTable, scalingFactor, sampleConcurrency),
						v6:                           v6,
						client:                       cl,
						metainfoRequester:            params.MetainfoRequester,
						banningChecker:               params.BanningChecker,
						bootstrapNodes:               params.Config.BootstrapNodes,
						reseedBootstrapNodesInterval: time.Minute * 10,
						getOldestNodesInterval:       time.Second * 10,
						oldPeerThreshold:             time.Minute * 15,
						discoveredNodes:              params.DiscoveredNodes,
						inboundHashes:                params.InboundHashes,
						inboundTriage: concurrency.NewBatchingChannel[nodeHasPeersForHash](
							10*scalingFactor, 1000, 2*time.Second),
						inboundSeen: &ignoreFilter{
							bloom: boom.NewStableBloomFilter(10_000_000, 2, 0.001),
						},
						hashesTotal:   hashesTotal,
						dialsTotal:    dialsTotal,
						stageGauge:    stageGauge,
						probeTotal:    probeTotal,
						swarmTotal:    swarmTotal,
						peerDialTotal: peerDialTotal,
						lookupQueries: lookupQueries,
						classTotal:    classTotal,
						trackers:      trackers,
						trackers6:     trackers6,
						failed:        failed,
						failedSeeded:  failedSeeded,
						closedSwarms:  closedSwarms,
						fetched: &ignoreFilter{
							bloom: boom.NewStableBloomFilter(10_000_000, 2, 0.001),
						},
						pingCache: newPingCache(
							time.Duration(envInt("BITMAGNET_PING_CACHE_SECONDS", 300)) * time.Second),
						deadPeers: &ignoreFilter{
							bloom: boom.NewStableBloomFilter(10_000_000, 2, 0.001),
						},
						infoHashTriage: concurrency.NewBatchingChannel[nodeHasPeersForHash](
							10*scalingFactor, 1000, 20*time.Second),
						getPeers: concurrency.NewBufferedConcurrentChannel[nodeHasPeersForHash](
							10*scalingFactor, 100*scalingFactor),
						lookups: concurrency.NewBufferedConcurrentChannel[lookupRequest](
							10*scalingFactor, 80*scalingFactor),
						lookupsInbound: concurrency.NewBufferedConcurrentChannel[lookupRequest](
							10*scalingFactor, 10*scalingFactor),
						scrape: concurrency.NewBufferedConcurrentChannel[nodeHasPeersForHash](
							10*scalingFactor, 20*scalingFactor),
						requestMetaInfo: concurrency.NewBufferedConcurrentChannel[infoHashWithPeers](
							10*scalingFactor,
							60*scalingFactor,
						),
						persistTorrents: concurrency.NewBatchingChannel[infoHashWithMetaInfo](
							1000,
							1000,
							5*time.Second,
						),
						persistSources: concurrency.NewBatchingChannel[infoHashWithScrape](
							1000,
							1000,
							time.Minute,
						),
						saveFilesThreshold: params.Config.SaveFilesThreshold,
						savePieces:         params.Config.SavePieces,
						rescrapeThreshold:  params.Config.RescrapeThreshold,
						dao:                query,
						ignoreHashes: &ignoreFilter{
							bloom: boom.NewStableBloomFilter(10_000_000, 2, 0.001),
						},
						blockingManager: blockingManager,
						soughtNodeID:    &concurrency.AtomicValue[protocol.ID]{},
						stopped:         make(chan struct{}),
						persistedTotal:  persistedTotal,
						logger:          params.Logger.Named("dht_crawler"),
					}
					c.soughtNodeID.Set(protocol.RandomNodeID())
					c.setupInboundPeers(params.MetainfoReceiver, params.DhtServerConfig.Port)

					// todo: Fix!
					//nolint:contextcheck
					go c.start()
					return nil
				},
				OnStop: func(context.Context) error {
					active.Set(false)
					if c.stopped != nil {
						close(c.stopped)
					}

					if err := c.failed.save(); err != nil {
						return err
					}

					if err := c.failedSeeded.save(); err != nil {
						return err
					}

					if err := c.closedSwarms.save(); err != nil {
						return err
					}

					return nil
				},
			},
		),
		PersistedTotal:   persistedTotal,
		HashesTotal:      hashesTotal,
		DialsTotal:       dialsTotal,
		StageGauge:       stageGauge,
		ProbeTotal:       probeTotal,
		SwarmTotal:       swarmTotal,
		PeerDialTotal:    peerDialTotal,
		LookupQueries:    lookupQueries,
		ClassTotal:       classTotal,
		TrackerPackets:   trackerMetrics.packets,
		TrackerHashes:    trackerMetrics.hashes,
		TrackerOutcome:   trackerMetrics.outcome,
		TrackerSent:      trackerMetrics.sent,
		DhtCrawlerActive: active,
	}
}
