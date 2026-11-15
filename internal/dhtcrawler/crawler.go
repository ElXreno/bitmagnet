package dhtcrawler

import (
	"context"
	"net/netip"
	"sync"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/blocking"
	"github.com/bitmagnet-io/bitmagnet/internal/bloom"
	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/responder"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/banning"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	"github.com/prometheus/client_golang/prometheus"
	boom "github.com/tylertreat/BoomFilters"
	"go.uber.org/zap"
)

type crawler struct {
	v4                           *nodeFamily
	v6                           *nodeFamily
	client                       client.Client
	metainfoRequester            metainforequester.Requester
	banningChecker               banning.Checker
	bootstrapNodes               []string
	reseedBootstrapNodesInterval time.Duration
	getOldestNodesInterval       time.Duration
	oldPeerThreshold             time.Duration
	discoveredNodes              concurrency.BatchingChannel[ktable.Node]
	inboundHashes                <-chan responder.InboundHash
	inboundSeen                  *ignoreFilter
	inboundTriage                concurrency.BatchingChannel[nodeHasPeersForHash]
	hashesTotal                  *prometheus.CounterVec
	dialsTotal                   *prometheus.CounterVec
	stageGauge                   *prometheus.GaugeVec
	probeTotal                   *prometheus.CounterVec
	lookupQueries                *prometheus.HistogramVec
	classTotal                   *prometheus.CounterVec
	deadPeers                    *ignoreFilter
	pingCache                    *pingCache
	trackers                     *trackerScraper
	trackers6                    *trackerScraper
	fetched                      *ignoreFilter
	failed                       *failedHashes
	failedSeeded                 *failedHashes
	infoHashTriage               concurrency.BatchingChannel[nodeHasPeersForHash]
	getPeers                     concurrency.BufferedConcurrentChannel[nodeHasPeersForHash]
	lookups                      concurrency.BufferedConcurrentChannel[lookupRequest]
	lookupsInbound               concurrency.BufferedConcurrentChannel[lookupRequest]
	scrape                       concurrency.BufferedConcurrentChannel[nodeHasPeersForHash]
	requestMetaInfo              concurrency.BufferedConcurrentChannel[infoHashWithPeers]
	persistTorrents              concurrency.BatchingChannel[infoHashWithMetaInfo]
	persistSources               concurrency.BatchingChannel[infoHashWithScrape]
	rescrapeThreshold            time.Duration
	saveFilesThreshold           uint
	savePieces                   bool
	dao                          *dao.Query
	// ignoreHashes is a thread-safe bloom filter that the crawler keeps in memory,
	// containing every hash it has already encountered.
	// This avoids multiple attempts to crawl the same hash, and takes a lot of load off the database query
	// that checks if a hash has already been indexed.
	ignoreHashes    *ignoreFilter
	blockingManager blocking.Manager
	// soughtNodeID is a random node ID used as the target for find_node and sample_infohashes requests.
	// It is rotated every 10 seconds.
	soughtNodeID   *concurrency.AtomicValue[protocol.ID]
	stopped        chan struct{}
	persistedTotal *prometheus.CounterVec
	logger         *zap.SugaredLogger
}

func (c *crawler) start() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// start the various pipeline workers
	go c.rotateSoughtNodeID(ctx)
	go c.runDiscoveredNodes(ctx)

	for _, f := range c.families() {
		go c.runPing(ctx, f)
		go c.runFindNode(ctx, f)
		go c.getNodesForFindNode(ctx, f)
		go c.runSampleInfoHashes(ctx, f)
		go c.getNodesForSampleInfoHashes(ctx, f)
		go c.getOldNodes(ctx, f)
		go c.runNodePool(ctx, f)
		go c.runRevisits(ctx, f)
	}

	if c.v6 != nil {
		go c.runDiscoveredNodes6(ctx)
	}

	for range envInt("BITMAGNET_TRIAGE_WORKERS", 4) {
		go c.runInfoHashTriage(ctx, "triage", c.infoHashTriage)
	}

	go c.runInfoHashTriage(ctx, "triage_inbound", c.inboundTriage)
	go c.runInboundHashes(ctx)
	go c.runGetPeers(ctx)
	go c.runLookups(ctx, c.lookups)
	go c.runLookups(ctx, c.lookupsInbound)
	go c.reportStages(ctx)
	go c.saveFailedHashes(ctx)
	if c.trackers != nil {
		c.trackers.onPeers = c.trackerPeers
	}

	if c.trackers6 != nil {
		c.trackers6.onPeers = c.trackerPeers
	}

	go c.trackers.run(ctx)
	go c.trackers6.run(ctx)
	go c.runRequestMetaInfo(ctx)
	go c.runScrape(ctx)
	go c.reseedBootstrapNodes(ctx)
	go c.runPersistTorrents(ctx)
	go c.runPersistSources(ctx)
	<-c.stopped
}

type nodeHasPeersForHash struct {
	infoHash protocol.ID
	node     netip.AddrPort
	source   string
	peers    []netip.AddrPort
	visit    string
	class    string
}

const (
	sourceSample6       = "sample_infohashes6"
	sourceSampleLookup6 = "sample_infohashes6_lookup"
	sourceSampleLookup  = "sample_infohashes_lookup"
	stageLookupFound    = "lookup_found"
	stageLookupEmpty    = "lookup_empty"
	stageLookupQueries  = "lookup_queries"
	stageScrapeDropped  = "scrape_dropped"
	stageAlreadyFetched = "already_fetched"
	stageLookupDropped  = "lookup_dropped"
	sourceSample        = "sample_infohashes"
	stageDiscovered     = "discovered"
	stageNeedsMetaInfo  = "needs_metainfo"
	stagePersisted      = "persisted"
	stageDuplicate      = "duplicate"
	stageBlocked        = "blocked"
	stageKnown          = "known"
	stageScrape         = "scrape"
	stageGetPeersError  = "get_peers_error"
	stageNoPeers        = "no_peers"
	stagePeersFound     = "peers_found"
	stagePeersSum       = "peers_sum"
	stageMetaInfoOK     = "metainfo_ok"
	stageMetaInfoFail   = "metainfo_failed"
	stageBanned         = "banned"
)

func (c *crawler) countHash(source string, stage string, n int) {
	c.hashesTotal.With(prometheus.Labels{"source": source, "stage": stage}).Add(float64(n))
}

type infoHashWithMetaInfo struct {
	nodeHasPeersForHash
	metaInfo metainfo.Info
}

type infoHashWithPeers struct {
	nodeHasPeersForHash
	peers []netip.AddrPort
}

type infoHashWithScrape struct {
	nodeHasPeersForHash
	bfsd bloom.Filter
	bfpe bloom.Filter
}

type ignoreFilter struct {
	mutex sync.Mutex
	bloom *boom.StableBloomFilter
}

func (i *ignoreFilter) testAndAdd(id protocol.ID) bool {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	return i.bloom.TestAndAdd(id[:])
}

func (i *ignoreFilter) test(id protocol.ID) bool {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	return i.bloom.Test(id[:])
}

func (i *ignoreFilter) add(id protocol.ID) {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	i.bloom.Add(id[:])
}

func (c *crawler) rotateSoughtNodeID(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
			c.soughtNodeID.Set(protocol.RandomNodeID())
		}
	}
}

func prometheusLabels(stage string, kind string) prometheus.Labels {
	return prometheus.Labels{"stage": stage, "kind": kind}
}

func (c *crawler) saveFailedHashes(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Minute):
			for _, memory := range []*failedHashes{c.failed, c.failedSeeded} {
				if err := memory.save(); err != nil {
					c.logger.Errorf("failed to save the failed info hash memory: %s", err.Error())
				}
			}
		}
	}
}

// giveUp records that the meta info of hash could not be fetched, so that it is left alone for a while.
func (c *crawler) giveUp(hash protocol.ID) {
	if c.failedSeeded != nil && (c.trackers.swarm(hash) == "seeded" || c.trackers6.swarm(hash) == "seeded") {
		c.failedSeeded.add(hash)

		return
	}

	c.failed.add(hash)
}
