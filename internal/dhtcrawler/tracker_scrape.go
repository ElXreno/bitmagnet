package dhtcrawler

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/netip"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/tracker"
	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"
)

const (
	trackerCacheSize     = 1_000_000
	trackerCacheTTL      = 10 * time.Minute
	trackerBatchWait     = 250 * time.Millisecond
	trackerScrapeTimeout = 3 * time.Second

	trackerAnnounceQueue    = 2000
	trackerScrapeSettle     = 800 * time.Millisecond
	trackerAnnounceIdleWait = 20 * time.Millisecond
	trackerAnnounceNumWant  = 30
)

var defaultTrackers6 = []string{
	"open.stealth.si:80",
	"tracker.torrent.eu.org:451",
	"open.demonii.com:1337",
	"tracker.qu.ax:6969",
	"tracker-udp.gbitt.info:80",
}

var defaultTrackers = []string{
	"tracker.opentrackr.org:1337",
	"open.stealth.si:80",
	"tracker.torrent.eu.org:451",
	"open.demonii.com:1337",
	"tracker.qu.ax:6969",
	"tracker-udp.gbitt.info:80",
	"retracker01-msk-virt.corbina.net:80",
	"t.overflow.biz:6969",
}

type trackerMetrics struct {
	packets *prometheus.CounterVec
	hashes  *prometheus.CounterVec
	outcome *prometheus.CounterVec
	sent    *prometheus.CounterVec
}

func newTrackerMetrics() trackerMetrics {
	return trackerMetrics{
		packets: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bitmagnet",
			Subsystem: "dht_crawler",
			Name:      "tracker_packets_total",
			Help:      "A counter of UDP tracker scrape requests by tracker and result.",
		}, []string{"tracker", "result"}),
		hashes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bitmagnet",
			Subsystem: "dht_crawler",
			Name:      "tracker_hashes_total",
			Help:      "A counter of scraped info hashes by tracker and swarm state.",
		}, []string{"tracker", "swarm"}),
		outcome: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bitmagnet",
			Subsystem: "dht_crawler",
			Name:      "tracker_outcome_total",
			Help:      "A counter of info hashes by what the trackers said about the swarm and how the meta info fetch ended.",
		}, []string{"swarm", "outcome"}),
		sent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bitmagnet",
			Subsystem: "dht_crawler",
			Name:      "tracker_sent_total",
			Help:      "A counter of UDP packets sent to trackers by kind: connect, scrape, started, stopped.",
		}, []string{"tracker", "kind"}),
	}
}

type trackerInfo struct {
	seeders   uint32
	leechers  uint32
	seededBy  uint32
	best      uint8
	announced bool
}

const announcePriorities = 4

func announcePriority(seeders uint32) int {
	switch {
	case seeders >= 20:
		return 0
	case seeders >= 5:
		return 1
	case seeders >= 2:
		return 2
	default:
		return 3
	}
}

// trackerScraper asks public UDP trackers about the swarms of info hashes in batches (BEP 15 scrape) and
// announces the seeded ones to take their peers.
type trackerScraper struct {
	client           *tracker.Client
	limiters         []*rate.Limiter
	in               chan protocol.ID
	announces        [][announcePriorities]chan protocol.ID
	announceEnabled  bool
	announceLimiters []*rate.Limiter
	announceParams   tracker.AnnounceParams
	onPeers          func(ctx context.Context, hash protocol.ID, source string, peers []netip.AddrPort)
	stops            []chan protocol.ID
	holdParams       tracker.AnnounceParams
	holdMu           sync.Mutex
	held             map[protocol.ID]int
	join             joinFunc
	onHeld           func(hash protocol.ID)
	draining         atomic.Bool
	prefix           string
	hosts            []string
	mu               sync.Mutex
	cache            *lru.LRU[protocol.ID, trackerInfo]
	metrics          trackerMetrics
}

// newTrackerScraper builds the scraper for one address family: network is "udp4" or "udp6".
func newTrackerScraper(metrics trackerMetrics, network string) (*trackerScraper, error) {
	hosts, env, prefix, suffix := defaultTrackers, "BITMAGNET_TRACKERS", "tracker", ""
	if network == "udp6" {
		hosts, env, prefix, suffix = defaultTrackers6, "BITMAGNET_TRACKERS6", "tracker6", "/v6"
	}

	switch configured := os.Getenv(env); configured {
	case "":
	case "off":
		return nil, nil //nolint:nilnil
	default:
		hosts = strings.Split(configured, ",")
	}

	client, err := tracker.NewClient(network, hosts, trackerScrapeTimeout)
	if err != nil {
		return nil, err
	}

	packetsPerSecond := envInt("BITMAGNET_TRACKER_PPS", 20)
	limiters := make([]*rate.Limiter, len(hosts))

	for i := range hosts {
		limiters[i] = rate.NewLimiter(rate.Limit(packetsPerSecond), packetsPerSecond)
	}

	labels := make([]string, len(hosts))
	for i, host := range hosts {
		labels[i] = host + suffix
	}

	client.OnSend = func(index int, kind string) {
		metrics.sent.With(prometheus.Labels{"tracker": labels[index], "kind": kind}).Inc()
	}

	scraper := &trackerScraper{
		prefix:   prefix,
		hosts:    labels,
		client:   client,
		limiters: limiters,
		in:       make(chan protocol.ID, 100*tracker.MaxScrapeHashes),
		cache:    lru.NewLRU[protocol.ID, trackerInfo](trackerCacheSize, nil, trackerCacheTTL),
		metrics:  metrics,
	}

	if os.Getenv("BITMAGNET_TRACKER_ANNOUNCE") != "off" {
		announcesPerSecond := envInt("BITMAGNET_TRACKER_ANNOUNCE_PPS", 30)

		scraper.announceEnabled = true

		scraper.announces = make([][announcePriorities]chan protocol.ID, len(hosts))

		for i := range scraper.announces {
			for priority := range scraper.announces[i] {
				scraper.announces[i][priority] = make(chan protocol.ID, trackerAnnounceQueue)
			}
		}

		scraper.announceLimiters = make([]*rate.Limiter, len(hosts))

		for i := range hosts {
			scraper.announceLimiters[i] = rate.NewLimiter(rate.Limit(announcesPerSecond), announcesPerSecond)
		}

		scraper.announceParams = tracker.AnnounceParams{
			PeerID:  protocol.RandomPeerID(),
			Key:     rand.Uint32(),
			Port:    uint16(envInt("BITMAGNET_TRACKER_ANNOUNCE_PORT", 6881)),
			NumWant: trackerAnnounceNumWant,
		}
	}

	return scraper, nil
}

func (s *trackerScraper) submit(hash protocol.ID) {
	if s == nil {
		return
	}

	select {
	case s.in <- hash:
	default:
		s.metrics.packets.With(prometheus.Labels{"tracker": "all", "result": "queue_full"}).Inc()
	}
}

func (s *trackerScraper) swarm(hash protocol.ID) string {
	if s == nil {
		return "off"
	}

	info, ok := s.cache.Get(hash)

	switch {
	case !ok:
		return "pending"
	case info.seeders > 0:
		return "seeded"
	case info.leechers > 0:
		return "leechers"
	default:
		return "unknown"
	}
}

func (s *trackerScraper) run(ctx context.Context) {
	if s == nil {
		return
	}

	if s.announceEnabled {
		for i := range s.announceLimiters {
			go s.runAnnounces(ctx, i)
		}
	}

	batch := make([]protocol.ID, 0, tracker.MaxScrapeHashes)
	ticker := time.NewTicker(trackerBatchWait)

	defer ticker.Stop()
	defer s.client.Close()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		hashes := batch
		batch = make([]protocol.ID, 0, tracker.MaxScrapeHashes)

		go s.scrapeBatch(ctx, hashes)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case hash := <-s.in:
			batch = append(batch, hash)
			if len(batch) == tracker.MaxScrapeHashes {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// scrapeBatch asks every tracker about the batch, gives the answers a moment to arrive and then queues the
// seeded hashes for an announce on the tracker that saw the most seeders.
func (s *trackerScraper) scrapeBatch(ctx context.Context, hashes []protocol.ID) {
	var wg sync.WaitGroup

	for i, host := range s.hosts {
		if !s.limiters[i].Allow() {
			s.metrics.packets.With(prometheus.Labels{"tracker": host, "result": "rate_limited"}).Inc()

			continue
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			s.scrape(ctx, i, host, hashes)
		}()
	}

	if !s.announceEnabled {
		return
	}

	settled := make(chan struct{})

	go func() {
		wg.Wait()
		close(settled)
	}()

	select {
	case <-ctx.Done():
		return
	case <-settled:
	case <-time.After(trackerScrapeSettle):
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, hash := range hashes {
		info, ok := s.cache.Get(hash)
		if !ok || info.seeders == 0 || info.announced {
			continue
		}

		priority := announcePriority(info.seeders)
		swarm := s.prefix + trackerSource(info.seeders)
		queued := false

		for offset := range len(s.announces) {
			index := (int(info.best) + offset) % len(s.announces)
			if offset > 0 && info.seededBy&(1<<index) == 0 {
				continue
			}

			select {
			case s.announces[index][priority] <- hash:
				queued = true
			default:
			}

			if queued {
				break
			}
		}

		if queued {
			info.announced = true
			s.cache.Add(hash, info)
			s.metrics.hashes.With(prometheus.Labels{"tracker": "queued", "swarm": swarm}).Inc()
		} else {
			s.metrics.hashes.With(prometheus.Labels{"tracker": "queue_full", "swarm": swarm}).Inc()
		}
	}
}

func (s *trackerScraper) scrape(ctx context.Context, index int, host string, hashes []protocol.ID) {
	results, err := s.client.Scrape(ctx, index, hashes)

	switch {
	case err == nil:
		s.metrics.packets.With(prometheus.Labels{"tracker": host, "result": "ok"}).Inc()
	case errors.Is(err, tracker.ErrTimeout):
		s.metrics.packets.With(prometheus.Labels{"tracker": host, "result": "timeout"}).Inc()

		return
	default:
		s.metrics.packets.With(prometheus.Labels{"tracker": host, "result": "error"}).Inc()

		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for i, r := range results {
		swarm := "unknown"

		switch {
		case r.Seeders > 0:
			swarm = "seeded"
		case r.Leechers > 0:
			swarm = "leechers"
		}

		s.metrics.hashes.With(prometheus.Labels{"tracker": host, "swarm": swarm}).Inc()

		info, _ := s.cache.Get(hashes[i])

		if r.Seeders > info.seeders {
			info.best = uint8(index)
		}

		info.seeders = max(info.seeders, r.Seeders)
		info.leechers = max(info.leechers, r.Leechers)

		if r.Seeders > 0 {
			info.seededBy |= 1 << index
		}

		s.cache.Add(hashes[i], info)
	}
}

func (c *crawler) trackerOutcome(hash protocol.ID, outcome string) {
	if c.trackers == nil {
		return
	}

	c.trackers.metrics.outcome.With(prometheus.Labels{"swarm": c.trackers.swarm(hash), "outcome": outcome}).Inc()
}

// trackerPolicy selects where a hash without seeders on any tracker gets less effort:
// "off" nowhere, "lookup" no iterative lookup, anything else also no dials to peers that ignored the ping.
var trackerPolicy = os.Getenv("BITMAGNET_TRACKER_POLICY")

func (c *crawler) unseeded(hash protocol.ID) bool {
	if c.trackers == nil || trackerPolicy == "off" {
		return false
	}

	swarm := c.trackers.swarm(hash)

	return swarm == "leechers" || swarm == "unknown"
}

type announceJob struct {
	hash    protocol.ID
	seeders uint32
	hold    bool
	stop    bool
}

// nextAnnounce pops the queued hash with the most seeders for tracker number index.
func (s *trackerScraper) nextAnnounce(index int) (announceJob, bool) {
	for _, queue := range s.announces[index] {
		select {
		case hash := <-queue:
			info, _ := s.cache.Get(hash)

			return announceJob{hash: hash, seeders: info.seeders}, true
		default:
		}
	}

	return announceJob{}, false
}

func (s *trackerScraper) runAnnounces(ctx context.Context, index int) {
	host := s.hosts[index]

	for {
		if err := s.announceLimiters[index].Wait(ctx); err != nil {
			return
		}

		jobs := s.spendToken(index)
		if len(jobs) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(trackerAnnounceIdleWait):
			}

			continue
		}

		for _, job := range jobs {
			switch {
			case job.stop:
				go s.announceStop(ctx, index, job.hash)
			case job.hold:
				go s.announceMember(ctx, index, host, job)
			default:
				go s.announce(ctx, index, host, job.hash, job.seeders)
			}
		}
	}
}

func (s *trackerScraper) announce(ctx context.Context, index int, host string, hash protocol.ID, seeders uint32) {
	result, err := s.client.Announce(ctx, index, hash, s.announceParams)

	s.announced(ctx, host, hash, seeders, result.Peers, err)
}

func trackerSource(seeders uint32) string {
	switch {
	case seeders >= 20:
		return "_20"
	case seeders >= 5:
		return "_5_19"
	case seeders >= 2:
		return "_2_4"
	default:
		return "_1"
	}
}

func (c *crawler) trackerPeers(ctx context.Context, hash protocol.ID, source string, peers []netip.AddrPort) {
	c.countHash(source, stageDiscovered, 1)
	c.countHash(source, stagePeersSum, len(peers))

	if c.fetched.test(hash) {
		c.countHash(source, stageAlreadyFetched, 1)

		return
	}

	c.forwardPeers(ctx, nodeHasPeersForHash{infoHash: hash, source: source}, peers)
}
