package dhtcrawler

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/netip"
	"os"
	"strings"
	"sync"
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
	trackerAnnounceSkips    = 8
	trackerAnnounceIdleWait = 20 * time.Millisecond
	trackerAnnounceNumWant  = 30
)

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
	}
}

type trackerInfo struct {
	seeders   uint32
	leechers  uint32
	seededBy  uint32
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

// trackerScraper asks public UDP trackers about the swarms of info hashes in batches (BEP 15 scrape).
// It only reads swarm sizes: it never announces, so the crawler does not show up in any swarm.
type trackerScraper struct {
	client           *tracker.Client
	limiters         []*rate.Limiter
	in               chan protocol.ID
	announces        [announcePriorities]chan protocol.ID
	announceEnabled  bool
	announceLimiters []*rate.Limiter
	announceParams   tracker.AnnounceParams
	onPeers          func(ctx context.Context, hash protocol.ID, seeders uint32, peers []netip.AddrPort)
	mu               sync.Mutex
	cache            *lru.LRU[protocol.ID, trackerInfo]
	metrics          trackerMetrics
}

func newTrackerScraper(metrics trackerMetrics) (*trackerScraper, error) {
	hosts := defaultTrackers

	switch configured := os.Getenv("BITMAGNET_TRACKERS"); configured {
	case "":
	case "off":
		return nil, nil //nolint:nilnil
	default:
		hosts = strings.Split(configured, ",")
	}

	client, err := tracker.NewClient(hosts, trackerScrapeTimeout)
	if err != nil {
		return nil, err
	}

	packetsPerSecond := envInt("BITMAGNET_TRACKER_PPS", 20)
	limiters := make([]*rate.Limiter, len(hosts))

	for i := range hosts {
		limiters[i] = rate.NewLimiter(rate.Limit(packetsPerSecond), packetsPerSecond)
	}

	scraper := &trackerScraper{
		client:   client,
		limiters: limiters,
		in:       make(chan protocol.ID, 100*tracker.MaxScrapeHashes),
		cache:    lru.NewLRU[protocol.ID, trackerInfo](trackerCacheSize, nil, trackerCacheTTL),
		metrics:  metrics,
	}

	if os.Getenv("BITMAGNET_TRACKER_ANNOUNCE") != "off" {
		announcesPerSecond := envInt("BITMAGNET_TRACKER_ANNOUNCE_PPS", 30)

		scraper.announceEnabled = true

		for i := range scraper.announces {
			scraper.announces[i] = make(chan protocol.ID, trackerAnnounceQueue)
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

		for i, host := range s.client.Trackers() {
			if !s.limiters[i].Allow() {
				s.metrics.packets.With(prometheus.Labels{"tracker": host, "result": "rate_limited"}).Inc()

				continue
			}

			go s.scrape(ctx, i, host, hashes)
		}
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
		info.seeders = max(info.seeders, r.Seeders)
		info.leechers = max(info.leechers, r.Leechers)

		if r.Seeders > 0 {
			info.seededBy |= 1 << index
		}

		if s.announceEnabled && r.Seeders > 0 && !info.announced {
			priority := announcePriority(r.Seeders)

			select {
			case s.announces[priority] <- hashes[i]:
				info.announced = true

				s.metrics.hashes.With(prometheus.Labels{"tracker": "queued", "swarm": trackerSource(r.Seeders)}).Inc()
			default:
				s.metrics.hashes.With(prometheus.Labels{"tracker": "queue_full", "swarm": trackerSource(r.Seeders)}).Inc()
			}
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

// nextAnnounce pops the best queued hash that tracker number index reported seeders for. Hashes it has to pass
// over go back to their queue.
func (s *trackerScraper) nextAnnounce(index int) (protocol.ID, trackerInfo, bool) {
	for priority, queue := range s.announces {
		for range trackerAnnounceSkips {
			var hash protocol.ID

			select {
			case hash = <-queue:
			default:
			}

			if hash.IsZero() {
				break
			}

			info, _ := s.cache.Get(hash)
			if info.seededBy&(1<<index) != 0 {
				return hash, info, true
			}

			if info.seededBy == 0 {
				continue
			}

			select {
			case s.announces[priority] <- hash:
			default:
			}
		}
	}

	return protocol.ID{}, trackerInfo{}, false
}

func (s *trackerScraper) runAnnounces(ctx context.Context, index int) {
	host := s.client.Trackers()[index]

	for {
		if err := s.announceLimiters[index].Wait(ctx); err != nil {
			return
		}

		hash, info, ok := s.nextAnnounce(index)
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-time.After(trackerAnnounceIdleWait):
			}

			continue
		}

		go s.announce(ctx, index, host, hash, info.seeders)
	}
}

func (s *trackerScraper) announce(ctx context.Context, index int, host string, hash protocol.ID, seeders uint32) {
	result, err := s.client.Announce(ctx, index, hash, s.announceParams)

	switch {
	case err == nil && len(result.Peers) > 0:
		s.metrics.packets.With(prometheus.Labels{"tracker": host, "result": "announce_ok"}).Inc()

		if s.onPeers != nil {
			s.onPeers(ctx, hash, seeders, result.Peers)
		}
	case err == nil:
		s.metrics.packets.With(prometheus.Labels{"tracker": host, "result": "announce_no_peers"}).Inc()
	case errors.Is(err, tracker.ErrTimeout):
		s.metrics.packets.With(prometheus.Labels{"tracker": host, "result": "announce_timeout"}).Inc()
	default:
		s.metrics.packets.With(prometheus.Labels{"tracker": host, "result": "announce_error"}).Inc()
	}
}

func trackerSource(seeders uint32) string {
	switch {
	case seeders >= 20:
		return "tracker_20"
	case seeders >= 5:
		return "tracker_5_19"
	case seeders >= 2:
		return "tracker_2_4"
	default:
		return "tracker_1"
	}
}

func (c *crawler) trackerPeers(ctx context.Context, hash protocol.ID, seeders uint32, peers []netip.AddrPort) {
	source := trackerSource(seeders)

	c.countHash(source, stageDiscovered, 1)
	c.countHash(source, stagePeersSum, len(peers))

	if c.fetched.test(hash) {
		c.countHash(source, stageAlreadyFetched, 1)

		return
	}

	c.forwardPeers(ctx, nodeHasPeersForHash{infoHash: hash, source: source}, peers)
}
