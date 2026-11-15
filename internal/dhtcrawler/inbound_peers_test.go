package dhtcrawler

import (
	"bytes"
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anacrolix/torrent/bencode"
	mi "github.com/anacrolix/torrent/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/blocking"
	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester/peertest"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/tracker"
	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/prometheus/client_golang/prometheus"
	boom "github.com/tylertreat/BoomFilters"
	"go.uber.org/zap"
)

func inboundMetadata(t *testing.T, name string) ([]byte, protocol.ID) {
	t.Helper()

	encoded, err := bencode.Marshal(metainfo.Info{
		Name:        name,
		PieceLength: 16384,
		Length:      16384 * 1000,
		Pieces:      bytes.Repeat([]byte{0xcd}, 20*1000),
	})
	if err != nil {
		t.Fatal(err)
	}

	return encoded, protocol.ID(mi.HashBytes(encoded))
}

func offlineScraper(hosts int) *trackerScraper {
	s := &trackerScraper{
		prefix:          "tracker",
		announceEnabled: true,
		announceParams:  tracker.AnnounceParams{Port: 6881, NumWant: trackerAnnounceNumWant},
		announces:       make([][announcePriorities]chan protocol.ID, hosts),
		cache:           lru.NewLRU[protocol.ID, trackerInfo](1000, nil, time.Minute),
		metrics:         newTrackerMetrics(),
	}

	for i := range hosts {
		s.hosts = append(s.hosts, fmt.Sprintf("tracker%d", i))

		for priority := range s.announces[i] {
			s.announces[i][priority] = make(chan protocol.ID, 16)
		}
	}

	return s
}

type blockRecorder struct {
	blocking.Manager
	mu      sync.Mutex
	blocked []protocol.ID
}

func (b *blockRecorder) Block(_ context.Context, hashes []protocol.ID, _ bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.blocked = append(b.blocked, hashes...)

	return nil
}

type banAll struct{}

func (banAll) Check(metainfo.Info) error { return errors.New("banned") }

type panicking struct{}

func (panicking) Check(metainfo.Info) error { panic("checker blew up") }

type inboundTest struct {
	*crawler
	scraper *trackerScraper
	clock   *atomic.Int64
	blocks  *blockRecorder
}

func newInboundTest(t *testing.T, config inboundConfig, scraper *trackerScraper) inboundTest {
	t.Helper()

	return startInboundTest(t, newTestCrawler(nil, nil), config, scraper)
}

func startInboundTest(t *testing.T, c *crawler, config inboundConfig, scraper *trackerScraper) inboundTest {
	t.Helper()

	blocks := &blockRecorder{}
	c.logger = zap.NewNop().Sugar()
	c.fetched = &ignoreFilter{bloom: boom.NewStableBloomFilter(1000, 2, 0.001)}
	c.persistTorrents = concurrency.NewBatchingChannel[infoHashWithMetaInfo](10, 1, 10*time.Millisecond)
	c.trackers = scraper
	c.blockingManager = blocks

	c.openInboundPeers(config, metainforequester.NewReceiver(protocol.RandomPeerID(), 2*time.Second), 0)

	if c.inbound == nil {
		t.Fatal("the incoming peer listener did not start")
	}

	clock := &atomic.Int64{}
	clock.Store(time.Now().UnixNano())

	c.inbound.now = func() time.Time { return time.Unix(0, clock.Load()) }

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go c.inbound.run(ctx)

	return inboundTest{crawler: c, scraper: scraper, clock: clock, blocks: blocks}
}

func (i inboundTest) join(hash protocol.ID, seeders uint32) joinVerdict {
	return i.inbound.join(i.scraper, 0, hash, seeders)
}

func sampleRequest(hash protocol.ID) nodeHasPeersForHash {
	return nodeHasPeersForHash{
		infoHash: hash,
		node:     netip.MustParseAddrPort("192.0.2.9:6881"),
		source:   sourceSample,
		class:    "fresh",
	}
}

func (i inboundTest) stops() []protocol.ID {
	var stops []protocol.ID

	for {
		select {
		case hash := <-i.scraper.stops[0]:
			stops = append(stops, hash)
		default:
			return stops
		}
	}
}

func counterValue(t *testing.T, collector prometheus.Collector, want map[string]string) float64 {
	t.Helper()

	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}

	total := 0.0

	for _, family := range families {
	metrics:
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}

			for name, value := range want {
				if labels[name] != value {
					continue metrics
				}
			}

			total += metric.GetCounter().GetValue()
		}
	}

	return total
}

func (i inboundTest) address() string {
	return fmt.Sprintf("127.0.0.1:%d", listenerPort(i.inbound.listeners[0]))
}

func (i inboundTest) registered() map[protocol.ID]bool {
	hashes := make(map[protocol.ID]bool)
	for _, hash := range i.inbound.hashes() {
		hashes[hash] = true
	}

	return hashes
}

func (i inboundTest) persisted(t *testing.T) infoHashWithMetaInfo {
	t.Helper()

	select {
	case batch := <-i.persistTorrents.Out():
		if len(batch) != 1 {
			t.Fatalf("want one torrent handed to persistence, got %d", len(batch))
		}

		return batch[0]
	case <-time.After(5 * time.Second):
		t.Fatal("nothing reached persistence")

		return infoHashWithMetaInfo{}
	}
}

func (i inboundTest) nothingPersisted(t *testing.T) {
	t.Helper()

	select {
	case batch := <-i.persistTorrents.Out():
		t.Fatalf("%d torrents reached persistence, want none", len(batch))
	case <-time.After(100 * time.Millisecond):
	}
}

func eventually(t *testing.T, what string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func dialCount(t *testing.T, c *crawler, result string) float64 {
	t.Helper()

	registry := prometheus.NewRegistry()
	registry.MustRegister(c.dialsTotal)

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}

	for _, family := range families {
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}

			if labels["source"] == sourceInboundPeer && labels["result"] == result {
				return metric.GetCounter().GetValue()
			}
		}
	}

	return 0
}

func TestInboundRegistryCapacityAndReplacement(t *testing.T) {
	t.Parallel()

	i := newInboundTest(t, inboundConfig{maxHashes: 3, hold: time.Minute, maxConns: 4}, offlineScraper(1))
	older, newer, nine, six := protocol.ID{1}, protocol.ID{2}, protocol.ID{3}, protocol.ID{4}

	i.fetched.add(protocol.ID{7})

	if i.join(protocol.ID{7}, 50) != joinNone || len(i.registered()) != 0 {
		t.Fatal("a hash fetched already must get the usual announce")
	}

	for n, step := range []struct {
		hash    protocol.ID
		seeders uint32
		want    joinVerdict
	}{
		{older, 5, joinMember},
		{newer, 5, joinMember},
		{nine, 9, joinMember},
		{older, 5, joinHeld},
		{protocol.ID{5}, 3, joinNone},
		{protocol.ID{6}, 5, joinNone},
		{six, 6, joinMember},
	} {
		if got := i.join(step.hash, step.seeders); got != step.want {
			t.Fatalf("step %d: want verdict %d, got %d", n, step.want, got)
		}
	}

	if got := i.registered(); len(got) != 3 || !got[newer] || !got[nine] || !got[six] {
		t.Fatalf("want the oldest of the hashes with the fewest seeders replaced, got %v", got)
	}

	if stops := i.stops(); len(stops) != 1 || stops[0] != older {
		t.Fatalf("want one stop, for the replaced hash, got %v", stops)
	}

	other := offlineScraper(1)
	other.enableHolds(protocol.ID{}, 1, 4, i.inbound.join)

	if i.inbound.join(other, 0, nine, 9) != joinMember || !other.holding(nine) || len(i.registered()) != 3 {
		t.Fatal("the scraper of the other family must join a known hash without a new entry")
	}

	for stage, want := range map[string]float64{
		stageInboundRegistered:           4,
		stageInboundRegistered + "_5_19": 4,
		stageInboundSkippedFull:          2,
		stageInboundReplaced:             1,
	} {
		if got := hashCount(t, i.crawler, sourceInboundPeer, stage); got != want {
			t.Errorf("stage %s: want %v, got %v", stage, want, got)
		}
	}
}

func TestInboundRegistryEnds(t *testing.T) {
	t.Parallel()

	i := newInboundTest(t, inboundConfig{maxHashes: 10, hold: time.Minute, maxConns: 4}, offlineScraper(1))
	expiring, busy, closed, fetched := protocol.ID{1}, protocol.ID{2}, protocol.ID{3}, protocol.ID{4}

	i.join(expiring, 5)
	i.join(busy, 5)

	claimed, kind := i.inbound.claim(busy, false)
	if claimed == nil || kind != stageInboundActiveConn {
		t.Fatalf("claim of a held hash: %s", kind)
	}

	deadline := claimed.deadline

	i.inbound.outgoingFailed(sampleRequest(busy))
	i.inbound.outgoingFailed(nodeHasPeersForHash{infoHash: busy, source: "tracker_20"})
	i.inbound.outgoingFailed(sampleRequest(protocol.ID{0xee}))

	if !claimed.deadline.Equal(deadline) || i.inbound.origin(claimed).source != sourceSample {
		t.Fatal("a failed outgoing fetch records where the hash came from and leaves the deadline alone")
	}

	i.clock.Add(int64(59 * time.Second))
	i.inbound.expire()

	if len(i.registered()) != 2 {
		t.Fatal("a hash expired before its deadline")
	}

	i.clock.Add(int64(time.Second))
	i.inbound.expire()

	if len(i.registered()) != 0 {
		t.Fatalf("want both hashes expired, got %v", i.registered())
	}

	if !i.inbound.take(claimed) || i.inbound.take(claimed) {
		t.Fatal("a fetch in flight at the deadline must still deliver, once")
	}

	i.join(closed, 5)
	i.join(fetched, 5)

	inFlight, _ := i.inbound.claim(closed, false)

	i.inbound.release(closed, "released_closed_swarm")
	i.inbound.release(protocol.ID{0xee}, "released_closed_swarm")
	i.inbound.fetchedOutgoing(fetched)

	if i.inbound.take(inFlight) || len(i.registered()) != 0 {
		t.Fatal("a released hash must not be delivered")
	}

	if stops := i.stops(); len(stops) != 4 {
		t.Fatalf("want one stop per membership, got %v", stops)
	}

	for stage, want := range map[string]float64{
		stageInboundExpired:     2,
		"released_closed_swarm": 1,
		stageInboundFetchedOut:  1,
		"outgoing_failed":       1,
	} {
		if got := hashCount(t, i.crawler, sourceInboundPeer, stage); got != want {
			t.Errorf("stage %s: want %v, got %v", stage, want, got)
		}
	}
}

func TestAnnounceTokenBuysTwoPackets(t *testing.T) {
	t.Parallel()

	s := offlineScraper(1)
	first, second := protocol.ID{1}, protocol.ID{2}
	member, usual, held, another := protocol.ID{3}, protocol.ID{4}, protocol.ID{5}, protocol.ID{6}

	s.cache.Add(first, trackerInfo{seeders: 40})
	s.announces[0][0] <- first
	s.announces[0][0] <- second

	for n, want := range []protocol.ID{first, second} {
		jobs := s.spendToken(0)
		if len(jobs) != 1 || jobs[0].hash != want || jobs[0].hold || jobs[0].stop {
			t.Fatalf("token %d without the feature: want the usual announce alone, got %+v", n, jobs)
		}
	}

	if jobs := s.spendToken(0); len(jobs) != 0 || s.stops != nil {
		t.Fatalf("want an idle token, got %+v", jobs)
	}

	verdicts := map[protocol.ID]joinVerdict{member: joinMember, usual: joinNone, held: joinHeld, another: joinMember}

	if !s.enableHolds(protocol.ID{0xaa}, 3334, 8, func(_ *trackerScraper, _ int, hash protocol.ID, _ uint32) joinVerdict {
		return verdicts[hash]
	}) {
		t.Fatal("holds did not turn on")
	}

	if s.holdParams.Port != 3334 || s.holdParams.NumWant != trackerAnnounceNumWant || s.holdParams.PeerID != (protocol.ID{0xaa}) {
		t.Fatalf("membership announces must carry the listener port and the usual numwant: %+v", s.holdParams)
	}

	for _, hash := range []protocol.ID{member, usual, held, another} {
		s.announces[0][1] <- hash
	}

	stop := func(n byte) protocol.ID { return protocol.ID{0x50, n} }

	for n := range byte(5) {
		s.stops[0] <- stop(n)
	}

	for n, want := range [][]announceJob{
		{{hash: member, hold: true}, {hash: stop(0), stop: true}},
		{{hash: usual}},
		{{hash: another, hold: true}, {hash: stop(1), stop: true}},
		{{hash: stop(2), stop: true}, {hash: stop(3), stop: true}},
		{{hash: stop(4), stop: true}},
		{},
	} {
		jobs := s.spendToken(0)
		packets := 0

		for _, job := range jobs {
			if !job.hold && !job.stop {
				packets++
			}

			packets++
		}

		if len(jobs) != len(want) || packets > announceTokenPackets {
			t.Fatalf("token %d: want %+v, got %+v", n, want, jobs)
		}

		for k := range want {
			if jobs[k] != want[k] {
				t.Fatalf("token %d: want %+v, got %+v", n, want, jobs)
			}
		}
	}

	s.markHeld(member, 0)
	s.release(member)
	s.release(member)
	s.release(usual)

	if len(s.stops[0]) != 1 || <-s.stops[0] != member || s.holding(member) {
		t.Fatal("want one stop for the one membership that was released")
	}

	for n := range byte(9) {
		s.markHeld(stop(n), 0)
		s.release(stop(n))
	}

	if len(s.stops[0]) != 8 {
		t.Fatalf("a full stop queue must drop, not block: %d queued", len(s.stops[0]))
	}

	for len(s.stops[0]) > 1 {
		<-s.stops[0]
	}

	s.announces[0][0] <- first
	s.draining.Store(true)

	if jobs := s.spendToken(0); len(jobs) != 1 || !jobs[0].stop || len(s.announces[0][0]) != 1 {
		t.Fatalf("a draining scraper sends stops only, got %+v", jobs)
	}

	var off *trackerScraper

	off.release(member)

	if off.enableHolds(protocol.ID{}, 1, 1, nil) {
		t.Fatal("a scraper that is off must not hold anything")
	}
}

func TestInboundFetchReachesPersistence(t *testing.T) {
	t.Parallel()

	for name, encrypted := range map[string]bool{"plaintext": false, "mse": true} {
		i := newInboundTest(t, inboundConfig{maxHashes: 10, hold: time.Minute, maxConns: 4}, offlineScraper(1))
		metadata, hash := inboundMetadata(t, "incoming "+name)

		if i.join(hash, 7) != joinMember {
			t.Fatalf("%s: the hash was not held", name)
		}

		i.inbound.outgoingFailed(sampleRequest(hash))
		i.clock.Add(int64(42 * time.Second))

		seen, err := peertest.Peer{InfoHash: hash, Metadata: metadata, Encrypted: encrypted}.Dial(i.address())
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		if seen.PeerID != [20]byte(i.inbound.receiver.PeerID()) {
			t.Fatalf("%s: the peer saw id %q", name, seen.PeerID[:])
		}

		got := i.persisted(t)
		if got.infoHash != hash || got.metaInfo.Name != "incoming "+name || got.metaInfo.PieceLength != 16384 {
			t.Fatalf("%s: persisted %s %q", name, got.infoHash, got.metaInfo.Name)
		}

		if got.source != sourceInboundPeer || got.origin != sourceSample || got.node.Port() != 6881 || got.class != "" {
			t.Fatalf("%s: persisted as %+v", name, got.nodeHasPeersForHash)
		}

		if _, err = createTorrentModel(got.infoHash, got.metaInfo, false, 100); err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		if !i.fetched.test(hash) || len(i.registered()) != 0 {
			t.Fatalf("%s: fetched %v, still registered %v", name, i.fetched.test(hash), i.registered())
		}

		if stops := i.stops(); len(stops) != 1 || stops[0] != hash {
			t.Fatalf("%s: want one stopped announce after the fetch, got %v", name, stops)
		}

		result := "ok"
		if encrypted {
			result = "ok_mse"
		}

		eventually(t, name+" result", func() bool { return dialCount(t, i.crawler, result) == 1 })

		for stage, want := range map[string]float64{
			stageInboundRegistered:            1,
			stageInboundConnAccepted:          1,
			stageInboundActiveConn:            1,
			"outgoing_failed":                 1,
			stageMetaInfoOK:                   1,
			stageMetaInfoOK + "_5_19":         1,
			stageMetaInfoOK + "_wait_seconds": 42,
			stageInboundHandshake:             0,
			stageInboundFetchFailed:           0,
		} {
			if got := hashCount(t, i.crawler, sourceInboundPeer, stage); got != want {
				t.Errorf("%s: stage %s: want %v, got %v", name, stage, want, got)
			}
		}

		late := peertest.Peer{InfoHash: hash, Metadata: metadata, Encrypted: encrypted}
		if _, err = late.Dial(i.address()); err == nil {
			t.Fatalf("%s: a hash that is no longer wanted was answered", name)
		}

		eventually(t, name+" unknown hash", func() bool {
			return hashCount(t, i.crawler, sourceInboundPeer, stageInboundUnknownHash) == 1
		})
		i.nothingPersisted(t)
	}
}

func TestInboundLateArrivals(t *testing.T) {
	t.Parallel()

	config := inboundConfig{maxHashes: 1, hold: time.Minute, maxConns: 4, lateMax: 2, lateTTL: 10 * time.Minute}
	i := newInboundTest(t, config, offlineScraper(1))
	metadata, hash := inboundMetadata(t, "late")
	stronger, third, fourth := protocol.ID{1}, protocol.ID{2}, protocol.ID{3}

	i.join(hash, 5)
	i.inbound.outgoingFailed(sampleRequest(hash))
	i.join(stronger, 9)

	if got := i.registered(); len(got) != 1 || !got[stronger] || i.inbound.lateSize() != 1 {
		t.Fatalf("want the replaced hash kept for late arrivals, got %v and %d late", got, i.inbound.lateSize())
	}

	i.clock.Add(int64(9 * time.Minute))
	i.inbound.expire()

	if len(i.registered()) != 0 || i.inbound.lateSize() != 2 {
		t.Fatalf("want the expired hash kept for late arrivals too, got %d late", i.inbound.lateSize())
	}

	if _, err := (peertest.Peer{InfoHash: hash, Metadata: metadata, Encrypted: true}).Dial(i.address()); err == nil {
		t.Fatal("a late hash must not be an MSE key")
	}

	if _, err := (peertest.Peer{InfoHash: hash, Metadata: metadata}).Dial(i.address()); err != nil {
		t.Fatal(err)
	}

	if got := i.persisted(t); got.infoHash != hash || got.metaInfo.Name != "late" || got.origin != sourceSample {
		t.Fatalf("persisted %+v", got.nodeHasPeersForHash)
	}

	if !i.fetched.test(hash) || i.inbound.lateSize() != 1 {
		t.Fatalf("the late hash must be gone after its fetch, %d late", i.inbound.lateSize())
	}

	i.fetched.add(stronger)

	if _, err := (peertest.Peer{InfoHash: stronger, Metadata: metadata}).Dial(i.address()); err == nil {
		t.Fatal("a late hash fetched meanwhile was answered")
	}

	eventually(t, "late counters", func() bool {
		return hashCount(t, i.crawler, sourceInboundPeer, stageInboundUnknownHash) == 2 && i.inbound.lateSize() == 0
	})

	for _, late := range []protocol.ID{third, fourth, {4}} {
		i.join(late, 5)
		i.clock.Add(int64(time.Minute))
		i.inbound.expire()
	}

	if i.inbound.lateSize() != 2 {
		t.Fatalf("the late set must stay within its bound, %d late", i.inbound.lateSize())
	}

	i.inbound.fetchedOutgoing(fourth)

	if i.join(protocol.ID{4}, 5) != joinMember || i.inbound.lateSize() != 0 {
		t.Fatalf("a late hash fetched by the outgoing path or held again must leave the late set")
	}

	i.clock.Add(int64(time.Minute))
	i.inbound.expire()
	i.clock.Add(int64(10 * time.Minute))
	i.inbound.expire()

	if i.inbound.lateSize() != 0 {
		t.Fatal("a late hash outlived its time")
	}

	for stage, want := range map[string]float64{
		stageInboundLateConn:             1,
		stageInboundActiveConn:           0,
		stageMetaInfoOK:                  1,
		stageMetaInfoOK + "_late":        1,
		stageInboundReplaced:             1,
		stageInboundExpired:              5,
		"late_evicted":                   1,
		"late_expired":                   1,
		"late_" + stageInboundFetchedOut: 1,
	} {
		if got := hashCount(t, i.crawler, sourceInboundPeer, stage); got != want {
			t.Errorf("stage %s: want %v, got %v", stage, want, got)
		}
	}
}

func TestInboundRefusesWhatItCannotUse(t *testing.T) {
	t.Parallel()

	i := newInboundTest(t, inboundConfig{maxHashes: 10, hold: time.Minute, maxConns: 4}, offlineScraper(1))
	metadata, hash := inboundMetadata(t, "wanted")
	forged, stranger := inboundMetadata(t, "forged")

	i.join(hash, 7)

	for _, test := range []struct {
		name   string
		peer   peertest.Peer
		stage  string
		result string
	}{
		{
			"unknown hash",
			peertest.Peer{InfoHash: stranger, Metadata: forged},
			stageInboundUnknownHash, stageInboundUnknownHash,
		},
		{"no meta info", peertest.Peer{InfoHash: hash}, stageInboundNoMetaInfo, "no_metadata_size"},
		{
			"oversized length",
			peertest.Peer{InfoHash: hash, Metadata: metadata, Prelude: []byte{0xff, 0xff, 0xff, 0xff}},
			stageInboundFetchFailed, "piece_invalid",
		},
		{"forged meta info", peertest.Peer{InfoHash: hash, Metadata: forged}, stageInboundFetchFailed, "other"},
		{
			"no extension protocol",
			peertest.Peer{InfoHash: hash, Metadata: metadata, NoExtensions: true},
			stageInboundHandshake, "no_extension_protocol",
		},
		{
			"closed network",
			peertest.Peer{InfoHash: hash, ID: [20]byte([]byte("-XT1000-04d9f58343e2")), Silent: true},
			stageInboundClosedNetwork, "closed_network_peer",
		},
	} {
		_, _ = test.peer.Dial(i.address())

		eventually(t, test.name, func() bool {
			return hashCount(t, i.crawler, sourceInboundPeer, test.stage) >= 1 &&
				dialCount(t, i.crawler, test.result) == 1
		})
	}

	if got := hashCount(t, i.crawler, sourceInboundPeer, stageInboundFetchFailed); got != 2 {
		t.Fatalf("want 2 failed fetches, got %v", got)
	}

	raw, err := (&net.Dialer{}).DialContext(context.Background(), "tcp4", i.address())
	if err != nil {
		t.Fatal(err)
	}

	_, _ = raw.Write([]byte("GET / HTTP/1.1\r\n\r\n"))
	_ = raw.Close()

	eventually(t, "handshake failure", func() bool {
		return hashCount(t, i.crawler, sourceInboundPeer, stageInboundHandshake) == 2 &&
			dialCount(t, i.crawler, "handshake_eof") == 1
	})

	i.nothingPersisted(t)

	if got := i.registered(); len(got) != 1 || !got[hash] || i.fetched.test(hash) {
		t.Fatalf("the hash must stay wanted, got %v", got)
	}

	eventually(t, "free connection slots", func() bool { return len(i.inbound.slots) == 0 })

	if _, err = (peertest.Peer{InfoHash: hash, Metadata: metadata}).Dial(i.address()); err != nil {
		t.Fatal(err)
	}

	if got := i.persisted(t); got.metaInfo.Name != "wanted" {
		t.Fatalf("persisted %q", got.metaInfo.Name)
	}
}

func TestInboundConnectionCaps(t *testing.T) {
	t.Parallel()

	i := newInboundTest(t, inboundConfig{maxHashes: 10, hold: time.Minute, maxConns: 1}, offlineScraper(1))

	silent, err := (&net.Dialer{}).DialContext(context.Background(), "tcp4", i.address())
	if err != nil {
		t.Fatal(err)
	}

	defer silent.Close()

	eventually(t, "the first connection", func() bool { return len(i.inbound.slots) == 1 })

	refused, err := (&net.Dialer{}).DialContext(context.Background(), "tcp4", i.address())
	if err != nil {
		t.Fatal(err)
	}

	defer refused.Close()

	_ = refused.SetDeadline(time.Now().Add(time.Second))

	if _, err = refused.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("a connection over the cap must be closed at once, got %v", err)
	}

	if got := hashCount(t, i.crawler, sourceInboundPeer, stageInboundConnRejected); got != 1 {
		t.Fatalf("want 1 rejected connection, got %v", got)
	}

	_ = silent.Close()

	eventually(t, "the slot to free up", func() bool { return len(i.inbound.slots) == 0 })

	metadata, hash := inboundMetadata(t, "busy")
	i.join(hash, 7)

	claims := make([]*inboundEntry, 0, inboundConnsPerHash)

	for range inboundConnsPerHash {
		entry, _ := i.inbound.claim(hash, false)
		claims = append(claims, entry)
	}

	_, _ = peertest.Peer{InfoHash: hash, Metadata: metadata}.Dial(i.address())

	eventually(t, "the per-hash cap", func() bool {
		return hashCount(t, i.crawler, sourceInboundPeer, stageInboundBusy) == 1
	})

	for _, entry := range claims {
		i.inbound.unclaim(entry)
	}

	i.nothingPersisted(t)
}

func TestInboundCapPerHost(t *testing.T) {
	t.Parallel()

	i := newInboundTest(t, inboundConfig{maxHashes: 10, hold: time.Minute, maxConns: 10, maxPerIP: 2}, offlineScraper(1))
	silent := make([]net.Conn, 0, 2)

	for range 2 {
		conn, err := (&net.Dialer{}).DialContext(context.Background(), "tcp4", i.address())
		if err != nil {
			t.Fatal(err)
		}

		defer conn.Close()

		silent = append(silent, conn)
	}

	eventually(t, "two connections of one host", func() bool { return len(i.inbound.slots) == 2 })

	refused, err := (&net.Dialer{}).DialContext(context.Background(), "tcp4", i.address())
	if err != nil {
		t.Fatal(err)
	}

	defer refused.Close()

	_ = refused.SetDeadline(time.Now().Add(time.Second))

	if _, err = refused.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("a third connection of the same host must be closed at once, got %v", err)
	}

	if hashCount(t, i.crawler, sourceInboundPeer, stageInboundConnRejectedIP) != 1 ||
		hashCount(t, i.crawler, sourceInboundPeer, stageInboundConnRejected) != 0 || len(i.inbound.slots) != 2 {
		t.Fatal("want the refusal counted for the host, not for the global cap")
	}

	_ = silent[0].Close()

	eventually(t, "the host to get a slot back", func() bool { return len(i.inbound.slots) == 1 })

	metadata, hash := inboundMetadata(t, "per host")
	i.join(hash, 7)

	if _, err = (peertest.Peer{InfoHash: hash, Metadata: metadata}).Dial(i.address()); err != nil {
		t.Fatal(err)
	}

	i.persisted(t)

	near := inboundHost(&net.TCPAddr{IP: net.ParseIP("2001:db8:1:2:aaaa::1"), Port: 1})
	same := inboundHost(&net.TCPAddr{IP: net.ParseIP("2001:db8:1:2:bbbb::2"), Port: 2})
	other := inboundHost(&net.TCPAddr{IP: net.ParseIP("2001:db8:1:3::1"), Port: 1})
	mapped := inboundHost(&net.TCPAddr{IP: net.ParseIP("::ffff:192.0.2.7"), Port: 1})

	if near != same || near == other || mapped != netip.MustParseAddr("192.0.2.7") {
		t.Fatalf("hosts: %s %s %s %s", near, same, other, mapped)
	}
}

func TestInboundBannedAndPanickingChecks(t *testing.T) {
	t.Parallel()

	i := newInboundTest(t, inboundConfig{maxHashes: 10, hold: time.Minute, maxConns: 4}, offlineScraper(1))
	i.banningChecker = banAll{}

	metadata, hash := inboundMetadata(t, "banned")
	i.join(hash, 7)

	_, _ = peertest.Peer{InfoHash: hash, Metadata: metadata}.Dial(i.address())

	eventually(t, "the ban", func() bool { return hashCount(t, i.crawler, sourceInboundPeer, stageBanned) == 1 })
	i.nothingPersisted(t)

	i.blocks.mu.Lock()
	blocked := append([]protocol.ID(nil), i.blocks.blocked...)
	i.blocks.mu.Unlock()

	if len(blocked) != 1 || blocked[0] != hash || len(i.registered()) != 0 || i.fetched.test(hash) {
		t.Fatalf("blocked %v, registered %v", blocked, i.registered())
	}

	j := newInboundTest(t, inboundConfig{maxHashes: 10, hold: time.Minute, maxConns: 4}, offlineScraper(1))
	j.banningChecker = panicking{}

	j.join(hash, 7)

	_, _ = peertest.Peer{InfoHash: hash, Metadata: metadata}.Dial(j.address())

	eventually(t, "the recovered panic", func() bool {
		return hashCount(t, j.crawler, sourceInboundPeer, "panic") == 1 && len(j.inbound.slots) == 0
	})
	j.nothingPersisted(t)

	if entry, _ := j.inbound.claim(hash, false); entry == nil || entry.active != 1 {
		t.Fatalf("a panic must give the claim on the hash back, got %+v", entry)
	}
}

type dhtAnnounce struct {
	addr  netip.AddrPort
	hash  protocol.ID
	port  uint16
	token string
}

type announcingClient struct {
	client.Client
	mu        sync.Mutex
	nodes     map[netip.AddrPort]protocol.ID
	refusing  netip.AddrPort
	announces []dhtAnnounce
}

func (a *announcingClient) GetPeers(
	_ context.Context, addr netip.AddrPort, _ protocol.ID,
) (client.GetPeersResult, error) {
	id, ok := a.nodes[addr]
	if !ok {
		return client.GetPeersResult{}, errors.New("no reply")
	}

	return client.GetPeersResult{ID: id, Token: "token of " + addr.String()}, nil
}

func (a *announcingClient) AnnouncePeer(
	_ context.Context, addr netip.AddrPort, hash protocol.ID, port uint16, token string,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.announces = append(a.announces, dhtAnnounce{addr: addr, hash: hash, port: port, token: token})

	if addr == a.refusing {
		return errors.New("bad token")
	}

	return nil
}

func (a *announcingClient) seen() []dhtAnnounce {
	a.mu.Lock()
	defer a.mu.Unlock()

	return append([]dhtAnnounce(nil), a.announces...)
}

func TestInboundDHTAnnounce(t *testing.T) {
	t.Parallel()

	hash := protocol.ID{0x80}
	config := inboundConfig{maxHashes: 10, hold: time.Minute, maxConns: 4}

	newClient := func() *announcingClient {
		return &announcingClient{
			nodes:    make(map[netip.AddrPort]protocol.ID),
			refusing: netip.MustParseAddrPort("192.0.2.3:6881"),
		}
	}

	start := func(cl *announcingClient, config inboundConfig) inboundTest {
		c := newFamilyTestCrawler(false, cl)

		for n := 1; n <= 10; n++ {
			addr := netip.AddrPortFrom(netip.AddrFrom4([4]byte{192, 0, 2, byte(n)}), 6881)
			cl.nodes[addr] = protocol.ID{0x80 + byte(n)}
			c.v4.nodePool.add(cl.nodes[addr], addr)
		}

		c.v4.nodePool.rebuild()

		return startInboundTest(t, c, config, offlineScraper(1))
	}

	off := newClient()
	i := start(off, config)

	i.join(hash, 7)

	if i.inbound.dhtQueue != nil || len(i.registered()) != 1 {
		t.Fatal("without the switch nothing may be queued for the DHT")
	}

	config.dht = true
	on := newClient()
	i = start(on, config)

	i.join(hash, 7)

	eventually(t, "the DHT announce", func() bool {
		return hashCount(t, i.crawler, sourceInboundPeer, "dht_announced") == 1
	})

	announces := on.seen()
	if len(announces) != inboundDHTNodes {
		t.Fatalf("want an announce to the %d closest nodes, got %d", inboundDHTNodes, len(announces))
	}

	port := listenerPort(i.inbound.listeners[0])

	for _, announce := range announces {
		id, known := on.nodes[announce.addr]
		if !known || id[0] > 0x88 {
			t.Errorf("announce to %s, which is not among the closest nodes", announce.addr)
		}

		if announce.hash != hash || announce.port != port || announce.token != "token of "+announce.addr.String() {
			t.Errorf("announce %+v, want the listener port %d and the token of that node", announce, port)
		}
	}

	if got := hashCount(t, i.crawler, sourceInboundPeer, "dht_announce_nodes"); got != inboundDHTNodes-1 {
		t.Fatalf("want %d nodes to accept, got %v", inboundDHTNodes-1, got)
	}

	if len(off.seen()) != 0 {
		t.Fatalf("the DHT announce is off, yet %d announces were sent", len(off.seen()))
	}

	silent := newClient()
	i = start(silent, config)
	silent.nodes = map[netip.AddrPort]protocol.ID{}

	i.join(hash, 7)

	eventually(t, "the failed DHT announce", func() bool {
		return hashCount(t, i.crawler, sourceInboundPeer, "dht_announce_failed") == 1
	})
}

type heldAnnounce struct {
	event   uint32
	hash    protocol.ID
	left    uint64
	port    uint16
	numWant uint32
	at      time.Time
}

func fakeHoldTracker(t *testing.T) (string, chan heldAnnounce, *atomic.Int64) {
	t.Helper()

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	announces := make(chan heldAnnounce, 64)
	startedDelay := &atomic.Int64{}

	go func() {
		buffer := make([]byte, 2048)

		for {
			n, from, readErr := conn.ReadFromUDP(buffer)
			if readErr != nil {
				return
			}

			if n < 16 {
				continue
			}

			packet := buffer[:n]
			response := make([]byte, 8, 64)
			copy(response[4:8], packet[12:16])

			switch action := binary.BigEndian.Uint32(packet[8:12]); {
			case action == 0:
				response = binary.BigEndian.AppendUint64(response, 0x1122334455667788)
			case action == 2:
				binary.BigEndian.PutUint32(response[0:4], 2)

				for at := 16; at+20 <= n; at += 20 {
					response = binary.BigEndian.AppendUint32(response, uint32(packet[at])+1)
					response = append(response, make([]byte, 8)...)
				}
			case action == 1 && n == 98:
				seen := heldAnnounce{
					event:   binary.BigEndian.Uint32(packet[80:84]),
					hash:    protocol.ID(packet[16:36]),
					left:    binary.BigEndian.Uint64(packet[64:72]),
					port:    binary.BigEndian.Uint16(packet[96:98]),
					numWant: binary.BigEndian.Uint32(packet[92:96]),
					at:      time.Now(),
				}
				announces <- seen

				if seen.event != 2 {
					continue
				}

				binary.BigEndian.PutUint32(response[0:4], 1)
				response = append(response, make([]byte, 12)...)
				response = append(response, 192, 0, 2, 1, 0x1a, 0xe1)

				if delay := time.Duration(startedDelay.Load()); delay > 0 {
					go func() {
						time.Sleep(delay)

						_, _ = conn.WriteToUDP(response, from)
					}()

					continue
				}
			default:
				continue
			}

			_, _ = conn.WriteToUDP(response, from)
		}
	}()

	return conn.LocalAddr().String(), announces, startedDelay
}

func TestInboundSwitchAndTrackerMembership(t *testing.T) {
	address, announces, startedDelay := fakeHoldTracker(t)

	t.Setenv("BITMAGNET_TRACKERS", address)
	t.Setenv("BITMAGNET_TRACKER_ANNOUNCE", "")
	t.Setenv("BITMAGNET_TRACKER_ANNOUNCE_PORT", "")
	t.Setenv("BITMAGNET_TRACKER_ANNOUNCE_PPS", "")
	t.Setenv("BITMAGNET_INBOUND_MAX_HASHES", "")
	t.Setenv("BITMAGNET_INBOUND_DHT", "")
	t.Setenv("BITMAGNET_INBOUND_LATE_HASHES", "")
	t.Setenv("BITMAGNET_INBOUND_LATE_SECONDS", "")
	t.Setenv("BITMAGNET_INBOUND_MAX_CONNS_PER_IP", "")

	receiver := metainforequester.NewReceiver(protocol.RandomPeerID(), 2*time.Second)
	metadata, hash := inboundMetadata(t, "held")
	expiring, closed, late := protocol.ID{9, 9, 9}, protocol.ID{8, 8, 8}, protocol.ID{7, 7, 7}
	fed := make(chan protocol.ID, 16)

	next := func(event uint32, wanted protocol.ID, port uint16) heldAnnounce {
		t.Helper()

		select {
		case seen := <-announces:
			if seen.event != event || seen.hash != wanted || seen.port != port {
				t.Fatalf("want event %d for %s from port %d, got %+v", event, wanted, port, seen)
			}

			return seen
		case <-time.After(5 * time.Second):
			t.Fatalf("the tracker saw no event %d for %s", event, wanted)

			return heldAnnounce{}
		}
	}

	peersFed := func(wanted protocol.ID) {
		t.Helper()

		select {
		case got := <-fed:
			if got != wanted {
				t.Fatalf("peers of %s were handed on, want %s", got, wanted)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("the peers of %s never reached the outgoing path", wanted)
		}
	}

	quiet := func(what string) {
		t.Helper()

		select {
		case seen := <-announces:
			t.Fatalf("%s: unexpected announce %+v", what, seen)
		case <-time.After(300 * time.Millisecond):
		}
	}

	newCrawler := func() (*crawler, context.CancelFunc) {
		scraper, err := newTrackerScraper(newTrackerMetrics(), "udp4")
		if err != nil {
			t.Fatal(err)
		}

		scraper.onPeers = func(_ context.Context, hash protocol.ID, _ string, _ []netip.AddrPort) {
			fed <- hash
		}

		c := newTestCrawler(nil, nil)
		c.logger = zap.NewNop().Sugar()
		c.fetched = &ignoreFilter{bloom: boom.NewStableBloomFilter(1000, 2, 0.001)}
		c.persistTorrents = concurrency.NewBatchingChannel[infoHashWithMetaInfo](10, 1, 10*time.Millisecond)
		c.trackers = scraper

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		c.setupInboundPeers(receiver, 0)

		go scraper.run(ctx)
		go c.inbound.run(ctx)

		return c, cancel
	}

	sent := func(c *crawler, kind string) float64 {
		return counterValue(t, c.trackers.metrics.sent, map[string]string{"kind": kind})
	}

	for _, value := range []string{"", "off", "1"} {
		t.Setenv("BITMAGNET_INBOUND_PEERS", value)

		c, cancel := newCrawler()
		if c.inbound != nil || c.trackers.stops != nil || c.trackers.join != nil {
			t.Fatalf("BITMAGNET_INBOUND_PEERS=%q must leave the feature off", value)
		}

		c.trackers.submit(hash)

		if seen := next(2, hash, 6881); seen.numWant != trackerAnnounceNumWant {
			t.Fatalf("usual announce: numwant %d", seen.numWant)
		}

		next(3, hash, 6881)
		peersFed(hash)

		c.inbound.outgoingFailed(sampleRequest(hash))
		c.inbound.release(hash, "released_closed_swarm")
		c.inbound.fetchedOutgoing(hash)
		quiet("feature off")

		if sent(c, tracker.SentStarted) != 1 || sent(c, tracker.SentStopped) != 1 || sent(c, tracker.SentScrape) != 1 {
			t.Fatalf("feature off: want one scrape, one started and one stopped packet")
		}

		cancel()
	}

	t.Setenv("BITMAGNET_INBOUND_PEERS", "on")
	t.Setenv("BITMAGNET_INBOUND_HOLD_SECONDS", "1")
	t.Setenv("BITMAGNET_INBOUND_MAX_CONNS", "3")

	c, _ := newCrawler()
	if c.inbound == nil {
		t.Fatal("BITMAGNET_INBOUND_PEERS=on must start the listener")
	}

	if got := c.inbound.config; got != (inboundConfig{
		maxHashes: 60, hold: time.Second, maxConns: 3, maxPerIP: 8, lateMax: 50_000, lateTTL: 15 * time.Minute,
	}) {
		t.Fatalf("want room for two trackers announcing 30 hashes a second for the hold of one second, got %+v", got)
	}

	port := listenerPort(c.inbound.listeners[0])
	submitted := time.Now()

	c.trackers.submit(expiring)

	if seen := next(2, expiring, port); seen.numWant != trackerAnnounceNumWant || seen.left == 0 {
		t.Fatalf("the usual announce must be the start of the membership: %+v", seen)
	}

	peersFed(expiring)

	if !c.inbound.wanted(expiring) {
		t.Fatal("the hash must be registered by the time its announce is out")
	}

	c.inbound.outgoingFailed(sampleRequest(expiring))

	if held := next(3, expiring, port).at.Sub(submitted); held < time.Second {
		t.Fatalf("the swarm was left after %s, before the hold of one second ended", held)
	}

	c.inbound.mu.Lock()
	c.inbound.config.hold = time.Minute
	c.inbound.mu.Unlock()

	c.trackers.submit(hash)
	next(2, hash, port)
	peersFed(hash)

	seeder := peertest.Peer{InfoHash: hash, Metadata: metadata}
	if _, err := seeder.Dial(fmt.Sprintf("127.0.0.1:%d", port)); err != nil {
		t.Fatal(err)
	}

	next(3, hash, port)

	select {
	case batch := <-c.persistTorrents.Out():
		if len(batch) != 1 || batch[0].infoHash != hash || batch[0].metaInfo.Name != "held" {
			t.Fatalf("persisted %+v", batch)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing reached persistence")
	}

	c.trackers.submit(closed)
	next(2, closed, port)
	peersFed(closed)
	c.inbound.release(closed, "released_closed_swarm")
	next(3, closed, port)

	startedDelay.Store(int64(300 * time.Millisecond))
	c.trackers.submit(late)
	next(2, late, port)
	c.inbound.fetchedOutgoing(late)
	next(3, late, port)
	next(3, late, port)
	peersFed(late)
	startedDelay.Store(0)
	quiet("after the last membership")

	c.trackers.submit(protocol.ID{6, 6, 6})
	next(2, protocol.ID{6, 6, 6}, port)
	peersFed(protocol.ID{6, 6, 6})
	c.inbound.leaveAll(3 * time.Second)
	next(3, protocol.ID{6, 6, 6}, port)

	if c.inbound.size() != 0 || c.inbound.join(c.trackers, 0, protocol.ID{5, 5, 5}, 9) != joinNone {
		t.Fatal("after leaving every swarm nothing may be held")
	}

	if sent(c, tracker.SentStarted) != 5 || sent(c, tracker.SentStopped) != 6 {
		t.Fatalf("want one started and one stopped per hash plus the second stop after the late start, got %v and %v",
			sent(c, tracker.SentStarted), sent(c, tracker.SentStopped))
	}

	for stage, want := range map[string]float64{
		stageInboundRegistered:  5,
		"announced":             4,
		"outgoing_failed":       1,
		stageInboundExpired:     1,
		stageInboundFetchedOut:  1,
		"released_closed_swarm": 1,
		stageMetaInfoOK:         1,
	} {
		if got := hashCount(t, c, sourceInboundPeer, stage); got != want {
			t.Errorf("stage %s: want %v, got %v", stage, want, got)
		}
	}

	taken, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp4", ":0")
	if err != nil {
		t.Fatal(err)
	}

	defer taken.Close()

	busy := newTestCrawler(nil, nil)
	busy.logger = zap.NewNop().Sugar()
	busy.trackers = offlineScraper(1)
	busy.setupInboundPeers(receiver, listenerPort(taken))

	if busy.inbound != nil || busy.trackers.stops != nil {
		t.Fatal("a taken port must leave the feature off")
	}

	silenced := newTestCrawler(nil, nil)
	silenced.logger = zap.NewNop().Sugar()
	silenced.trackers = offlineScraper(1)
	silenced.trackers.announceEnabled = false
	silenced.setupInboundPeers(receiver, 0)

	if silenced.inbound != nil {
		t.Fatal("without tracker announces the feature must stay off")
	}
}

func BenchmarkInboundJoinWhenFull(b *testing.B) {
	for _, size := range []int{2000, 20000} {
		c := newTestCrawler(nil, nil)
		c.fetched = &ignoreFilter{bloom: boom.NewStableBloomFilter(1000, 2, 0.001)}

		p := &inboundPeers{
			c:         c,
			config:    inboundConfig{maxHashes: size, hold: time.Hour, lateMax: 50_000, lateTTL: time.Hour},
			now:       time.Now,
			entries:   make(map[protocol.ID]*inboundEntry),
			late:      make(map[protocol.ID]*inboundEntry),
			lateOrder: list.New(),
		}

		s := offlineScraper(1)
		s.enableHolds(protocol.ID{}, 1, size, p.join)
		p.trackers = []*trackerScraper{s}

		hash := func(n int) (id protocol.ID) {
			binary.BigEndian.PutUint64(id[:], uint64(n+1)*0x9e3779b97f4a7c15)

			return id
		}

		for n := range size {
			p.join(s, 0, hash(n), uint32(n%50+1))
		}

		next := size

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			for range b.N {
				next++

				if p.join(s, 0, hash(next), uint32(1000+next)) != joinMember {
					b.Fatal("the stronger hash was refused")
				}
			}
		})
	}
}
