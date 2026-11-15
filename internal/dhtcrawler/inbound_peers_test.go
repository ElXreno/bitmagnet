package dhtcrawler

import (
	"bytes"
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
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester/peertest"
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

	blocks := &blockRecorder{}
	c := newTestCrawler(nil, nil)
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

func (i inboundTest) seed(hash protocol.ID, seeders uint32, seededBy uint32) nodeHasPeersForHash {
	i.scraper.cache.Add(hash, trackerInfo{seeders: seeders, seededBy: seededBy})

	return nodeHasPeersForHash{
		infoHash: hash,
		node:     netip.MustParseAddrPort("192.0.2.9:6881"),
		source:   sourceSample,
		class:    "fresh",
	}
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

	i := newInboundTest(t, inboundConfig{maxHashes: 2, hold: time.Minute, maxConns: 4}, offlineScraper(1))
	five, ten, three, alsoFive, seven := protocol.ID{1}, protocol.ID{2}, protocol.ID{3}, protocol.ID{4}, protocol.ID{5}

	i.inbound.register(nodeHasPeersForHash{infoHash: protocol.ID{9}})
	i.scraper.cache.Add(protocol.ID{8}, trackerInfo{leechers: 4})
	i.inbound.register(nodeHasPeersForHash{infoHash: protocol.ID{8}})

	fetched := i.seed(protocol.ID{7}, 50, 1)
	i.fetched.add(protocol.ID{7})
	i.inbound.register(fetched)

	if len(i.registered()) != 0 {
		t.Fatalf("a hash without seeders or one fetched already was registered: %v", i.registered())
	}

	i.inbound.register(i.seed(five, 5, 1))
	i.inbound.register(i.seed(ten, 10, 1))
	i.inbound.register(i.seed(five, 5, 1))

	if got := i.registered(); len(got) != 2 || !got[five] || !got[ten] {
		t.Fatalf("want the first two hashes, got %v", got)
	}

	i.inbound.register(i.seed(three, 3, 1))
	i.inbound.register(i.seed(alsoFive, 5, 1))

	if got := i.registered(); len(got) != 2 || !got[five] || !got[ten] {
		t.Fatalf("a hash that is not stronger than the weakest one got in: %v", got)
	}

	i.inbound.register(i.seed(seven, 7, 1))

	if got := i.registered(); len(got) != 2 || !got[seven] || !got[ten] {
		t.Fatalf("want the stronger hash in place of the weakest, got %v", got)
	}

	busySeven, _ := i.inbound.claim(seven)
	busyTen, _ := i.inbound.claim(ten)

	i.inbound.register(i.seed(protocol.ID{6}, 100, 1))

	if got := i.registered(); len(got) != 2 || !got[seven] || !got[ten] {
		t.Fatalf("a hash with a fetch in flight was evicted: %v", got)
	}

	i.inbound.unclaim(busyTen)
	i.inbound.register(i.seed(protocol.ID{6}, 100, 1))

	if got := i.registered(); len(got) != 2 || !got[seven] || !got[protocol.ID{6}] {
		t.Fatalf("want the idle hash replaced and the busy one kept, got %v", got)
	}

	i.inbound.unclaim(busySeven)

	for stage, want := range map[string]float64{
		stageInboundRegistered:           4,
		stageInboundRegistered + "_5_19": 3,
		stageInboundRegistered + "_20":   1,
		stageInboundSkippedFull:          3,
		stageInboundReplaced:             2,
	} {
		if got := hashCount(t, i.crawler, sourceInboundPeer, stage); got != want {
			t.Errorf("stage %s: want %v, got %v", stage, want, got)
		}
	}
}

func TestInboundRegistryExpiry(t *testing.T) {
	t.Parallel()

	i := newInboundTest(t, inboundConfig{maxHashes: 10, hold: time.Minute, maxConns: 4}, offlineScraper(1))
	idle, busy := protocol.ID{1}, protocol.ID{2}

	i.inbound.register(i.seed(idle, 5, 1))
	i.inbound.register(i.seed(busy, 5, 1))

	if job, ok := i.scraper.nextAnnounce(0); !ok || !job.hold {
		t.Fatalf("want a hold announce queued, got %+v", job)
	}

	claimed, _ := i.inbound.claim(busy)

	i.clock.Add(int64(59 * time.Second))
	i.inbound.expire()

	if len(i.registered()) != 2 {
		t.Fatal("a hash expired before its deadline")
	}

	i.clock.Add(int64(time.Second))
	i.inbound.expire()

	if got := i.registered(); len(got) != 1 || !got[busy] {
		t.Fatalf("want only the hash with a fetch in flight to stay, got %v", got)
	}

	i.inbound.unclaim(claimed)
	i.inbound.expire()

	if len(i.registered()) != 0 {
		t.Fatal("the hash stayed after its fetch ended")
	}

	if got := hashCount(t, i.crawler, sourceInboundPeer, "expired"); got != 2 {
		t.Fatalf("want 2 expired hashes, got %v", got)
	}

	stops := 0

	for {
		job, ok := i.scraper.nextAnnounce(0)
		if !ok {
			break
		}

		if job.stop {
			stops++
		}
	}

	if stops != 1 {
		t.Fatalf("want a stopped announce for the one swarm that was joined, got %d", stops)
	}
}

func TestHoldAnnouncesKeepTheSeederPriority(t *testing.T) {
	t.Parallel()

	s := offlineScraper(2)
	plain, many, some, elsewhere := protocol.ID{1}, protocol.ID{2}, protocol.ID{3}, protocol.ID{4}

	s.hold(many)

	if s.holdableSeeders(many) != 0 || s.holdQueues != nil {
		t.Fatal("holds must stay off until they are enabled")
	}

	s.cache.Add(many, trackerInfo{seeders: 25, seededBy: 0b01})
	s.cache.Add(some, trackerInfo{seeders: 6, seededBy: 0b01})
	s.cache.Add(elsewhere, trackerInfo{seeders: 30, seededBy: 0b10})
	s.cache.Add(plain, trackerInfo{seeders: 40})
	s.announces[0][0] <- plain
	s.announces[0][1] <- plain

	if job, ok := s.nextAnnounce(0); !ok || job.hash != plain || job.hold || job.stop || job.seeders != 40 {
		t.Fatalf("without holds the queue must work as before, got %+v", job)
	}

	s.announces[0][0] <- plain

	if !s.enableHolds(protocol.ID{0xaa}, 3334) || s.holdParams.Port != 3334 || s.holdParams.NumWant != 0 {
		t.Fatalf("holds did not turn on: %+v", s.holdParams)
	}

	s.hold(some)
	s.hold(many)
	s.hold(elsewhere)
	s.hold(protocol.ID{0xff})

	type step struct {
		hash protocol.ID
		hold bool
	}

	for n, want := range []step{{plain, false}, {many, true}, {plain, false}, {some, true}} {
		job, ok := s.nextAnnounce(0)
		if !ok || job.hash != want.hash || job.hold != want.hold || job.stop {
			t.Fatalf("step %d: want %+v, got %+v", n, want, job)
		}
	}

	if job, ok := s.nextAnnounce(0); ok {
		t.Fatalf("tracker 0 saw no seeders for the last hash, got %+v", job)
	}

	s.release(elsewhere)

	if job, ok := s.nextAnnounce(1); ok {
		t.Fatalf("a hold released before it was sent must be dropped without a stop, got %+v", job)
	}

	s.release(many)
	s.release(many)

	if job, ok := s.nextAnnounce(0); !ok || job.hash != many || !job.stop || job.hold {
		t.Fatalf("want one stop for the released swarm, got %+v", job)
	}

	if job, ok := s.nextAnnounce(0); ok {
		t.Fatalf("want nothing after the stop, got %+v", job)
	}

	var off *trackerScraper

	off.hold(many)
	off.release(many)

	if off.enableHolds(protocol.ID{}, 1) || off.holdableSeeders(many) != 0 {
		t.Fatal("a scraper that is off must not hold anything")
	}
}

func TestInboundFetchReachesPersistence(t *testing.T) {
	t.Parallel()

	for name, encrypted := range map[string]bool{"plaintext": false, "mse": true} {
		i := newInboundTest(t, inboundConfig{maxHashes: 10, hold: time.Minute, maxConns: 4}, offlineScraper(1))
		metadata, hash := inboundMetadata(t, "incoming "+name)

		i.inbound.register(i.seed(hash, 7, 1))

		if job, ok := i.scraper.nextAnnounce(0); !ok || !job.hold || job.hash != hash {
			t.Fatalf("%s: want a hold announce for the hash, got %+v", name, job)
		}

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

		if job, ok := i.scraper.nextAnnounce(0); !ok || !job.stop || job.hash != hash {
			t.Fatalf("%s: want a stopped announce after the fetch, got %+v", name, job)
		}

		result := "ok"
		if encrypted {
			result = "ok_mse"
		}

		eventually(t, name+" result", func() bool { return dialCount(t, i.crawler, result) == 1 })

		for stage, want := range map[string]float64{
			stageInboundRegistered:            1,
			stageInboundConnAccepted:          1,
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
			t.Fatalf("%s: a hash that is no longer registered was answered", name)
		}

		eventually(t, name+" unknown hash", func() bool {
			return hashCount(t, i.crawler, sourceInboundPeer, stageInboundUnknownHash) == 1
		})
		i.nothingPersisted(t)
	}
}

func TestInboundRefusesWhatItCannotUse(t *testing.T) {
	t.Parallel()

	i := newInboundTest(t, inboundConfig{maxHashes: 10, hold: time.Minute, maxConns: 4}, offlineScraper(1))
	metadata, hash := inboundMetadata(t, "wanted")
	forged, stranger := inboundMetadata(t, "forged")

	i.inbound.register(i.seed(hash, 7, 1))

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
	i.inbound.register(i.seed(hash, 7, 1))

	claims := make([]*inboundEntry, 0, inboundConnsPerHash)

	for range inboundConnsPerHash {
		entry, _ := i.inbound.claim(hash)
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

func TestInboundBannedAndPanickingChecks(t *testing.T) {
	t.Parallel()

	i := newInboundTest(t, inboundConfig{maxHashes: 10, hold: time.Minute, maxConns: 4}, offlineScraper(1))
	i.banningChecker = banAll{}

	metadata, hash := inboundMetadata(t, "banned")
	i.inbound.register(i.seed(hash, 7, 1))

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

	j.inbound.register(j.seed(hash, 7, 1))

	_, _ = peertest.Peer{InfoHash: hash, Metadata: metadata}.Dial(j.address())

	eventually(t, "the recovered panic", func() bool {
		return hashCount(t, j.crawler, sourceInboundPeer, "panic") == 1 && len(j.inbound.slots) == 0
	})
	j.nothingPersisted(t)

	if entry, _ := j.inbound.claim(hash); entry == nil || entry.active != 1 {
		t.Fatalf("a panic must give the claim on the hash back, got %+v", entry)
	}
}

type heldAnnounce struct {
	event uint32
	hash  protocol.ID
	left  uint64
	port  uint16
}

func fakeHoldTracker(t *testing.T) (string, chan heldAnnounce) {
	t.Helper()

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	announces := make(chan heldAnnounce, 64)

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
					response = binary.BigEndian.AppendUint32(response, uint32(packet[at]))
					response = append(response, make([]byte, 8)...)
				}
			case action == 1 && n == 98:
				seen := heldAnnounce{
					event: binary.BigEndian.Uint32(packet[80:84]),
					hash:  protocol.ID(packet[16:36]),
					left:  binary.BigEndian.Uint64(packet[64:72]),
					port:  binary.BigEndian.Uint16(packet[96:98]),
				}
				announces <- seen

				if seen.event != 2 {
					continue
				}

				binary.BigEndian.PutUint32(response[0:4], 1)
				response = append(response, make([]byte, 12)...)
			default:
				continue
			}

			_, _ = conn.WriteToUDP(response, from)
		}
	}()

	return conn.LocalAddr().String(), announces
}

func nextAnnounce(t *testing.T, announces chan heldAnnounce) heldAnnounce {
	t.Helper()

	select {
	case seen := <-announces:
		return seen
	case <-time.After(5 * time.Second):
		t.Fatal("the tracker saw no announce")

		return heldAnnounce{}
	}
}

func TestInboundSwitchAndTrackerHold(t *testing.T) {
	address, announces := fakeHoldTracker(t)

	t.Setenv("BITMAGNET_TRACKERS", address)
	t.Setenv("BITMAGNET_TRACKER_ANNOUNCE", "")
	t.Setenv("BITMAGNET_TRACKER_ANNOUNCE_PORT", "")

	receiver := metainforequester.NewReceiver(protocol.RandomPeerID(), 2*time.Second)
	metadata, hash := inboundMetadata(t, "held")
	hash2 := protocol.ID{9, 9, 9}

	newCrawler := func() (*crawler, context.CancelFunc) {
		scraper, err := newTrackerScraper(newTrackerMetrics(), "udp4")
		if err != nil {
			t.Fatal(err)
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

	scrapeAndAnnounce := func(c *crawler, wanted protocol.ID) {
		c.trackers.submit(wanted)

		for _, event := range []uint32{2, 3} {
			if seen := nextAnnounce(t, announces); seen.event != event || seen.hash != wanted || seen.port != 6881 {
				t.Fatalf("want the usual announce event %d from port 6881, got %+v", event, seen)
			}
		}
	}

	for _, value := range []string{"", "off", "1"} {
		t.Setenv("BITMAGNET_INBOUND_PEERS", value)

		c, cancel := newCrawler()
		if c.inbound != nil || c.trackers.holdQueues != nil {
			t.Fatalf("BITMAGNET_INBOUND_PEERS=%q must leave the feature off", value)
		}

		scrapeAndAnnounce(c, hash)
		c.inbound.register(nodeHasPeersForHash{infoHash: hash, source: sourceSample})
		c.inbound.fetchedOutgoing(hash)

		select {
		case seen := <-announces:
			t.Fatalf("BITMAGNET_INBOUND_PEERS=%q: unexpected announce %+v", value, seen)
		case <-time.After(200 * time.Millisecond):
		}

		cancel()
	}

	t.Setenv("BITMAGNET_INBOUND_PEERS", "on")
	t.Setenv("BITMAGNET_INBOUND_MAX_HASHES", "7")
	t.Setenv("BITMAGNET_INBOUND_HOLD_SECONDS", "1")
	t.Setenv("BITMAGNET_INBOUND_MAX_CONNS", "3")

	c, _ := newCrawler()
	if c.inbound == nil {
		t.Fatal("BITMAGNET_INBOUND_PEERS=on must start the listener")
	}

	if got := c.inbound.config; got != (inboundConfig{maxHashes: 7, hold: time.Second, maxConns: 3}) {
		t.Fatalf("got config %+v", got)
	}

	port := listenerPort(c.inbound.listeners[0])

	scrapeAndAnnounce(c, hash)
	scrapeAndAnnounce(c, hash2)

	c.inbound.register(nodeHasPeersForHash{infoHash: hash2, source: sourceSample})

	if seen := nextAnnounce(t, announces); seen.event != 2 || seen.hash != hash2 || seen.port != port || seen.left == 0 {
		t.Fatalf("want a started announce as a leecher on the listener port %d, got %+v", port, seen)
	}

	select {
	case seen := <-announces:
		t.Fatalf("the swarm was left %+v before the hold ended", seen)
	case <-time.After(500 * time.Millisecond):
	}

	if seen := nextAnnounce(t, announces); seen.event != 3 || seen.hash != hash2 || seen.port != port {
		t.Fatalf("want a stopped announce when the hold expires, got %+v", seen)
	}

	c.inbound.config.hold = time.Minute
	c.inbound.register(nodeHasPeersForHash{infoHash: hash, source: sourceSample})

	if seen := nextAnnounce(t, announces); seen.event != 2 || seen.hash != hash || seen.port != port {
		t.Fatalf("want a started announce for the second hash, got %+v", seen)
	}

	seeder := peertest.Peer{InfoHash: hash, Metadata: metadata}
	if _, err := seeder.Dial(fmt.Sprintf("127.0.0.1:%d", port)); err != nil {
		t.Fatal(err)
	}

	if seen := nextAnnounce(t, announces); seen.event != 3 || seen.hash != hash || seen.port != port {
		t.Fatalf("want a stopped announce once the meta info arrived, got %+v", seen)
	}

	select {
	case batch := <-c.persistTorrents.Out():
		if len(batch) != 1 || batch[0].infoHash != hash || batch[0].metaInfo.Name != "held" {
			t.Fatalf("persisted %+v", batch)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing reached persistence")
	}

	for stage, want := range map[string]float64{
		stageInboundRegistered: 2,
		"announced":            2,
		"expired":              1,
		stageMetaInfoOK:        1,
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

	if busy.inbound != nil || busy.trackers.holdQueues != nil {
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
