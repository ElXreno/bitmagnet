package dhtcrawler

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
	"github.com/prometheus/client_golang/prometheus"
	boom "github.com/tylertreat/BoomFilters"
	"go.uber.org/zap"
)

var (
	node4a = client.NodeInfo{ID: protocol.ID{0x10}, Addr: netip.MustParseAddrPort("192.0.2.1:6881")}
	node4b = client.NodeInfo{ID: protocol.ID{0x20}, Addr: netip.MustParseAddrPort("192.0.2.2:6881")}
	node6a = client.NodeInfo{ID: protocol.ID{0x30}, Addr: netip.MustParseAddrPort("[2001:db8::1]:6881")}
	node6b = client.NodeInfo{ID: protocol.ID{0x40}, Addr: netip.MustParseAddrPort("[2001:db8::2]:6881")}
	peer4  = netip.MustParseAddrPort("198.51.100.4:51413")
	peer6  = netip.MustParseAddrPort("[2001:db8:ffff::6]:51413")
)

type familyClient struct {
	client.Client
	mu      sync.Mutex
	queried []netip.AddrPort
	answers map[netip.AddrPort]client.GetPeersResult
	samples []protocol.ID
}

func (f *familyClient) GetPeers(_ context.Context, addr netip.AddrPort, _ protocol.ID) (client.GetPeersResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.queried = append(f.queried, addr)

	if answer, ok := f.answers[addr]; ok {
		return answer, nil
	}

	return client.GetPeersResult{}, errors.New("no reply")
}

func (f *familyClient) SampleInfoHashes(
	context.Context, netip.AddrPort, protocol.ID,
) (client.SampleInfoHashesResult, error) {
	return client.SampleInfoHashesResult{Samples: f.samples, Interval: 60}, nil
}

func (f *familyClient) queriedAddrs() []netip.AddrPort {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]netip.AddrPort(nil), f.queried...)
}

func newLookupClient() *familyClient {
	return &familyClient{answers: map[netip.AddrPort]client.GetPeersResult{
		node4a.Addr: {ID: node4a.ID, Values: []netip.AddrPort{peer4}, Nodes: []client.NodeInfo{node4b, node6b}},
		node4b.Addr: {ID: node4b.ID},
		node6a.Addr: {ID: node6a.ID, Values: []netip.AddrPort{peer6}, Nodes: []client.NodeInfo{node6b, node4b}},
		node6b.Addr: {ID: node6b.ID},
	}}
}

func newFamilyTestCrawler(ipv6 bool, cl client.Client) *crawler {
	tables := ktable.New(ktable.Params{NodeID: protocol.RandomNodeID()})
	c := newTestCrawler(nil, nil)
	c.client = cl
	c.v4 = newNodeFamily("", tables.Table, 1, 40)

	if ipv6 {
		c.v6 = newNodeFamily("6", tables.Table6, 1, 40)
		c.v6.discovered = make(chan ktable.Node, 100)
	}

	c.discoveredNodes = concurrency.NewBatchingChannel[ktable.Node](100, 1, 10*time.Millisecond)
	c.infoHashTriage = concurrency.NewBatchingChannel[nodeHasPeersForHash](100, 100, 10*time.Millisecond)
	c.requestMetaInfo = concurrency.NewBufferedConcurrentChannel[infoHashWithPeers](10, 1)
	c.lookupQueries = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "lookups"}, []string{"source", "result"})
	c.ignoreHashes = &ignoreFilter{bloom: boom.NewStableBloomFilter(1000, 2, 0.001)}
	c.soughtNodeID = &concurrency.AtomicValue[protocol.ID]{}
	c.logger = zap.NewNop().Sugar()

	return c
}

func hashCount(t *testing.T, c *crawler, source string, stage string) float64 {
	t.Helper()

	registry := prometheus.NewRegistry()
	registry.MustRegister(c.hashesTotal)

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

			if labels["source"] == source && labels["stage"] == stage {
				return metric.GetCounter().GetValue()
			}
		}
	}

	return 0
}

func sameAddrs(got []netip.AddrPort, want ...netip.AddrPort) bool {
	if len(got) != len(want) {
		return false
	}

	seen := make(map[netip.AddrPort]bool, len(got))
	for _, addr := range got {
		seen[addr] = true
	}

	for _, addr := range want {
		if !seen[addr] {
			return false
		}
	}

	return true
}

func TestFamilySelection(t *testing.T) {
	t.Parallel()

	v4 := netip.MustParseAddr("192.0.2.1")
	mapped := netip.MustParseAddr("::ffff:192.0.2.1")
	v6 := netip.MustParseAddr("2001:db8::1")

	off := newFamilyTestCrawler(false, nil)
	if len(off.families()) != 1 || off.families()[0] != off.v4 {
		t.Fatal("without IPv6 only the IPv4 family runs")
	}

	for _, addr := range []netip.Addr{v4, mapped, v6, {}} {
		if off.family(addr) != off.v4 {
			t.Fatalf("without IPv6 %s must stay in the IPv4 family", addr)
		}
	}

	on := newFamilyTestCrawler(true, nil)
	if len(on.families()) != 2 {
		t.Fatal("with IPv6 both families run")
	}

	for _, addr := range []netip.Addr{v4, mapped, {}} {
		if on.family(addr) != on.v4 {
			t.Fatalf("%s belongs to the IPv4 family", addr)
		}
	}

	if on.family(v6) != on.v6 {
		t.Fatal("an IPv6 address belongs to the IPv6 family")
	}
}

func TestSampleSources(t *testing.T) {
	t.Parallel()

	c := newFamilyTestCrawler(true, nil)
	if c.v4.sourceSample() != sourceSample || c.v6.sourceSample() != sourceSample6 {
		t.Fatalf("sample sources: %q %q", c.v4.sourceSample(), c.v6.sourceSample())
	}

	if sourceSample6 != "sample_infohashes6" || sourceSampleLookup6 != "sample_infohashes6_lookup" {
		t.Fatal("the IPv6 source names are part of the metrics")
	}

	for _, source := range []string{sourceSample, sourceSample6} {
		if !isSampleSource(source) || isSampleLookupSource(source) {
			t.Fatalf("%q is a sample source", source)
		}
	}

	for _, source := range []string{sourceSampleLookup, sourceSampleLookup6} {
		if isSampleSource(source) || !isSampleLookupSource(source) {
			t.Fatalf("%q is a sample lookup source", source)
		}
	}

	for _, source := range []string{dht.QGetPeers, dht.QGetPeers + "6", "tracker_1", dht.QAnnouncePeer} {
		if isSampleSource(source) || isSampleLookupSource(source) {
			t.Fatalf("%q is not a sample source", source)
		}
	}

	if sampleLookupSource(sourceSample) != sourceSampleLookup ||
		sampleLookupSource(sourceSample6) != sourceSampleLookup6 {
		t.Fatal("a lookup keeps the family of its sample source")
	}

	if !isAnnounceSource(dht.QAnnouncePeer) || !isAnnounceSource(dht.QAnnouncePeer+"6") ||
		isAnnounceSource(dht.QGetPeers) {
		t.Fatal("announce sources")
	}
}

func TestLookupStaysInsideOneFamily(t *testing.T) {
	t.Parallel()

	seeds := []client.NodeInfo{node4a, node6a}
	ctx := context.Background()
	hash := protocol.ID{1}

	cl := newLookupClient()
	c := newFamilyTestCrawler(true, cl)

	if got := c.lookupFamilyPeers(ctx, c.v4, sourceSampleLookup, hash, seeds); !sameAddrs(got, peer4) {
		t.Fatalf("IPv4 lookup: %v", got)
	}

	if queried := cl.queriedAddrs(); !sameAddrs(queried, node4a.Addr, node4b.Addr) {
		t.Fatalf("the IPv4 lookup asked %v", queried)
	}

	cl = newLookupClient()
	c = newFamilyTestCrawler(true, cl)

	if got := c.lookupFamilyPeers(ctx, c.v6, sourceSampleLookup, hash, seeds); !sameAddrs(got, peer6) {
		t.Fatalf("IPv6 lookup: %v", got)
	}

	if queried := cl.queriedAddrs(); !sameAddrs(queried, node6a.Addr, node6b.Addr) {
		t.Fatalf("the IPv6 lookup asked %v", queried)
	}

	c.v6.nodePool.rebuild()

	if c.v6.nodePool.size() != 2 || c.v4.nodePool.size() != 0 {
		t.Fatal("answering IPv6 nodes belong to the IPv6 pool only")
	}
}

func TestLookupPeersJoinsBothFamilies(t *testing.T) {
	t.Parallel()

	cl := newLookupClient()
	c := newFamilyTestCrawler(true, cl)
	source := sourceSampleLookup

	got := c.lookupPeers(context.Background(), source, protocol.ID{1}, []client.NodeInfo{node4a, node6a})
	if !sameAddrs(got, peer4, peer6) {
		t.Fatalf("want the peers of both families, got %v", got)
	}

	if got[0] != peer4 {
		t.Fatal("IPv4 peers come first")
	}

	for stage, want := range map[string]float64{
		"lookup_found6":      1,
		"peers_sum6":         1,
		"lookup_found6_only": 0,
		"lookup_queries":     2,
		"lookup_queries6":    2,
	} {
		if count := hashCount(t, c, source, stage); count != want {
			t.Fatalf("%s: want %v, got %v", stage, want, count)
		}
	}

	cl = newLookupClient()
	delete(cl.answers, node4a.Addr)
	c = newFamilyTestCrawler(true, cl)

	got = c.lookupPeers(context.Background(), source, protocol.ID{1}, []client.NodeInfo{node4a, node6a})
	if !sameAddrs(got, peer6) {
		t.Fatalf("want the IPv6 peer alone, got %v", got)
	}

	if hashCount(t, c, source, "lookup_found6_only") != 1 {
		t.Fatal("a hash found over IPv6 alone must be counted")
	}
}

func TestLookupWithoutIPv6IsOneLookup(t *testing.T) {
	t.Parallel()

	cl := newLookupClient()
	c := newFamilyTestCrawler(false, cl)
	source := sourceSampleLookup

	got := c.lookupPeers(context.Background(), source, protocol.ID{1}, []client.NodeInfo{node4a, node6a})
	if !sameAddrs(got, peer4, peer6) {
		t.Fatalf("got %v", got)
	}

	if queried := cl.queriedAddrs(); len(queried) != 4 {
		t.Fatalf("one lookup over every candidate, asked %v", queried)
	}

	for _, stage := range []string{"lookup_found6", "peers_sum6", "lookup_found6_only", "lookup_queries6"} {
		if count := hashCount(t, c, source, stage); count != 0 {
			t.Fatalf("%s must not be counted without IPv6, got %v", stage, count)
		}
	}

	if hashCount(t, c, source, "lookup_queries") != 4 {
		t.Fatal("lookup_queries")
	}
}

func hashPeers(table ktable.Table, hash protocol.ID) []netip.AddrPort {
	result := table.GetHashOrClosestNodes(hash)
	if !result.Found {
		return nil
	}

	peers := make([]netip.AddrPort, 0, len(result.Hash.Peers()))
	for _, p := range result.Hash.Peers() {
		peers = append(peers, p.Addr)
	}

	return peers
}

func TestFetchedHashesKeepFamiliesApart(t *testing.T) {
	t.Parallel()

	hash := protocol.ID{7}
	peers := []netip.AddrPort{peer4, peer6}

	on := newFamilyTestCrawler(true, nil)
	on.forwardPeers(context.Background(), nodeHasPeersForHash{infoHash: hash, source: sourceSample}, peers)

	if hashPeers(on.v4.kTable, hash) != nil || hashPeers(on.v6.kTable, hash) != nil {
		t.Fatal("a hash whose peers were only found must not be offered to other nodes")
	}

	on.putHash(hash, peers)

	if got := hashPeers(on.v4.kTable, hash); !sameAddrs(got, peer4) {
		t.Fatalf("IPv4 table: %v", got)
	}

	if got := hashPeers(on.v6.kTable, hash); !sameAddrs(got, peer6) {
		t.Fatalf("IPv6 table: %v", got)
	}

	on.putHash(protocol.ID{8}, peers[:1])

	if got := hashPeers(on.v6.kTable, protocol.ID{8}); got != nil {
		t.Fatalf("a hash without IPv6 peers must stay out of the IPv6 table, got %v", got)
	}

	off := newFamilyTestCrawler(false, nil)
	off.putHash(hash, peers)

	if got := hashPeers(off.v4.kTable, hash); !sameAddrs(got, peer4, peer6) {
		t.Fatalf("without IPv6 every peer stays in the one table, got %v", got)
	}
}

func queued(f *nodeFamily) int {
	return f.nodesForPing.Len() + f.nodesForFindNode.Len() + f.nodesForSampleInfoHashes.Len()
}

func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func TestDiscoveredNodesGoToTheirFamily(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	on := newFamilyTestCrawler(true, nil)

	go on.runDiscoveredNodes(ctx)
	go on.runDiscoveredNodes6(ctx)

	on.discoveredNodes.In() <- ktable.NewNode(node4a.ID, node4a.Addr)
	on.discoveredNodes.In() <- ktable.NewNode(node6a.ID, node6a.Addr)

	waitFor(t, "one node per family", func() bool { return queued(on.v4) == 1 && queued(on.v6) == 1 })

	off := newFamilyTestCrawler(false, nil)

	go off.runDiscoveredNodes(ctx)

	off.discoveredNodes.In() <- ktable.NewNode(node4a.ID, node4a.Addr)
	off.discoveredNodes.In() <- ktable.NewNode(node6a.ID, node6a.Addr)

	waitFor(t, "both nodes in the one family", func() bool { return queued(off.v4) == 2 })
}

func TestIPv6SamplesGetTheirOwnSource(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := newFamilyTestCrawler(true, &familyClient{samples: []protocol.ID{{1}, {2}}})

	go c.runSampleInfoHashes(ctx, c.v6)

	c.v6.nodesForSampleInfoHashes.In() <- ktable.NewNode(node6a.ID, node6a.Addr)

	select {
	case batch := <-c.infoHashTriage.Out():
		if len(batch) != 2 {
			t.Fatalf("want 2 hashes, got %d", len(batch))
		}

		for _, h := range batch {
			if h.source != sourceSample6 || h.node != node6a.Addr {
				t.Fatalf("unexpected hash %+v", h)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no sampled hashes")
	}

	if hashCount(t, c, sourceSample6, stageDiscovered) != 2 || hashCount(t, c, sourceSample, stageDiscovered) != 0 {
		t.Fatal("IPv6 samples must be counted under their own source")
	}

	waitFor(t, "the sampled node in the IPv6 table", func() bool {
		return len(c.v6.kTable.GetClosestNodes(node6a.ID)) == 1
	})

	if len(c.v4.kTable.GetClosestNodes(node6a.ID)) != 0 {
		t.Fatal("an IPv6 node must stay out of the IPv4 table")
	}
}

func TestBootstrapAddrs6(t *testing.T) {
	t.Parallel()

	on := newFamilyTestCrawler(true, nil)

	if got := on.bootstrapAddrs6(context.Background(), "[2001:db8::1]:6881"); !sameAddrs(got, node6a.Addr) {
		t.Fatalf("an IPv6 literal: %v", got)
	}

	for _, addr := range []string{"192.0.2.1:6881", "[::ffff:192.0.2.1]:6881", "no-port"} {
		if got := on.bootstrapAddrs6(context.Background(), addr); len(got) != 0 {
			t.Fatalf("%s has no IPv6 address, got %v", addr, got)
		}
	}

	off := newFamilyTestCrawler(false, nil)

	if got := off.bootstrapAddrs6(context.Background(), "[2001:db8::1]:6881"); got != nil {
		t.Fatalf("without IPv6 nothing is resolved, got %v", got)
	}
}
