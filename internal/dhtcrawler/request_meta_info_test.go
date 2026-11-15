package dhtcrawler

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"syscall"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	"github.com/prometheus/client_golang/prometheus"
	boom "github.com/tylertreat/BoomFilters"
)

type fakeClient struct {
	client.Client
	alive map[netip.AddrPort]bool
}

func (f fakeClient) Ping(_ context.Context, addr netip.AddrPort) (client.PingResult, error) {
	if f.alive[addr] {
		return client.PingResult{}, nil
	}

	return client.PingResult{}, errors.New("no reply")
}

type fakeRequester struct {
	good map[netip.AddrPort]bool
}

func (f fakeRequester) Request(_ context.Context, _ protocol.ID, addr netip.AddrPort) (metainforequester.Response, error) {
	if f.good[addr] {
		return metainforequester.Response{Info: metainfo.Info{PieceLength: 16384}}, nil
	}

	return metainforequester.Response{}, &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}
}

type allowAll struct{}

func (allowAll) Check(metainfo.Info) error { return nil }

func newTestCrawler(alive map[netip.AddrPort]bool, good map[netip.AddrPort]bool) *crawler {
	return &crawler{
		v4:                newNodeFamily("", nil, 1, 40),
		client:            fakeClient{alive: alive},
		metainfoRequester: fakeRequester{good: good},
		banningChecker:    allowAll{},
		deadPeers:         &ignoreFilter{bloom: boom.NewStableBloomFilter(1000, 2, 0.001)},
		pingCache:         newPingCache(time.Minute),
		hashesTotal:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "hashes"}, []string{"source", "stage"}),
		probeTotal:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "probe"}, []string{"prev", "ping", "dial"}),
		dialsTotal:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "dials"}, []string{"source", "result"}),
	}
}

func TestDoRequestMetaInfoNoCandidatesIsAnError(t *testing.T) {
	t.Parallel()

	silent := netip.MustParseAddrPort("192.0.2.1:6881")
	c := newTestCrawler(nil, nil)
	c.deadPeers.add(peerKey(silent))

	for _, peers := range [][]netip.AddrPort{nil, {}, {silent}} {
		_, err := c.doRequestMetaInfo(context.Background(), sourceSample, protocol.ID{1}, peers)
		if !errors.Is(err, errNoCandidates) {
			t.Fatalf("peers %v: want errNoCandidates, got %v", peers, err)
		}
	}
}

func TestDoRequestMetaInfoAllDialsFail(t *testing.T) {
	t.Parallel()

	a := netip.MustParseAddrPort("192.0.2.1:6881")
	b := netip.MustParseAddrPort("192.0.2.2:6881")
	c := newTestCrawler(map[netip.AddrPort]bool{a: true}, nil)

	_, err := c.doRequestMetaInfo(context.Background(), sourceSample, protocol.ID{1}, []netip.AddrPort{a, b})
	if err == nil {
		t.Fatal("want an error when every dial fails")
	}

	if !c.deadPeers.test(peerKey(a)) {
		t.Fatal("a failed dial must not be forgotten")
	}
}

func TestDoRequestMetaInfoSucceeds(t *testing.T) {
	t.Parallel()

	a := netip.MustParseAddrPort("192.0.2.1:6881")
	b := netip.MustParseAddrPort("192.0.2.2:6881")
	c := newTestCrawler(map[netip.AddrPort]bool{b: true}, map[netip.AddrPort]bool{b: true})

	res, err := c.doRequestMetaInfo(context.Background(), sourceSample, protocol.ID{1}, []netip.AddrPort{a, b})
	if err != nil {
		t.Fatalf("want success, got %v", err)
	}

	if res.Info.PieceLength != 16384 {
		t.Fatal("want the meta info of the responding peer")
	}
}

func TestCreateTorrentModelRefusesEmptyMetaInfo(t *testing.T) {
	t.Parallel()

	if _, err := createTorrentModel(protocol.ID{1}, metainfo.Info{}, false, 100); err == nil {
		t.Fatal("an empty meta info must not become a torrent row")
	}
}

func TestPingCacheServesRepeatedAddresses(t *testing.T) {
	t.Parallel()

	addr := netip.MustParseAddrPort("192.0.2.1:6881")
	cache := newPingCache(time.Minute)

	if _, cached := cache.get(addr); cached {
		t.Fatal("an unseen address must not be cached")
	}

	cache.put(addr, true)

	if alive, cached := cache.get(addr); !cached || !alive {
		t.Fatal("want a cached positive answer")
	}

	off := newPingCache(0)
	off.put(addr, true)

	if _, cached := off.get(addr); cached {
		t.Fatal("a disabled cache must stay empty")
	}
}
