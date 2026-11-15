package dhtcrawler

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sync/atomic"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	dto "github.com/prometheus/client_model/go"
)

type closedNetworkRequester struct {
	good  map[netip.AddrPort]bool
	calls *atomic.Int32
}

func (f closedNetworkRequester) Request(
	_ context.Context,
	_ protocol.ID,
	addr netip.AddrPort,
) (metainforequester.Response, error) {
	f.calls.Add(1)

	if f.good[addr] {
		return metainforequester.Response{Info: metainfo.Info{PieceLength: 16384}}, nil
	}

	return metainforequester.Response{}, errors.Join(metainforequester.ErrClosedNetworkPeer, errors.New("no metadata"))
}

type versionClient struct {
	client.Client
	versions map[netip.AddrPort]string
}

func (f versionClient) Ping(_ context.Context, addr netip.AddrPort) (client.PingResult, error) {
	version, ok := f.versions[addr]
	if !ok {
		return client.PingResult{}, errors.New("no reply")
	}

	return client.PingResult{ClientID: version}, nil
}

func counted(t *testing.T, c *crawler, source string, stage string) float64 {
	t.Helper()

	m := &dto.Metric{}
	if err := c.hashesTotal.WithLabelValues(source, stage).Write(m); err != nil {
		t.Fatal(err)
	}

	return m.GetCounter().GetValue()
}

func TestSwarmClassFollowsTheNodeVersions(t *testing.T) {
	t.Parallel()

	peers, _ := testPeers(3)

	for name, test := range map[string]struct {
		versions map[netip.AddrPort]string
		swarm    string
	}{
		"hint only": {map[netip.AddrPort]string{peers[0]: "LT\x01\x00", peers[1]: "LT\x01\x00"}, "swarm_hint_only"},
		"mixed":     {map[netip.AddrPort]string{peers[0]: "LT\x01\x00", peers[1]: "LT\x02\x0b"}, "swarm_hint_mixed"},
		"regular":   {map[netip.AddrPort]string{peers[0]: "UT\xb9\x1e", peers[1]: ""}, "swarm_regular"},
		"silent":    {nil, "swarm_silent"},
	} {
		c := newTestCrawler(nil, nil)
		c.client = versionClient{versions: test.versions}
		c.metainfoRequester = closedNetworkRequester{calls: new(atomic.Int32)}

		_, err := c.doRequestMetaInfo(context.Background(), sourceSample, protocol.ID{1}, peers)
		if !errors.Is(err, errClosedSwarm) {
			t.Fatalf("%s: want errClosedSwarm, got %v", name, err)
		}

		if got := counted(t, c, test.swarm, "closed_swarm"); got != 1 {
			t.Errorf("%s: want one closed swarm counted as %s, got %v", name, test.swarm, got)
		}

		if test.swarm == "swarm_hint_only" {
			if got := counted(t, c, "dial_"+implClosedHint, "closed_network_peer"); got != 2 {
				t.Errorf("%s: want two dials to hinted peers, got %v", name, got)
			}

			if !c.pingCache.closedHint(peers[0]) || c.pingCache.closedHint(peers[2]) {
				t.Errorf("%s: the ping cache must keep the hint of the nodes that carry it", name)
			}
		}
	}
}

func testPeers(n int) ([]netip.AddrPort, map[netip.AddrPort]bool) {
	peers := make([]netip.AddrPort, 0, n)
	alive := make(map[netip.AddrPort]bool, n)

	for i := range n {
		addr := netip.MustParseAddrPort(fmt.Sprintf("192.0.2.%d:6881", i+1))
		peers = append(peers, addr)
		alive[addr] = true
	}

	return peers, alive
}

func TestClosedSwarmStopsDialing(t *testing.T) {
	t.Parallel()

	peers, alive := testPeers(12)
	calls := new(atomic.Int32)
	c := newTestCrawler(alive, nil)
	c.metainfoRequester = closedNetworkRequester{calls: calls}

	_, err := c.doRequestMetaInfo(context.Background(), sourceSample, protocol.ID{1}, peers)
	if !errors.Is(err, errClosedSwarm) {
		t.Fatalf("want errClosedSwarm, got %v", err)
	}

	if got := calls.Load(); got != metaInfoParallelDials {
		t.Fatalf("want only the first %d dials, got %d", metaInfoParallelDials, got)
	}
}

func TestClosedNetworkPeerDoesNotHideARealOne(t *testing.T) {
	t.Parallel()

	peers, alive := testPeers(metaInfoParallelDials)
	c := newTestCrawler(alive, nil)
	c.metainfoRequester = closedNetworkRequester{
		good:  map[netip.AddrPort]bool{peers[len(peers)-1]: true},
		calls: new(atomic.Int32),
	}

	res, err := c.doRequestMetaInfo(context.Background(), sourceSample, protocol.ID{1}, peers)
	if err != nil {
		t.Fatalf("want the meta info of the real peer, got %v", err)
	}

	if res.Info.PieceLength != 16384 {
		t.Fatal("want the meta info of the real peer")
	}
}

func TestClosedNetworkPeerDialResult(t *testing.T) {
	t.Parallel()

	closed := errors.Join(metainforequester.ErrClosedNetworkPeer, errors.New("metadata too big or its size is less than or equal zero"))

	for name, err := range map[string]error{
		"plain":         closed,
		"after the mse": fmt.Errorf("mse retry: %w", closed),
	} {
		if got := classifyDialError(err); got != "closed_network_peer" {
			t.Errorf("%s: got %s", name, got)
		}
	}
}
