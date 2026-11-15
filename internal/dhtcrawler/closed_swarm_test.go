package dhtcrawler

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sync/atomic"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
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
