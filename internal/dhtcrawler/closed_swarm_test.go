package dhtcrawler

import (
	"context"
	"crypto/sha1" //nolint:gosec
	"crypto/sha256"
	"errors"
	"fmt"
	"net/netip"
	"sync/atomic"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	boom "github.com/tylertreat/BoomFilters"
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

func counted(t *testing.T, vec *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()

	m := &dto.Metric{}
	if err := vec.WithLabelValues(labels...).Write(m); err != nil {
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
		"hint only": {map[netip.AddrPort]string{peers[0]: "LT\x01\x00", peers[1]: "LT\x01\x00"}, "hint_only"},
		"mixed":     {map[netip.AddrPort]string{peers[0]: "LT\x01\x00", peers[1]: "LT\x02\x0b"}, "hint_mixed"},
		"regular":   {map[netip.AddrPort]string{peers[0]: "UT\xb9\x1e", peers[1]: ""}, "regular"},
		"silent":    {nil, "silent"},
	} {
		c := newTestCrawler(nil, nil)
		c.client = versionClient{versions: test.versions}

		calls := new(atomic.Int32)
		c.metainfoRequester = closedNetworkRequester{calls: calls}

		_, err := c.doRequestMetaInfo(context.Background(), sourceSample, protocol.ID{1}, peers)

		if test.swarm == "hint_only" {
			if !errors.Is(err, errClosedSwarmHint) || calls.Load() != 0 {
				t.Fatalf("%s: want errClosedSwarmHint and no dials, got %v after %d dials", name, err, calls.Load())
			}

			if got := counted(t, c.swarmTotal, test.swarm, "skipped_hint"); got != 1 {
				t.Errorf("%s: want one skipped swarm, got %v", name, got)
			}

			if !c.pingCache.closedHint(peers[0]) || c.pingCache.closedHint(peers[2]) {
				t.Errorf("%s: the ping cache must keep the hint of the nodes that carry it", name)
			}

			continue
		}

		if !errors.Is(err, errClosedSwarm) {
			t.Fatalf("%s: want errClosedSwarm, got %v", name, err)
		}

		if got := counted(t, c.swarmTotal, test.swarm, "closed_swarm"); got != 1 {
			t.Errorf("%s: want one closed swarm counted as %s, got %v", name, test.swarm, got)
		}

		if test.swarm == "hint_mixed" {
			if got := counted(t, c.peerDialTotal, implClosedHint, "closed_network_peer"); got != 1 {
				t.Errorf("%s: want one dial to the hinted peer, got %v", name, got)
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

func TestPersistAsFollowsTheIdentity(t *testing.T) {
	t.Parallel()

	alias, canonical := protocol.ID{1}, protocol.ID{2}

	c := newTestCrawler(nil, nil)
	c.fetched = &ignoreFilter{bloom: boom.NewStableBloomFilter(1000, 2, 0.001)}

	req := nodeHasPeersForHash{infoHash: alias, source: sourceSample}
	if !c.persistAs(&req, metainfo.Identity{InfoHash: alias, Kind: metainfo.KindV1}) || req.infoHash != alias {
		t.Fatal("a v1 torrent is stored under the hash it was fetched for")
	}

	req = nodeHasPeersForHash{infoHash: alias, source: sourceSample}
	if !c.persistAs(&req, metainfo.Identity{InfoHash: alias, Kind: metainfo.KindV2}) || req.infoHash != alias {
		t.Fatal("a v2 only torrent is stored under its v2 hash")
	}

	req = nodeHasPeersForHash{infoHash: alias, source: sourceSample}
	if !c.persistAs(&req, metainfo.Identity{InfoHash: canonical, Kind: metainfo.KindHybridV2}) || req.infoHash != canonical {
		t.Fatal("a hybrid torrent fetched under its v2 hash is stored under its v1 hash")
	}

	if !c.fetched.test(canonical) {
		t.Fatal("the v1 hash of a hybrid torrent counts as fetched")
	}

	req = nodeHasPeersForHash{infoHash: alias, source: sourceSample}
	if c.persistAs(&req, metainfo.Identity{InfoHash: canonical, Kind: metainfo.KindHybridV2}) {
		t.Fatal("a hybrid torrent whose v1 hash was just fetched is not stored twice")
	}
}

func TestCreateTorrentModelReadsTheV2FileTree(t *testing.T) {
	t.Parallel()

	tree := "9:file treed1:ad0:d6:lengthi10e11:pieces root32:rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrree" +
		"3:dird1:bd0:d6:lengthi20e11:pieces root32:sssssssssssssssssssssssssssssssseeee"
	multi := "d" + tree + "12:meta versioni2e4:name4:test12:piece lengthi16384ee"
	single := "d9:file treed4:testd0:d6:lengthi10e11:pieces root32:rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrreee" +
		"12:meta versioni2e4:name4:test12:piece lengthi16384ee"

	v2Hash := func(info string) protocol.ID {
		sum := sha256.Sum256([]byte(info))

		return protocol.ID(sum[:20])
	}

	info, _, err := metainfo.ParseMetaInfo(v2Hash(multi), []byte(multi))
	if err != nil {
		t.Fatal(err)
	}

	torrent, err := createTorrentModel(v2Hash(multi), info, false, 100)
	if err != nil {
		t.Fatal(err)
	}

	if torrent.Size != 30 || torrent.FilesStatus != model.FilesStatusMulti || len(torrent.Files) != 2 ||
		torrent.Files[0].Path != "a" || torrent.Files[0].Size != 10 ||
		torrent.Files[1].Path != "dir/b" || torrent.Files[1].Size != 20 {
		t.Fatalf("multi file v2 torrent: %+v", torrent)
	}

	info, _, err = metainfo.ParseMetaInfo(v2Hash(single), []byte(single))
	if err != nil {
		t.Fatal(err)
	}

	torrent, err = createTorrentModel(v2Hash(single), info, false, 100)
	if err != nil {
		t.Fatal(err)
	}

	if torrent.Size != 10 || torrent.FilesStatus != model.FilesStatusSingle || len(torrent.Files) != 0 {
		t.Fatalf("single file v2 torrent: %+v", torrent)
	}
}

func TestCreateTorrentModelLeavesPaddingFilesOut(t *testing.T) {
	t.Parallel()

	encoded := "d5:filesld6:lengthi10e4:pathl1:aeed4:attr1:p6:lengthi6e4:pathl4:.pad1:6eed6:lengthi20e4:pathl1:beee" +
		"4:name4:test12:piece lengthi16e6:pieces20:aaaaaaaaaaaaaaaaaaaae"

	info, _, err := metainfo.ParseMetaInfo(sha1.Sum([]byte(encoded)), []byte(encoded)) //nolint:gosec
	if err != nil {
		t.Fatal(err)
	}

	torrent, err := createTorrentModel(protocol.ID{1}, info, false, 100)
	if err != nil {
		t.Fatal(err)
	}

	if torrent.Size != 30 || torrent.FilesCount.Uint != 2 || len(torrent.Files) != 2 ||
		torrent.Files[0].Path != "a" || torrent.Files[0].Index != 0 ||
		torrent.Files[1].Path != "b" || torrent.Files[1].Index != 2 {
		t.Fatalf("padding file kept: %+v", torrent)
	}
}
