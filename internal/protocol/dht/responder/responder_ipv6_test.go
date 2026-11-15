package responder

import (
	"context"
	"net/netip"
	"testing"

	"github.com/anacrolix/torrent/bencode"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type dualStackFixture struct {
	responder responder
	node4     netip.AddrPort
	node6     netip.AddrPort
	querier4  netip.AddrPort
	querier6  netip.AddrPort
}

func newDualStackFixture(t *testing.T, ipv6 bool) dualStackFixture {
	t.Helper()

	tables := ktable.New(ktable.Params{NodeID: protocol.RandomNodeID()})
	f := dualStackFixture{
		node4:    netip.MustParseAddrPort("192.0.2.10:6881"),
		node6:    netip.MustParseAddrPort("[2001:db8::10]:6881"),
		querier4: netip.MustParseAddrPort("198.51.100.1:6881"),
		querier6: netip.MustParseAddrPort("[2001:db8:1::1]:6881"),
	}

	tables.Table.PutNode(protocol.RandomNodeID(), f.node4)
	tables.Table6.PutNode(protocol.RandomNodeID(), f.node6)

	f.responder = responder{
		nodeID:                   tables.Table.Origin(),
		kTable:                   tables.Table,
		tokenSecret:              []byte("secret"),
		sampleInfoHashesInterval: 20,
	}
	if ipv6 {
		f.responder.kTable6 = tables.Table6
	}

	return f
}

func (f dualStackFixture) respond(t *testing.T, from netip.AddrPort, q string, args dht.MsgArgs) dht.Return {
	t.Helper()

	args.ID = protocol.ID{1}
	ret, err := f.responder.Respond(context.Background(), dht.RecvMsg{From: from, Msg: dht.Msg{Q: q, A: &args}})
	require.NoError(t, err)

	_, err = bencode.Marshal(dht.Msg{Y: dht.YResponse, R: &ret})
	require.NoError(t, err)

	return ret
}

func addrs(nodes []dht.NodeInfo) []netip.AddrPort {
	result := make([]netip.AddrPort, 0, len(nodes))
	for _, n := range nodes {
		result = append(result, n.Addr.ToAddrPort())
	}

	return result
}

func values(ret dht.Return) []netip.AddrPort {
	result := make([]netip.AddrPort, 0, len(ret.Values))
	for _, v := range ret.Values {
		result = append(result, v.ToAddrPort())
	}

	return result
}

func TestResponderFindNodeFamilies(t *testing.T) {
	t.Parallel()

	f := newDualStackFixture(t, true)
	target := protocol.RandomNodeID()
	both := []dht.Want{dht.WantNodes, dht.WantNodes6}

	for _, tc := range []struct {
		name   string
		from   netip.AddrPort
		want   []dht.Want
		nodes  []netip.AddrPort
		nodes6 []netip.AddrPort
	}{
		{"v4 querier", f.querier4, nil, []netip.AddrPort{f.node4}, []netip.AddrPort{}},
		{"v6 querier", f.querier6, nil, []netip.AddrPort{}, []netip.AddrPort{f.node6}},
		{"v4 querier wants n6", f.querier4, []dht.Want{dht.WantNodes6}, []netip.AddrPort{}, []netip.AddrPort{f.node6}},
		{"v6 querier wants n4", f.querier6, []dht.Want{dht.WantNodes}, []netip.AddrPort{f.node4}, []netip.AddrPort{}},
		{"v4 querier wants both", f.querier4, both, []netip.AddrPort{f.node4}, []netip.AddrPort{f.node6}},
		{"v6 querier wants both", f.querier6, both, []netip.AddrPort{f.node4}, []netip.AddrPort{f.node6}},
		{"unknown want", f.querier6, []dht.Want{"n9"}, []netip.AddrPort{}, []netip.AddrPort{f.node6}},
	} {
		for _, q := range []string{dht.QFindNode, dht.QGetPeers} {
			ret := f.respond(t, tc.from, q, dht.MsgArgs{Target: target, InfoHash: target, Want: tc.want})
			assert.Equal(t, tc.nodes, addrs(ret.Nodes), "%s %s nodes", tc.name, q)
			assert.Equal(t, tc.nodes6, addrs(ret.Nodes6), "%s %s nodes6", tc.name, q)
		}
	}
}

func TestResponderAnnounceAndValuesStayInTheirFamily(t *testing.T) {
	t.Parallel()

	f := newDualStackFixture(t, true)
	hash := protocol.RandomNodeID()

	for _, querier := range []netip.AddrPort{f.querier4, f.querier6} {
		token := f.respond(t, querier, dht.QGetPeers, dht.MsgArgs{InfoHash: hash}).Token
		require.NotNil(t, token)

		f.respond(t, querier, dht.QAnnouncePeer, dht.MsgArgs{InfoHash: hash, Token: *token, ImpliedPort: true})
	}

	ret4 := f.respond(t, f.querier4, dht.QGetPeers, dht.MsgArgs{InfoHash: hash})
	assert.Equal(t, []netip.AddrPort{f.querier4}, values(ret4))
	assert.Len(t, ret4.Values[0].IP, 4)

	ret6 := f.respond(t, f.querier6, dht.QGetPeers, dht.MsgArgs{InfoHash: hash})
	assert.Equal(t, []netip.AddrPort{f.querier6}, values(ret6))
	assert.Len(t, ret6.Values[0].IP, 16)
}

func TestResponderSampleInfohashesFamilies(t *testing.T) {
	t.Parallel()

	f := newDualStackFixture(t, true)
	hash4, hash6 := protocol.RandomNodeID(), protocol.RandomNodeID()

	f.responder.kTable.PutHash(hash4, []ktable.HashPeer{{Addr: f.querier4}})
	f.responder.kTable6.PutHash(hash6, []ktable.HashPeer{{Addr: f.querier6}})

	ret4 := f.respond(t, f.querier4, dht.QSampleInfohashes, dht.MsgArgs{Target: hash4})
	assert.Equal(t, dht.CompactInfohashes{hash4}, *ret4.Samples)
	assert.Equal(t, []netip.AddrPort{f.node4}, addrs(ret4.Nodes))
	assert.Empty(t, ret4.Nodes6)

	ret6 := f.respond(t, f.querier6, dht.QSampleInfohashes, dht.MsgArgs{Target: hash6})
	assert.Equal(t, dht.CompactInfohashes{hash6}, *ret6.Samples)
	assert.Equal(t, []netip.AddrPort{f.node6}, addrs(ret6.Nodes6))
	assert.Empty(t, ret6.Nodes)
}

func TestResponderWithoutIPv6IgnoresWant(t *testing.T) {
	t.Parallel()

	f := newDualStackFixture(t, false)
	target := protocol.RandomNodeID()

	for _, q := range []string{dht.QFindNode, dht.QGetPeers} {
		ret := f.respond(t, f.querier4, q, dht.MsgArgs{
			Target: target, InfoHash: target, Want: []dht.Want{dht.WantNodes6},
		})
		assert.Equal(t, []netip.AddrPort{f.node4}, addrs(ret.Nodes), q)
		assert.Empty(t, ret.Nodes6, q)
	}
}

func TestLimiterCountsAnIPv6Prefix(t *testing.T) {
	t.Parallel()

	l := NewLimiter(1000, 1000, 1, 2, 100, 0)

	assert.True(t, l.Allow(netip.MustParseAddr("2001:db8:0:1::1")))
	assert.True(t, l.Allow(netip.MustParseAddr("2001:db8:0:1::2")))
	assert.False(t, l.Allow(netip.MustParseAddr("2001:db8:0:1::3")))
	assert.True(t, l.Allow(netip.MustParseAddr("2001:db8:0:2::1")))
	assert.True(t, l.Allow(netip.MustParseAddr("192.0.2.1")))
	assert.True(t, l.Allow(netip.MustParseAddr("192.0.2.2")))
}

func TestResponderMetricsCarryTheFamily(t *testing.T) {
	t.Parallel()

	f := newDualStackFixture(t, true)
	collector := newPrometheusCollector(f.responder)

	for _, from := range []netip.AddrPort{f.querier4, f.querier6, f.querier6} {
		_, err := collector.Respond(context.Background(), dht.RecvMsg{
			From: from,
			Msg:  dht.Msg{Q: dht.QPing, A: &dht.MsgArgs{ID: protocol.ID{1}}},
		})
		require.NoError(t, err)
	}

	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(collector.querySuccessTotal))

	families, err := registry.Gather()
	require.NoError(t, err)

	counts := make(map[string]float64)

	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == labelFamily {
					counts[label.GetValue()] = metric.GetCounter().GetValue()
				}
			}
		}
	}

	assert.Equal(t, map[string]float64{dht.FamilyIPv4: 1, dht.FamilyIPv6: 2}, counts)
}
