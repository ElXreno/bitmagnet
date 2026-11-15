package client

import (
	"context"
	"net/netip"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeServer struct {
	server.Server
	ipv6 bool
	ret  dht.Return
	args []dht.MsgArgs
}

func (f *fakeServer) IPv6() bool {
	return f.ipv6
}

func (f *fakeServer) Query(_ context.Context, addr netip.AddrPort, _ string, args dht.MsgArgs) (dht.RecvMsg, error) {
	f.args = append(f.args, args)
	ret := f.ret

	return dht.RecvMsg{Msg: dht.Msg{Y: dht.YResponse, R: &ret}, From: addr}, nil
}

func nodeInfo(id byte, addr string) dht.NodeInfo {
	return dht.NodeInfo{ID: protocol.ID{id}, Addr: dht.NewNodeAddrFromAddrPort(netip.MustParseAddrPort(addr))}
}

func newFakeServer(ipv6 bool) *fakeServer {
	samples := dht.CompactInfohashes{protocol.ID{9}}

	return &fakeServer{
		ipv6: ipv6,
		ret: dht.Return{
			ID:    protocol.ID{1},
			Nodes: dht.CompactIPv4NodeInfo{nodeInfo(2, "192.0.2.2:6881")},
			Nodes6: dht.CompactIPv6NodeInfo{
				nodeInfo(3, "[2001:db8::3]:6881"),
				nodeInfo(4, "[::ffff:192.0.2.4]:6881"),
				nodeInfo(5, "[::1]:6881"),
				nodeInfo(6, "[fe80::6]:6881"),
				nodeInfo(7, "[2001:db8::7]:0"),
			},
			Values: []dht.NodeAddr{
				dht.NewNodeAddrFromAddrPort(netip.MustParseAddrPort("192.0.2.8:6881")),
				dht.NewNodeAddrFromAddrPort(netip.MustParseAddrPort("[2001:db8::8]:6881")),
			},
			BFsd:        &dht.ScrapeBloomFilter{},
			BFpe:        &dht.ScrapeBloomFilter{},
			Bep51Return: dht.Bep51Return{Samples: &samples},
		},
	}
}

func queryAll(t *testing.T, a serverAdapter) [][]NodeInfo {
	t.Helper()

	ctx := context.Background()
	addr := netip.MustParseAddrPort("192.0.2.1:6881")

	_, err := a.Ping(ctx, addr)
	require.NoError(t, err)

	findNode, err := a.FindNode(ctx, addr, protocol.ID{1})
	require.NoError(t, err)

	getPeers, err := a.GetPeers(ctx, addr, protocol.ID{1})
	require.NoError(t, err)
	assert.Equal(t, []netip.AddrPort{
		netip.MustParseAddrPort("192.0.2.8:6881"),
		netip.MustParseAddrPort("[2001:db8::8]:6881"),
	}, getPeers.Values)

	scrape, err := a.GetPeersScrape(ctx, addr, protocol.ID{1})
	require.NoError(t, err)

	sample, err := a.SampleInfoHashes(ctx, addr, protocol.ID{1})
	require.NoError(t, err)
	assert.Equal(t, []protocol.ID{{9}}, sample.Samples)

	return [][]NodeInfo{findNode.Nodes, getPeers.Nodes, scrape.Nodes, sample.Nodes}
}

func TestWithIPv6WantIsSentAndNodes6AreUsed(t *testing.T) {
	t.Parallel()

	s := newFakeServer(true)
	a := serverAdapter{nodeID: protocol.ID{1}, server: s}

	require.True(t, a.IPv6())

	want := []NodeInfo{
		{ID: protocol.ID{2}, Addr: netip.MustParseAddrPort("192.0.2.2:6881")},
		{ID: protocol.ID{3}, Addr: netip.MustParseAddrPort("[2001:db8::3]:6881")},
	}
	for _, nodes := range queryAll(t, a) {
		assert.Equal(t, want, nodes)
	}

	require.Len(t, s.args, 5)
	assert.Nil(t, s.args[0].Want)

	for _, args := range s.args[1:] {
		assert.Equal(t, []dht.Want{dht.WantNodes, dht.WantNodes6}, args.Want)
	}
}

func TestWithoutIPv6NoWantAndNodes6AreIgnored(t *testing.T) {
	t.Parallel()

	s := newFakeServer(false)
	a := serverAdapter{nodeID: protocol.ID{1}, server: s}

	require.False(t, a.IPv6())

	want := []NodeInfo{{ID: protocol.ID{2}, Addr: netip.MustParseAddrPort("192.0.2.2:6881")}}
	for _, nodes := range queryAll(t, a) {
		assert.Equal(t, want, nodes)
	}

	require.Len(t, s.args, 5)

	for _, args := range s.args {
		assert.Nil(t, args.Want)
	}
}
