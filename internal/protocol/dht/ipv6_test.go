package dht

import (
	"net"
	"net/netip"
	"testing"

	"github.com/anacrolix/torrent/bencode"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompactIPv6NodeInfoRoundTrip(t *testing.T) {
	t.Parallel()

	nodes := CompactIPv6NodeInfo{
		{
			ID:   protocol.NewIDFromRawString("aaaaaaaaaaaaaaaaaaaa"),
			Addr: NewNodeAddrFromAddrPort(netip.MustParseAddrPort("[2001:db8::1]:6881")),
		},
		{
			ID:   protocol.NewIDFromRawString("bbbbbbbbbbbbbbbbbbbb"),
			Addr: NewNodeAddrFromAddrPort(netip.MustParseAddrPort("[2001:db8:ffff::2]:51413")),
		},
	}

	raw, err := nodes.MarshalBinary()
	require.NoError(t, err)
	require.Len(t, raw, 2*38)
	assert.Equal(t, "aaaaaaaaaaaaaaaaaaaa", string(raw[:20]))
	assert.Equal(t, net.ParseIP("2001:db8::1").To16(), net.IP(raw[20:36]))
	assert.Equal(t, []byte{0x1a, 0xe1}, raw[36:38])

	var decoded CompactIPv6NodeInfo

	require.NoError(t, decoded.UnmarshalBinary(raw))
	require.Len(t, decoded, 2)

	for i, n := range decoded {
		assert.Equal(t, nodes[i].ID, n.ID)
		assert.Equal(t, nodes[i].Addr.ToAddrPort(), n.Addr.ToAddrPort())
		assert.True(t, IsIPv6(n.Addr.ToAddrPort().Addr()))
	}

	require.Error(t, decoded.UnmarshalBinary(raw[:37]))
}

func TestMsgNodes6AndIPv6ValuesRoundTrip(t *testing.T) {
	t.Parallel()

	peer4 := netip.MustParseAddrPort("192.0.2.7:6881")
	peer6 := netip.MustParseAddrPort("[2001:db8::7]:6882")
	node4 := netip.MustParseAddrPort("192.0.2.9:6883")
	node6 := netip.MustParseAddrPort("[2001:db8::9]:6884")
	id := protocol.NewIDFromRawString("cccccccccccccccccccc")

	msg := Msg{
		T: "t",
		Y: YResponse,
		R: &Return{
			ID:     id,
			Nodes:  CompactIPv4NodeInfo{{ID: id, Addr: NewNodeAddrFromAddrPort(node4)}},
			Nodes6: CompactIPv6NodeInfo{{ID: id, Addr: NewNodeAddrFromAddrPort(node6)}},
			Values: []NodeAddr{NewNodeAddrFromAddrPort(peer4), NewNodeAddrFromAddrPort(peer6)},
		},
	}

	raw, err := bencode.Marshal(msg)
	require.NoError(t, err)
	assert.Contains(t, string(raw), "5:nodes26:")
	assert.Contains(t, string(raw), "6:nodes638:")
	assert.Contains(t, string(raw), "6:valuesl6:")
	assert.Contains(t, string(raw), "18:")

	var decoded Msg

	require.NoError(t, bencode.Unmarshal(raw, &decoded))
	require.Len(t, decoded.R.Nodes, 1)
	require.Len(t, decoded.R.Nodes6, 1)
	require.Len(t, decoded.R.Values, 2)
	assert.Equal(t, node4, decoded.R.Nodes[0].Addr.ToAddrPort())
	assert.Equal(t, node6, decoded.R.Nodes6[0].Addr.ToAddrPort())
	assert.Equal(t, peer4, decoded.R.Values[0].ToAddrPort())
	assert.Equal(t, peer6, decoded.R.Values[1].ToAddrPort())
}

func TestWantRoundTrip(t *testing.T) {
	t.Parallel()

	id := protocol.NewIDFromRawString("dddddddddddddddddddd")

	with, err := bencode.Marshal(Msg{
		Q: QFindNode, T: "t", Y: YQuery,
		A: &MsgArgs{ID: id, Target: id, Want: []Want{WantNodes, WantNodes6}},
	})
	require.NoError(t, err)
	assert.Contains(t, string(with), "4:wantl2:n42:n6e")

	var decoded Msg

	require.NoError(t, bencode.Unmarshal(with, &decoded))
	assert.Equal(t, []Want{WantNodes, WantNodes6}, decoded.A.Want)

	without, err := bencode.Marshal(Msg{Q: QFindNode, T: "t", Y: YQuery, A: &MsgArgs{ID: id, Target: id}})
	require.NoError(t, err)
	assert.NotContains(t, string(without), "want")
}

//nolint:paralleltest
func TestIPv6Enabled(t *testing.T) {
	for value, want := range map[string]bool{"": true, "on": true, "off": false} {
		t.Setenv(IPv6Env, value)
		assert.Equal(t, want, IPv6Enabled(), "value %q", value)
	}
}

func TestFamily(t *testing.T) {
	t.Parallel()

	assert.Equal(t, FamilyIPv4, Family(netip.MustParseAddr("192.0.2.1")))
	assert.Equal(t, FamilyIPv4, Family(netip.MustParseAddr("::ffff:192.0.2.1")))
	assert.Equal(t, FamilyIPv6, Family(netip.MustParseAddr("2001:db8::1")))
	assert.Equal(t, FamilyIPv4, Family(netip.Addr{}))
}
