package ktable

import (
	"net/netip"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable/btree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEachTableKeepsOneFamily(t *testing.T) {
	t.Parallel()

	res := New(Params{NodeID: protocol.RandomNodeID()})
	v4 := netip.MustParseAddrPort("192.0.2.1:6881")
	mapped := netip.MustParseAddrPort("[::ffff:192.0.2.2]:6881")
	v6 := netip.MustParseAddrPort("[2001:db8::1]:6881")

	assert.Equal(t, res.Table.Origin(), res.Table6.Origin())

	assert.Equal(t, btree.PutAccepted, res.Table.PutNode(protocol.RandomNodeID(), v4))
	assert.Equal(t, btree.PutAccepted, res.Table.PutNode(protocol.RandomNodeID(), mapped))
	assert.Equal(t, btree.PutRejected, res.Table.PutNode(protocol.RandomNodeID(), v6))
	assert.Equal(t, btree.PutRejected, res.Table.PutNode(protocol.RandomNodeID(), netip.AddrPort{}))

	assert.Equal(t, btree.PutAccepted, res.Table6.PutNode(protocol.RandomNodeID(), v6))
	assert.Equal(t, btree.PutRejected, res.Table6.PutNode(protocol.RandomNodeID(), v4))
	assert.Equal(t, btree.PutRejected, res.Table6.PutNode(protocol.RandomNodeID(), mapped))

	nodes4 := res.Table.GetClosestNodes(protocol.RandomNodeID())
	require.Len(t, nodes4, 2)

	nodes6 := res.Table6.GetClosestNodes(protocol.RandomNodeID())
	require.Len(t, nodes6, 1)
	assert.Equal(t, v6, nodes6[0].Addr())
}
