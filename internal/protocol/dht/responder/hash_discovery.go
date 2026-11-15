package responder

import (
	"context"
	"net/netip"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
)

type InboundHash struct {
	InfoHash protocol.ID
	Node     netip.AddrPort
	Peer     netip.AddrPort
	Query    string
}

type responderHashDiscovery struct {
	responder     Responder
	inboundHashes chan<- InboundHash
}

func (r responderHashDiscovery) Respond(ctx context.Context, msg dht.RecvMsg) (dht.Return, error) {
	if args := msg.Msg.A; args != nil && args.InfoHash != [20]byte{} {
		h := InboundHash{InfoHash: args.InfoHash, Node: msg.From, Query: msg.Msg.Q}

		switch msg.Msg.Q {
		case dht.QGetPeers:
			h.Peer = msg.From
		case dht.QAnnouncePeer:
			h.Peer = netip.AddrPortFrom(msg.From.Addr(), msg.AnnouncePort())
		}

		if h.Peer.IsValid() && h.Peer.Port() != 0 {
			select {
			case r.inboundHashes <- h:
			default:
			}
		}
	}

	return r.responder.Respond(ctx, msg)
}
