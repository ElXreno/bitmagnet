package client

import (
	"context"
	"errors"
	"net/netip"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/server"
)

type serverAdapter struct {
	nodeID protocol.ID
	server server.Server
}

func (a serverAdapter) IPv6() bool {
	return a.server.IPv6()
}

func (a serverAdapter) want() []dht.Want {
	if !a.server.IPv6() {
		return nil
	}

	return []dht.Want{dht.WantNodes, dht.WantNodes6}
}

func (a serverAdapter) Ping(ctx context.Context, addr netip.AddrPort) (PingResult, error) {
	res, err := a.server.Query(ctx, addr, dht.QPing, dht.MsgArgs{ID: a.nodeID})
	if err != nil {
		return PingResult{}, err
	}

	return PingResult{ID: res.Msg.R.ID}, nil
}

func (a serverAdapter) FindNode(
	ctx context.Context,
	addr netip.AddrPort,
	target protocol.ID,
) (FindNodeResult, error) {
	res, err := a.server.Query(ctx, addr, dht.QFindNode, dht.MsgArgs{ID: a.nodeID, Target: target, Want: a.want()})
	if err != nil {
		return FindNodeResult{}, err
	}

	return FindNodeResult{
		ID:    res.Msg.R.ID,
		Nodes: a.extractNodes(res.Msg),
	}, nil
}

func (a serverAdapter) GetPeers(
	ctx context.Context,
	addr netip.AddrPort,
	infoHash protocol.ID,
) (GetPeersResult, error) {
	res, err := a.server.Query(
		ctx, addr, dht.QGetPeers, dht.MsgArgs{ID: a.nodeID, InfoHash: infoHash, Want: a.want()})
	if err != nil {
		return GetPeersResult{}, err
	}

	return GetPeersResult{
		ID:     res.Msg.R.ID,
		Values: extractValues(res.Msg),
		Nodes:  a.extractNodes(res.Msg),
	}, nil
}

func (a serverAdapter) GetPeersScrape(
	ctx context.Context,
	addr netip.AddrPort,
	infoHash protocol.ID,
) (GetPeersScrapeResult, error) {
	res, err := a.server.Query(
		ctx, addr, dht.QGetPeers, dht.MsgArgs{ID: a.nodeID, InfoHash: infoHash, Scrape: 1, Want: a.want()})
	if err != nil {
		return GetPeersScrapeResult{}, err
	}

	if res.Msg.R.BFpe == nil || res.Msg.R.BFsd == nil {
		return GetPeersScrapeResult{}, errors.New("missing bloom filter in scrape response")
	}

	return GetPeersScrapeResult{
		ID:        res.Msg.R.ID,
		Values:    extractValues(res.Msg),
		Nodes:     a.extractNodes(res.Msg),
		BfPeers:   *res.Msg.R.BFpe.ToBloomFilter(),
		BfSeeders: *res.Msg.R.BFsd.ToBloomFilter(),
	}, nil
}

func (a serverAdapter) SampleInfoHashes(
	ctx context.Context,
	addr netip.AddrPort,
	target protocol.ID,
) (SampleInfoHashesResult, error) {
	res, err := a.server.Query(
		ctx, addr, dht.QSampleInfohashes, dht.MsgArgs{ID: a.nodeID, Target: target, Want: a.want()})
	if err != nil {
		return SampleInfoHashesResult{}, err
	}

	var samples []protocol.ID
	if res.Msg.R.Samples != nil {
		samples = make([]protocol.ID, 0, len(*res.Msg.R.Samples))
		for _, s := range *res.Msg.R.Samples {
			samples = append(samples, s)
		}
	}

	totalNum := 0
	interval := 0

	if res.Msg.R.Num != nil {
		totalNum = int(*res.Msg.R.Num)
	}

	if res.Msg.R.Interval != nil {
		interval = int(*res.Msg.R.Interval)
	}

	return SampleInfoHashesResult{
		ID:       res.Msg.R.ID,
		Samples:  samples,
		Nodes:    a.extractNodes(res.Msg),
		Num:      totalNum,
		Interval: interval,
		Client:   res.Msg.ClientID,
	}, nil
}

func (a serverAdapter) extractNodes(msg dht.Msg) []NodeInfo {
	nodes6 := msg.R.Nodes6
	if !a.server.IPv6() {
		nodes6 = nil
	}

	if len(msg.R.Nodes) == 0 && len(nodes6) == 0 {
		return nil
	}

	nodes := make([]NodeInfo, 0, len(msg.R.Nodes)+len(nodes6))
	for _, n := range msg.R.Nodes {
		nodes = append(nodes, NodeInfo{ID: n.ID, Addr: n.Addr.ToAddrPort()})
	}

	for _, n := range nodes6 {
		addr := n.Addr.ToAddrPort()
		ip := addr.Addr()
		if dht.IsIPv6(ip) && ip.IsGlobalUnicast() && !ip.IsPrivate() && addr.Port() != 0 {
			nodes = append(nodes, NodeInfo{ID: n.ID, Addr: addr})
		}
	}

	return nodes
}

func extractValues(msg dht.Msg) []netip.AddrPort {
	if len(msg.R.Values) == 0 {
		return nil
	}

	values := make([]netip.AddrPort, 0, len(msg.R.Values))
	for _, v := range msg.R.Values {
		values = append(values, v.ToAddrPort())
	}

	return values
}
