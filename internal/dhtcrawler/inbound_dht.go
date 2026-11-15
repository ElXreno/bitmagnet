package dhtcrawler

import (
	"bytes"
	"context"
	"net"
	"sort"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

const (
	inboundDHTWorkers = 16
	inboundDHTQueue   = 256
	inboundDHTNodes   = 8
)

type inboundDHTTarget struct {
	family *nodeFamily
	port   uint16
}

func (p *inboundPeers) enableDHT(v4 net.Listener, v6 net.Listener) {
	p.dhtQueue = make(chan protocol.ID, inboundDHTQueue)
	p.dhtTargets = []inboundDHTTarget{{family: p.c.v4, port: listenerPort(v4)}}

	if v6 != nil && p.c.v6 != nil {
		p.dhtTargets = append(p.dhtTargets, inboundDHTTarget{family: p.c.v6, port: listenerPort(v6)})
	}
}

func (p *inboundPeers) queueDHT(hash protocol.ID) {
	if p.dhtQueue == nil {
		return
	}

	select {
	case p.dhtQueue <- hash:
	default:
		p.c.countHash(sourceInboundPeer, "dht_queue_full", 1)
	}
}

func (p *inboundPeers) runDHT(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case hash := <-p.dhtQueue:
			if p.held(hash) {
				p.announceDHT(ctx, hash)
			}
		}
	}
}

func (p *inboundPeers) held(hash protocol.ID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, ok := p.entries[hash]

	return ok
}

func (p *inboundPeers) announceDHT(ctx context.Context, hash protocol.ID) {
	c := p.c
	accepted := 0

	for _, target := range p.dhtTargets {
		var tokens []lookupToken

		c.lookupFamily(ctx, target.family, sourceInboundPeer, hash, nil, &tokens)

		sort.Slice(tokens, func(a, b int) bool {
			return bytes.Compare(tokens[a].distance[:], tokens[b].distance[:]) < 0
		})

		tokens = tokens[:min(len(tokens), inboundDHTNodes)]
		results := make(chan bool, len(tokens))

		for _, node := range tokens {
			go func() {
				queryCtx, cancel := context.WithTimeout(ctx, lookupQueryTimeout)
				defer cancel()

				results <- c.client.AnnouncePeer(queryCtx, node.addr, hash, target.port, node.token) == nil
			}()
		}

		for range tokens {
			if <-results {
				accepted++
			}
		}
	}

	c.countHash(sourceInboundPeer, "dht_announce_nodes", accepted)

	if accepted > 0 {
		c.countHash(sourceInboundPeer, "dht_announced", 1)
	} else {
		c.countHash(sourceInboundPeer, "dht_announce_failed", 1)
	}
}
