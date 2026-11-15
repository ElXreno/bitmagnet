package dhtcrawler

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"sort"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/server"
	"golang.org/x/time/rate"
)

const (
	inboundDHTWorkers = 16
	inboundDHTQueue   = 256
	inboundDHTNodes   = 8
	inboundDHTLookups = 64
	inboundDHTFile    = "inbound_dht_hashes.bin"
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

	if p.config.dhtMax <= 0 || p.config.dhtPPS <= 0 {
		return
	}

	p.dhtSet = newDHTHeldSet(p.config.dhtMax, p.config.dhtTTL)
	p.dhtJoins = make(chan nodeHasPeersForHash, 10*p.config.dhtPPS)
	p.dhtLimiter = rate.NewLimiter(rate.Limit(p.config.dhtPPS), p.config.dhtPPS)
	p.dhtLookups = make(chan struct{}, inboundDHTLookups)
}

func (p *inboundPeers) dhtFile() string {
	return filepath.Join(filepath.Dir(p.config.stateFile), inboundDHTFile)
}

func (p *inboundPeers) keepsTokens() bool {
	return p != nil && p.dhtJoins != nil
}

func (p *inboundPeers) queueDHT(hash protocol.ID) {
	if p.dhtQueue == nil {
		return
	}

	select {
	case p.dhtQueue <- hash:
	default:
		p.c.countHash(sourceInboundPeer, "dht_announce_queue_full", 1)
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

func (p *inboundPeers) lookupTokens(ctx context.Context, hash protocol.ID) []lookupToken {
	var tokens []lookupToken

	for _, target := range p.dhtTargets {
		p.c.lookupFamily(ctx, target.family, sourceInboundPeer, hash, nil, &tokens)
	}

	return tokens
}

// announceTo tells up to 8 token holders per family, closest first, that we are a peer; it returns how many agreed
func (p *inboundPeers) announceTo(ctx context.Context, hash protocol.ID, tokens []lookupToken) int {
	c := p.c
	accepted := 0

	sort.SliceStable(tokens, func(a, b int) bool {
		return bytes.Compare(tokens[a].distance[:], tokens[b].distance[:]) < 0
	})

	for _, target := range p.dhtTargets {
		results := make(chan bool, inboundDHTNodes)
		asked := 0

		for _, node := range tokens {
			if asked == inboundDHTNodes || c.family(node.addr.Addr()) != target.family {
				continue
			}

			asked++

			go func() {
				queryCtx, cancel := context.WithTimeout(
					server.WithMaxLimiterWait(ctx, lookupMaxLimiterWait), lookupQueryTimeout)
				defer cancel()

				results <- c.client.AnnouncePeer(queryCtx, node.addr, hash, target.port, node.token) == nil
			}()
		}

		for range asked {
			if <-results {
				accepted++
			}
		}
	}

	return accepted
}

func (p *inboundPeers) announceDHT(ctx context.Context, hash protocol.ID) {
	accepted := p.announceTo(ctx, hash, p.lookupTokens(ctx, hash))

	p.c.countHash(sourceInboundPeer, "dht_announce_nodes", accepted)

	if accepted > 0 {
		p.c.countHash(sourceInboundPeer, "dht_announced", 1)
	} else {
		p.c.countHash(sourceInboundPeer, "dht_announce_failed", 1)
	}
}

func (p *inboundPeers) queueJoin(req nodeHasPeersForHash) {
	select {
	case p.dhtJoins <- req:
		p.c.countHash(sourceInboundPeer, "dht_queued", 1)
	default:
		p.c.countHash(sourceInboundPeer, "dht_queue_full", 1)
	}
}

func (p *inboundPeers) runDHTJoins(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-p.dhtJoins:
			if p.dhtLimiter.Wait(ctx) != nil {
				return
			}

			go p.joinDHT(ctx, req)
		}
	}
}

func (p *inboundPeers) joinDHT(ctx context.Context, req nodeHasPeersForHash) {
	c := p.c
	tokens := req.tokens

	if len(tokens) > 0 {
		c.countHash(sourceInboundPeer, "dht_tokens_reused", 1)
	} else {
		select {
		case p.dhtLookups <- struct{}{}:
		default:
			c.countHash(sourceInboundPeer, "dht_join_failed", 1)

			return
		}

		tokens = p.lookupTokens(ctx, req.infoHash)
		<-p.dhtLookups

		c.countHash(sourceInboundPeer, "dht_tokens_looked_up", 1)
	}

	if p.announceTo(ctx, req.infoHash, tokens) == 0 {
		c.countHash(sourceInboundPeer, "dht_join_failed", 1)

		return
	}

	c.countHash(sourceInboundPeer, "dht_joined", 1)

	p.mu.Lock()
	_, evicted := p.dhtSet.add(newDHTHeld(req, uint32(p.now().Unix())))
	p.mu.Unlock()

	if evicted {
		c.countHash(sourceInboundPeer, "dht_evicted", 1)
	}
}

func (p *inboundPeers) saveDHT(data []byte) {
	if p.config.stateFile == "" || data == nil {
		return
	}

	file, err := os.CreateTemp(filepath.Dir(p.config.stateFile), "tmp-*")
	if err != nil {
		return
	}

	_, writeErr := file.Write(data)
	closeErr := file.Close()

	if writeErr != nil || closeErr != nil || os.Rename(file.Name(), p.dhtFile()) != nil {
		_ = os.Remove(file.Name())
	}
}
