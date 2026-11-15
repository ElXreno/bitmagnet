package dhtcrawler

import (
	"net/netip"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
)

const pingCacheSize = 500_000

// pingCache remembers for a while whether an address answered a DHT ping, so that a host listed as a peer of
// many torrents is probed once instead of once per torrent.
type pingCache struct {
	entries *lru.LRU[netip.AddrPort, pingState]
}

type pingState uint8

const (
	pingDead pingState = iota
	pingAlive
	pingAliveClosedHint
)

func newPingCache(ttl time.Duration) *pingCache {
	if ttl <= 0 {
		return &pingCache{}
	}

	return &pingCache{entries: lru.NewLRU[netip.AddrPort, pingState](pingCacheSize, nil, ttl)}
}

func (p *pingCache) get(addr netip.AddrPort) (alive bool, cached bool) {
	if p.entries == nil {
		return false, false
	}

	state, cached := p.entries.Get(addr)

	return state != pingDead, cached
}

func (p *pingCache) closedHint(addr netip.AddrPort) bool {
	if p.entries == nil {
		return false
	}

	state, _ := p.entries.Peek(addr)

	return state == pingAliveClosedHint
}

func (p *pingCache) put(addr netip.AddrPort, alive bool) {
	state := pingDead
	if alive {
		state = pingAlive
	}

	p.putState(addr, state)
}

func (p *pingCache) putState(addr netip.AddrPort, state pingState) {
	if p.entries != nil {
		p.entries.Add(addr, state)
	}
}
