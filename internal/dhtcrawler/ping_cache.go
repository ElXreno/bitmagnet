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
	entries *lru.LRU[netip.AddrPort, bool]
}

func newPingCache(ttl time.Duration) *pingCache {
	if ttl <= 0 {
		return &pingCache{}
	}

	return &pingCache{entries: lru.NewLRU[netip.AddrPort, bool](pingCacheSize, nil, ttl)}
}

func (p *pingCache) get(addr netip.AddrPort) (alive bool, cached bool) {
	if p.entries == nil {
		return false, false
	}

	return p.entries.Get(addr)
}

func (p *pingCache) put(addr netip.AddrPort, alive bool) {
	if p.entries != nil {
		p.entries.Add(addr, alive)
	}
}
