package dhtcrawler

import (
	"net/netip"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
	boom "github.com/tylertreat/BoomFilters"
)

// nodeFamily is the routing state of one address family; suffix is "" for IPv4 and "6" for IPv6.
type nodeFamily struct {
	suffix                   string
	kTable                   ktable.Table
	nodesForPing             concurrency.BufferedConcurrentChannel[ktable.Node]
	nodesForFindNode         concurrency.BufferedConcurrentChannel[ktable.Node]
	nodesForSampleInfoHashes concurrency.BufferedConcurrentChannel[ktable.Node]
	ignoreNodes              *ignoreFilter
	sampledNodes             *ignoreFilter
	nodePool                 *nodePool
	discovered               chan ktable.Node
	revisits                 *revisits
}

func newNodeFamily(suffix string, kTable ktable.Table, scalingFactor int, sampleConcurrency int) *nodeFamily {
	return &nodeFamily{
		suffix: suffix,
		kTable: kTable,
		nodesForPing: concurrency.NewBufferedConcurrentChannel[ktable.Node](
			scalingFactor, scalingFactor),
		nodesForFindNode: concurrency.NewBufferedConcurrentChannel[ktable.Node](
			10*scalingFactor, 10*scalingFactor),
		nodesForSampleInfoHashes: concurrency.NewBufferedConcurrentChannel[ktable.Node](
			sampleConcurrency,
			sampleConcurrency,
		),
		ignoreNodes: &ignoreFilter{
			bloom: boom.NewStableBloomFilter(200_000*uint(scalingFactor), 2, 0.001),
		},
		sampledNodes: &ignoreFilter{
			bloom: boom.NewStableBloomFilter(20_000_000, 2, 0.001),
		},
		nodePool: newNodePool(),
		revisits: newRevisits(),
	}
}

func (f *nodeFamily) sourceSample() string {
	return sourceSample + f.suffix
}

func (c *crawler) family(addr netip.Addr) *nodeFamily {
	if c.v6 != nil && dht.IsIPv6(addr) {
		return c.v6
	}

	return c.v4
}

func (c *crawler) families() []*nodeFamily {
	if c.v6 == nil {
		return []*nodeFamily{c.v4}
	}

	return []*nodeFamily{c.v4, c.v6}
}

func isSampleSource(source string) bool {
	return source == sourceSample || source == sourceSample6
}

func isSampleLookupSource(source string) bool {
	return source == sourceSampleLookup || source == sourceSampleLookup6
}

func sampleLookupSource(source string) string {
	if source == sourceSample6 {
		return sourceSampleLookup6
	}

	return sourceSampleLookup
}

func isAnnounceSource(source string) bool {
	return source == dht.QAnnouncePeer || source == dht.QAnnouncePeer+"6"
}
