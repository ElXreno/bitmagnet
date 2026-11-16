package dhtcrawler

import (
	"cmp"
	"container/heap"
	"container/list"
	"context"
	"errors"
	"expvar"
	"fmt"
	"net"
	"net/netip"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	sourceInboundPeer   = "inbound_peer"
	inboundConnsPerHash = 4

	stageInboundRegistered     = "registered"
	stageInboundReplaced       = "replaced"
	stageInboundSkippedFull    = "skipped_full"
	stageInboundExpired        = "expired"
	stageInboundFetchedOut     = "fetched_outgoing"
	stageInboundUnknownHash    = "unknown_hash"
	stageInboundUnknownKey     = "unknown_key"
	stageInboundUnknownConn    = "unknown_conn"
	stageInboundUnknownBusy    = "unknown_busy"
	stageInboundUnknownDup     = "unknown_dup"
	stageInboundUnknownKnown   = "unknown_known"
	suffixInboundUnknown       = "_unknown"
	stageInboundActiveConn     = "active_conn"
	stageInboundLateConn       = "late_conn"
	stageInboundDHTConn        = "dht_conn"
	stageInboundHandshake      = "handshake_failed"
	stageInboundNoMetaInfo     = "peer_no_metainfo"
	stageInboundFetchFailed    = "fetch_failed"
	stageInboundBusy           = "busy"
	stageInboundClosedNetwork  = "closed_network"
	stageInboundConnAccepted   = "conn_accepted"
	stageInboundConnRejected   = "conn_rejected"
	stageInboundConnRejectedIP = "conn_rejected_ip"
)

type inboundConfig struct {
	maxHashes  int
	hold       time.Duration
	maxConns   int
	maxPerIP   int
	lateMax    int
	lateTTL    time.Duration
	extraMin   uint32
	extraMax   int
	stateFile  string
	dht        bool
	dhtPPS     int
	dhtWorkers int
	dhtMax     int
	dhtTTL     time.Duration
	dhtSeeds   string
	unknown    bool
}

func inboundConfigFromEnv() (inboundConfig, bool) {
	if os.Getenv("BITMAGNET_INBOUND_PEERS") != "on" {
		return inboundConfig{}, false
	}

	stateFile := os.Getenv("BITMAGNET_INBOUND_STATE_FILE")
	if stateFile == "" {
		stateFile = inboundStateFile
	}

	holdSeconds := envInt("BITMAGNET_INBOUND_HOLD_SECONDS", 300)
	announcesPerSecond := envInt("BITMAGNET_TRACKER_ANNOUNCE_PPS", 30)

	return inboundConfig{
		maxHashes:  envInt("BITMAGNET_INBOUND_MAX_HASHES", 2*announcesPerSecond*holdSeconds),
		hold:       time.Duration(holdSeconds) * time.Second,
		maxConns:   envInt("BITMAGNET_INBOUND_MAX_CONNS", 3000),
		maxPerIP:   envInt("BITMAGNET_INBOUND_MAX_CONNS_PER_IP", 8),
		lateMax:    envInt("BITMAGNET_INBOUND_LATE_HASHES", 50_000),
		lateTTL:    time.Duration(envInt("BITMAGNET_INBOUND_LATE_SECONDS", 900)) * time.Second,
		extraMin:   uint32(envInt("BITMAGNET_INBOUND_EXTRA_MIN_SEEDERS", 2)),
		extraMax:   envInt("BITMAGNET_INBOUND_EXTRA_TRACKERS", 3),
		stateFile:  stateFile,
		dht:        os.Getenv("BITMAGNET_INBOUND_DHT") == "on",
		dhtPPS:     envInt("BITMAGNET_INBOUND_DHT_PPS", 150),
		dhtWorkers: envInt("BITMAGNET_INBOUND_DHT_WORKERS", inboundDHTWorkers),
		dhtMax:     envInt("BITMAGNET_INBOUND_DHT_HASHES", 300_000),
		dhtTTL:     time.Duration(envInt("BITMAGNET_INBOUND_DHT_SECONDS", 2700)) * time.Second,
		dhtSeeds:   dhtSeedsFromEnv(),
		unknown:    os.Getenv("BITMAGNET_INBOUND_UNKNOWN") == "on",
	}, true
}

func dhtSeedsFromEnv() string {
	switch mode := os.Getenv("BITMAGNET_INBOUND_DHT_SEEDS"); mode {
	case dhtSeedsOn, "off":
		return mode
	default:
		return dhtSeedsMeasure
	}
}

type inboundEntry struct {
	hash         protocol.ID
	req          nodeHasPeersForHash
	seeders      uint32
	seq          uint64
	since        time.Time
	deadline     time.Time
	until        time.Time
	elem         *list.Element
	active       int
	index        int
	announced    bool
	dhtAnnounced bool
	failed       bool
	extras       bool
	late         bool
	dht          bool
	unknown      bool
	done         bool
	class        uint8
}

type inboundHeap []*inboundEntry

func (h inboundHeap) Len() int { return len(h) }

func (h inboundHeap) Less(i, j int) bool {
	if h[i].seeders != h[j].seeders {
		return h[i].seeders < h[j].seeders
	}

	return h[i].seq < h[j].seq
}

func (h inboundHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

func (h *inboundHeap) Push(x any) {
	entry := x.(*inboundEntry)
	entry.index = len(*h)
	*h = append(*h, entry)
}

func (h *inboundHeap) Pop() any {
	old := *h
	entry := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]

	return entry
}

type inboundPeers struct {
	c          *crawler
	config     inboundConfig
	receiver   metainforequester.Receiver
	listeners  []net.Listener
	trackers   []*trackerScraper
	dhtQueue   chan protocol.ID
	dhtTargets []inboundDHTTarget
	dhtSet     *dhtHeldSet
	dhtJoins   chan nodeHasPeersForHash
	dhtLimiter *rate.Limiter
	dhtLookups chan struct{}
	slots      chan struct{}
	ipMu       sync.Mutex
	perIP      map[netip.Addr]int
	now        func() time.Time
	mu         sync.Mutex
	entries    map[protocol.ID]*inboundEntry
	weakest    inboundHeap
	late       map[protocol.ID]*inboundEntry
	lateOrder  *list.List
	mseIndex   *lru.LRU[metainforequester.MSEKey, protocol.ID]
	seq        uint64
	closing    bool

	unknownActive int
	unknownHashes map[protocol.ID]struct{}
}

func listenInbound(port uint16, logger *zap.SugaredLogger) (v4 net.Listener, v6 net.Listener) {
	config := net.ListenConfig{}
	address := fmt.Sprintf(":%d", port)

	v4, err := config.Listen(context.Background(), "tcp4", address)
	if err != nil {
		logger.Warnf("incoming peers are off: no TCP listener on port %d: %s", port, err.Error())

		return nil, nil
	}

	v6, err = config.Listen(context.Background(), "tcp6", address)
	if err != nil {
		logger.Warnf("incoming peers over IPv6 are off: no TCP listener on port %d: %s", port, err.Error())

		return v4, nil
	}

	return v4, v6
}

func listenerPort(listener net.Listener) uint16 {
	return uint16(listener.Addr().(*net.TCPAddr).Port)
}

func (c *crawler) setupInboundPeers(receiver metainforequester.Receiver, port uint16) {
	if config, on := inboundConfigFromEnv(); on {
		c.openInboundPeers(config, receiver, port)
	}
}

func (c *crawler) openInboundPeers(config inboundConfig, receiver metainforequester.Receiver, port uint16) {
	v4, v6 := listenInbound(port, c.logger)
	if v4 == nil {
		return
	}

	p := &inboundPeers{
		c:         c,
		config:    config,
		receiver:  receiver,
		listeners: []net.Listener{v4},
		slots:     make(chan struct{}, config.maxConns),
		perIP:     make(map[netip.Addr]int),
		now:       time.Now,
		entries:   make(map[protocol.ID]*inboundEntry),
		late:      make(map[protocol.ID]*inboundEntry),
		lateOrder: list.New(),

		unknownHashes: make(map[protocol.ID]struct{}),
		mseIndex: lru.NewLRU[metainforequester.MSEKey, protocol.ID](
			config.maxHashes+config.lateMax+config.dhtMax,
			nil,
			max(config.hold+config.lateTTL, config.dhtTTL)+time.Minute,
		),
	}

	stops := max(config.maxHashes, trackerAnnounceQueue)

	if c.trackers.enableHolds(receiver.PeerID(), listenerPort(v4), stops, p.join) {
		p.trackers = append(p.trackers, c.trackers)
	}

	if v6 != nil {
		p.listeners = append(p.listeners, v6)

		if c.trackers6.enableHolds(receiver.PeerID(), listenerPort(v6), stops, p.join) {
			p.trackers = append(p.trackers, c.trackers6)
		}
	}

	if len(p.trackers) == 0 {
		c.logger.Warn("incoming peers are off: they need tracker announces, and those are disabled")

		for _, listener := range p.listeners {
			_ = listener.Close()
		}

		return
	}

	for _, scraper := range p.trackers {
		scraper.onHeld = p.announced
		scraper.onExtra = p.extraJoined
	}

	if config.dht {
		p.enableDHT(v4, v6)
	}

	p.loadState()

	if p.dhtSet != nil && config.stateFile != "" {
		c.countHash(sourceInboundPeer, "dht_loaded", p.dhtSet.load(p.dhtFile(), uint32(p.now().Unix())))
	}

	c.inbound = p
}

func (p *inboundPeers) run(ctx context.Context) {
	if p == nil {
		return
	}

	for _, listener := range p.listeners {
		go p.serve(ctx, listener)
	}

	if p.dhtQueue != nil {
		for range max(p.config.dhtWorkers, 1) {
			go p.runDHT(ctx)
		}
	}

	if p.dhtJoins != nil {
		go p.runDHTJoins(ctx)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for tick := 1; ; tick++ {
		select {
		case <-ctx.Done():
			for _, listener := range p.listeners {
				_ = listener.Close()
			}

			return
		case <-ticker.C:
			p.expire()

			// mse keeps a counter per crypto_provides value a peer sends
			if provides, ok := expvar.Get("mseCryptoProvides").(*expvar.Map); ok && tick%60 == 0 {
				provides.Init()
			}
		}
	}
}

func (p *inboundPeers) serve(ctx context.Context, listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}

			p.c.countHash(sourceInboundPeer, "accept_error", 1)

			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}

			continue
		}

		host := inboundHost(conn.RemoteAddr())
		if !p.admit(host) {
			p.c.countHash(sourceInboundPeer, stageInboundConnRejectedIP, 1)

			_ = conn.Close()

			continue
		}

		select {
		case p.slots <- struct{}{}:
			p.c.countHash(sourceInboundPeer, stageInboundConnAccepted, 1)

			go p.handle(ctx, conn, host)
		default:
			p.depart(host)
			p.c.countHash(sourceInboundPeer, stageInboundConnRejected, 1)

			_ = conn.Close()
		}
	}
}

// one IPv6 host owns a whole /64
func inboundHost(addr net.Addr) netip.Addr {
	tcpAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		return netip.Addr{}
	}

	host := tcpAddr.AddrPort().Addr().Unmap()

	if prefix, err := host.Prefix(64); err == nil && host.Is6() {
		return prefix.Addr()
	}

	return host
}

func (p *inboundPeers) admit(host netip.Addr) bool {
	if p.config.maxPerIP <= 0 {
		return true
	}

	p.ipMu.Lock()
	defer p.ipMu.Unlock()

	if p.perIP[host] >= p.config.maxPerIP {
		return false
	}

	p.perIP[host]++

	return true
}

func (p *inboundPeers) depart(host netip.Addr) {
	if p.config.maxPerIP <= 0 {
		return
	}

	p.ipMu.Lock()
	defer p.ipMu.Unlock()

	if p.perIP[host]--; p.perIP[host] <= 0 {
		delete(p.perIP, host)
	}
}

func (p *inboundPeers) handle(ctx context.Context, conn net.Conn, host netip.Addr) {
	defer func() {
		if recovered := recover(); recovered != nil {
			p.c.countHash(sourceInboundPeer, "panic", 1)
			p.c.logger.Errorw("incoming peer connection panicked", "panic", recovered, "peer", conn.RemoteAddr())
		}
	}()

	defer func() {
		<-p.slots
		p.depart(host)
	}()

	defer conn.Close()

	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	result := p.receive(ctx, conn)

	p.c.dialsTotal.With(prometheus.Labels{"source": sourceInboundPeer, "result": result}).Inc()
}

func inboundHandshakeResult(err error) string {
	if result := classifyDialError(err); result == "no_extension_protocol" {
		return result
	}

	return "handshake_" + classifyReadError(err)
}

func (p *inboundPeers) receive(ctx context.Context, conn net.Conn) string {
	c := p.c

	in, err := p.receiver.Accept(conn, p.lookupMSE)

	switch {
	case errors.Is(err, metainforequester.ErrUnknownInfoHash):
		c.countHash(sourceInboundPeer, stageInboundUnknownKey, 1)

		return stageInboundUnknownKey
	case err != nil:
		c.countHash(sourceInboundPeer, stageInboundHandshake, 1)

		return inboundHandshakeResult(err)
	}

	entry, kind := p.claim(in.InfoHash, in.Encrypted)
	c.countHash(sourceInboundPeer, kind, 1)

	if in.Dialect {
		c.countHash(sourceInboundPeer, "dialect_conn", 1)
	}

	scraped := ""
	if entry != nil && entry.class != dhtClassNone {
		scraped = "_dht" + dhtClassNames[entry.class]
		c.countHash(sourceInboundPeer, stageInboundDHTConn+dhtClassNames[entry.class], 1)
	}

	if entry == nil {
		if kind == stageInboundUnknownHash {
			switch {
			case c.fetched.test(in.InfoHash):
				c.countHash(sourceInboundPeer, "unknown_fetched", 1)
			case c.closedSwarms.test(in.InfoHash):
				c.countHash(sourceInboundPeer, "unknown_closed", 1)
			default:
				c.countHash(sourceInboundPeer, "unknown_other", 1)

				if p.config.unknown && !in.Encrypted {
					entry = p.claimUnknown(in.InfoHash)
				}

				if entry != nil && c.torrentInDatabase(ctx, in.InfoHash) {
					c.fetched.add(in.InfoHash)
					c.countHash(sourceInboundPeer, stageInboundUnknownKnown, 1)
					p.unclaim(entry)

					entry = nil
				}
			}
		}

		if entry == nil {
			return kind
		}

		c.countHash(sourceInboundPeer, stageInboundUnknownConn, 1)
	}

	defer p.unclaim(entry)

	res, err := in.Fetch()

	_ = conn.Close()

	switch {
	case errors.Is(err, metainforequester.ErrSelfPeer):
		c.countHash(sourceInboundPeer, "self_conn", 1)

		return classifyDialError(err)
	case errors.Is(err, metainforequester.ErrClosedNetworkPeer):
		c.countHash(sourceInboundPeer, stageInboundClosedNetwork, 1)
		c.closedSwarms.add(in.InfoHash)
		p.release(in.InfoHash, "released_closed_swarm")

		return classifyDialError(err)
	case errors.Is(err, metainforequester.ErrIncomingHandshake):
		c.countHash(sourceInboundPeer, stageInboundHandshake, 1)

		return inboundHandshakeResult(err)
	case errors.Is(err, metainforequester.ErrPeerHasNoMetaInfo):
		c.countHash(sourceInboundPeer, stageInboundNoMetaInfo, 1)

		if scraped != "" {
			c.countHash(sourceInboundPeer, stageInboundNoMetaInfo+scraped, 1)
		}

		if entry.unknown {
			c.countHash(sourceInboundPeer, stageInboundNoMetaInfo+suffixInboundUnknown, 1)
		}

		return "no_metadata_size"
	case errors.Is(err, metainforequester.ErrFetchedElsewhere):
		c.countHash(sourceInboundPeer, stageAlreadyFetched, 1)

		return stageAlreadyFetched
	case errors.Is(err, metainforequester.ErrIncomingBudget):
		c.countFetchFailed(entry)

		return "budget"
	case err != nil:
		c.countFetchFailed(entry)

		return classifyDialError(err)
	}

	if banErr := c.banningChecker.Check(res.Info); banErr != nil {
		_ = c.blockingManager.Block(ctx, []protocol.ID{in.InfoHash}, false)

		p.take(entry)
		c.countHash(sourceInboundPeer, stageBanned, 1)

		return stageBanned
	}

	switch taken := p.take(entry); {
	case taken && !entry.unknown:
		c.fetched.add(in.InfoHash)
	case !taken && !entry.dht, c.fetched.testAndAdd(in.InfoHash):
		c.countHash(sourceInboundPeer, stageAlreadyFetched, 1)

		return stageAlreadyFetched
	}

	if kind == stageInboundLateConn {
		c.countHash(sourceInboundPeer, stageMetaInfoOK+"_late", 1)
	}

	if p.hadExtras(entry) {
		c.countHash(sourceInboundPeer, stageMetaInfoOK+"_extra", 1)
	}

	c.countHash(sourceInboundPeer, stageMetaInfoOK, 1)

	if in.Dialect {
		c.countHash(sourceInboundPeer, stageMetaInfoOK+"_dialect", 1)
	}

	if scraped != "" {
		c.countHash(sourceInboundPeer, stageMetaInfoOK+scraped, 1)
	}

	if waited := int(p.now().Sub(entry.since).Seconds()); entry.unknown {
		c.countHash(sourceInboundPeer, stageMetaInfoOK+suffixInboundUnknown, 1)
	} else if entry.dht {
		c.countHash(sourceInboundPeer, stageMetaInfoOK+"_dht", 1)
		c.countHash(sourceInboundPeer, stageMetaInfoOK+"_dht_wait_seconds", waited)
	} else {
		c.countHash(sourceInboundPeer, stageMetaInfoOK+trackerSource(entry.seeders), 1)
		c.countHash(sourceInboundPeer, stageMetaInfoOK+"_wait_seconds", waited)

		if p.hadDHTAnnounce(entry) {
			c.countHash(sourceInboundPeer, stageMetaInfoOK+"_dht_announced", 1)
		}
	}

	if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok && dht.IsIPv6(tcpAddr.AddrPort().Addr()) {
		c.countHash(sourceInboundPeer, stageMetaInfoOK+"6", 1)
	}

	origin := p.origin(entry)
	persist := nodeHasPeersForHash{
		infoHash: in.InfoHash,
		node:     origin.node,
		source:   sourceInboundPeer,
		origin:   origin.source,
	}

	if !c.persistAs(&persist, res.Identity) {
		return "ok_alias"
	}

	select {
	case <-ctx.Done():
	case c.persistTorrents.In() <- infoHashWithMetaInfo{nodeHasPeersForHash: persist, metaInfo: res.Info}:
	}

	if res.Encrypted {
		return "ok_mse"
	}

	return "ok"
}

// join decides whether the normal announce of a seeded hash stays in the swarm
func (p *inboundPeers) join(s *trackerScraper, index int, hash protocol.ID, seeders uint32) joinVerdict {
	if p.c.fetched.test(hash) {
		return joinNone
	}

	now := p.now()

	p.mu.Lock()
	defer p.mu.Unlock()

	_, known := p.entries[hash]

	switch {
	case p.closing:
		return joinNone
	case known && s.holding(hash):
		return joinHeld
	case known:
	case len(p.entries) >= p.config.maxHashes && p.weakest[0].seeders >= seeders:
		p.c.countHash(sourceInboundPeer, stageInboundSkippedFull, 1)

		return joinNone
	default:
		if len(p.entries) >= p.config.maxHashes {
			p.retire(p.weakest[0])
			p.c.countHash(sourceInboundPeer, stageInboundReplaced, 1)
		}

		if late, ok := p.late[hash]; ok {
			p.dropLate(late)
		}

		p.seq++

		entry := &inboundEntry{
			hash:     hash,
			seeders:  seeders,
			seq:      p.seq,
			since:    now,
			deadline: now.Add(p.config.hold),
		}

		p.entries[hash] = entry
		p.indexMSE(hash)
		heap.Push(&p.weakest, entry)
		p.c.countHash(sourceInboundPeer, stageInboundRegistered, 1)
		p.c.countHash(sourceInboundPeer, stageInboundRegistered+trackerSource(seeders), 1)
		p.queueDHT(hash)
	}

	s.markHeld(hash, index)

	return joinMember
}

func (p *inboundPeers) leave(entry *inboundEntry) {
	delete(p.entries, entry.hash)
	heap.Remove(&p.weakest, entry.index)

	for _, scraper := range p.trackers {
		scraper.release(entry.hash)
	}
}

// retire keeps the hash for late arrivals, forget drops it for good; both need mu
func (p *inboundPeers) retire(entry *inboundEntry) {
	p.leave(entry)

	if p.config.lateMax <= 0 {
		p.unindexMSE(entry.hash)

		return
	}

	evicted := 0

	for len(p.late) >= p.config.lateMax {
		p.removeLate(p.lateOrder.Front().Value.(*inboundEntry))

		evicted++
	}

	p.c.countHash(sourceInboundPeer, "late_evicted", evicted)

	entry.late = true
	entry.until = p.now().Add(p.config.lateTTL)
	entry.elem = p.lateOrder.PushBack(entry)
	p.late[entry.hash] = entry
}

func (p *inboundPeers) dropLate(entry *inboundEntry) {
	delete(p.late, entry.hash)
	p.lateOrder.Remove(entry.elem)
}

func (p *inboundPeers) removeLate(entry *inboundEntry) {
	p.dropLate(entry)
	p.unindexMSE(entry.hash)
}

func (p *inboundPeers) forget(hash protocol.ID) string {
	kind := ""

	if entry, ok := p.entries[hash]; ok {
		entry.done = true
		p.leave(entry)

		kind = "active"
	}

	if entry, ok := p.late[hash]; ok {
		entry.done = true
		p.removeLate(entry)

		kind = cmp.Or(kind, "late")
	}

	if p.dhtSet.remove(hash) {
		kind = cmp.Or(kind, "dht")
	}

	p.unindexMSE(hash)

	return kind
}

// leaveAll queues the stops of every membership, largest swarms first, and waits for the announce loops
func (p *inboundPeers) leaveAll(timeout time.Duration) {
	if p == nil {
		return
	}

	p.mu.Lock()
	p.closing = true

	entries := append([]*inboundEntry(nil), p.weakest...)
	sort.Slice(entries, func(a, b int) bool { return entries[a].seeders > entries[b].seeders })

	saved := p.wanted(entries)

	for _, entry := range entries {
		p.leave(entry)
	}

	var dhtHeld []byte
	if p.dhtSet != nil {
		dhtHeld = p.dhtSet.encode()
	}

	p.mu.Unlock()
	p.saveState(saved)
	p.saveDHT(dhtHeld)

	for _, scraper := range p.trackers {
		scraper.draining.Store(true)
	}

	pending := func() int {
		queued := 0

		for _, scraper := range p.trackers {
			for _, stops := range scraper.stops {
				queued += len(stops)
			}
		}

		return queued
	}

	for deadline := time.Now().Add(timeout); pending() > 0 && time.Now().Before(deadline); {
		time.Sleep(20 * time.Millisecond)
	}

	p.c.logger.Infow("left the swarms held for incoming peers", "swarms", len(entries), "stops_unsent", pending())
}

func (p *inboundPeers) announced(hash protocol.ID) {
	p.mu.Lock()
	entry, ok := p.entries[hash]
	first := ok && !entry.announced

	if first {
		entry.announced = true
	}

	p.mu.Unlock()

	if first {
		p.c.countHash(sourceInboundPeer, "announced", 1)
	}
}

func (p *inboundPeers) expire() {
	now := p.now()
	expired := 0

	p.mu.Lock()

	for _, entry := range p.entries {
		if !now.Before(entry.deadline) {
			p.retire(entry)

			expired++
		}
	}

	lateExpired := 0

	for front := p.lateOrder.Front(); front != nil; front = p.lateOrder.Front() {
		entry := front.Value.(*inboundEntry)
		if now.Before(entry.until) {
			break
		}

		p.removeLate(entry)

		lateExpired++
	}

	dhtExpired := p.dhtSet.expire(uint32(now.Unix()))

	p.mu.Unlock()

	p.c.countHash(sourceInboundPeer, stageInboundExpired, expired)
	p.c.countHash(sourceInboundPeer, "late_expired", lateExpired)
	p.c.countHash(sourceInboundPeer, "dht_expired", dhtExpired)
}

func (p *inboundPeers) fetchedOutgoing(hash protocol.ID) {
	p.release(hash, stageInboundFetchedOut)
}

func (p *inboundPeers) release(hash protocol.ID, stage string) {
	if p == nil {
		return
	}

	p.mu.Lock()
	kind := p.forget(hash)
	p.mu.Unlock()

	switch kind {
	case "active":
		p.c.countHash(sourceInboundPeer, stage, 1)
	case "late", "dht":
		p.c.countHash(sourceInboundPeer, kind+"_"+stage, 1)
	}
}

func (p *inboundPeers) outgoingFailed(req nodeHasPeersForHash) {
	if p == nil {
		return
	}

	p.mu.Lock()

	entry, ok := p.entries[req.infoHash]
	if !ok {
		entry, ok = p.late[req.infoHash]
	}

	first := ok && !entry.failed

	if first && !entry.late && entry.seeders >= p.config.extraMin {
		for _, scraper := range p.trackers {
			scraper.wantExtras(req.infoHash, p.config.extraMax)
		}
	}

	if ok {
		entry.failed = true

		if entry.req.source == "" || (!sampled(entry.req.source) && sampled(req.source)) {
			entry.req = req
			entry.req.tokens = nil
		}
	}

	joins := !ok && p.dhtJoins != nil && p.dhtSet.get(req.infoHash) == nil

	p.mu.Unlock()

	if first {
		p.c.countHash(sourceInboundPeer, "outgoing_failed", 1)
	}

	if joins && !p.c.seeded(req.infoHash) && !p.c.fetched.test(req.infoHash) {
		p.queueJoin(req)
	}
}

func (p *inboundPeers) extraJoined(hash protocol.ID) {
	p.mu.Lock()
	entry, ok := p.entries[hash]
	first := ok && !entry.extras

	if first {
		entry.extras = true
	}

	p.mu.Unlock()

	if first {
		p.c.countHash(sourceInboundPeer, "extra_joined", 1)
	}
}

func (p *inboundPeers) hadDHTAnnounce(entry *inboundEntry) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return entry.dhtAnnounced
}

func (p *inboundPeers) hadExtras(entry *inboundEntry) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return entry.extras
}

func sampled(source string) bool {
	return isSampleSource(source) || isSampleLookupSource(source)
}

func (p *inboundPeers) origin(entry *inboundEntry) nodeHasPeersForHash {
	p.mu.Lock()
	defer p.mu.Unlock()

	return entry.req
}

func (p *inboundPeers) indexMSE(hash protocol.ID) {
	p.mseIndex.Add(metainforequester.MSEKeyOf(hash), hash)
}

// unindexMSE drops the key of a hash that no set holds any more; it needs mu
func (p *inboundPeers) unindexMSE(hash protocol.ID) {
	if _, ok := p.entries[hash]; ok {
		return
	}

	if _, ok := p.late[hash]; ok {
		return
	}

	if p.dhtSet.get(hash) != nil {
		return
	}

	p.mseIndex.Remove(metainforequester.MSEKeyOf(hash))
}

func (p *inboundPeers) lookupMSE(key metainforequester.MSEKey) (protocol.ID, bool) {
	return p.mseIndex.Get(key)
}

func (p *inboundPeers) claim(hash protocol.ID, encrypted bool) (*inboundEntry, string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	kind := stageInboundActiveConn

	entry, ok := p.entries[hash]
	if !ok {
		kind = stageInboundLateConn
		entry, ok = p.late[hash]

		if ok && p.c.fetched.test(hash) {
			p.removeLate(entry)

			ok = false
		}
	}

	if held := p.dhtSet.get(hash); !ok && held != nil {
		switch {
		case p.c.fetched.test(hash), p.c.closedSwarms.test(hash):
			p.dhtSet.remove(hash)
		case held.active >= inboundConnsPerHash:
			return nil, stageInboundBusy
		default:
			held.active++

			return &inboundEntry{
				hash:  hash,
				req:   held.request(),
				since: time.Unix(int64(held.since), 0),
				dht:   true,
				class: held.class,
			}, stageInboundDHTConn
		}
	}

	switch {
	case !ok:
		return nil, stageInboundUnknownHash
	case entry.active >= inboundConnsPerHash:
		return nil, stageInboundBusy
	}

	entry.active++

	return entry, kind
}

// claimUnknown serves a plaintext hash that no set holds: one fetch per hash, a quarter of the connections at most
func (p *inboundPeers) claimUnknown(hash protocol.ID) *inboundEntry {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.unknownHashes[hash]; ok {
		p.c.countHash(sourceInboundPeer, stageInboundUnknownDup, 1)

		return nil
	}

	if p.unknownActive >= p.config.maxConns/4 {
		p.c.countHash(sourceInboundPeer, stageInboundUnknownBusy, 1)

		return nil
	}

	p.unknownActive++
	p.unknownHashes[hash] = struct{}{}

	return &inboundEntry{hash: hash, req: nodeHasPeersForHash{infoHash: hash}, since: p.now(), unknown: true}
}

func (c *crawler) torrentInDatabase(ctx context.Context, hash protocol.ID) bool {
	if c.inDatabase != nil {
		return c.inDatabase(ctx, hash)
	}

	if c.dao == nil {
		return false
	}

	count, err := c.dao.Torrent.WithContext(ctx).Where(c.dao.Torrent.InfoHash.Eq(hash)).Count()

	return err == nil && count > 0
}

func (c *crawler) countFetchFailed(entry *inboundEntry) {
	c.countHash(sourceInboundPeer, stageInboundFetchFailed, 1)

	if entry.unknown {
		c.countHash(sourceInboundPeer, stageInboundFetchFailed+suffixInboundUnknown, 1)
	}
}

func (p *inboundPeers) unclaim(entry *inboundEntry) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if entry.unknown {
		p.unknownActive--
		delete(p.unknownHashes, entry.hash)

		return
	}

	if !entry.dht {
		entry.active--
	} else if held := p.dhtSet.get(entry.hash); held != nil && held.active > 0 {
		held.active--
	}
}

func (p *inboundPeers) take(entry *inboundEntry) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if entry.done {
		return false
	}

	held := !entry.dht || p.dhtSet.get(entry.hash) != nil
	entry.done = true
	p.forget(entry.hash)

	return held
}

func (p *inboundPeers) size() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.entries)
}

func (p *inboundPeers) lateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.late)
}

func (p *inboundPeers) dhtSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.dhtSet.size()
}
