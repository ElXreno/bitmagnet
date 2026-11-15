package dhtcrawler

import (
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
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
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
	stageInboundActiveConn     = "active_conn"
	stageInboundLateConn       = "late_conn"
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
	maxHashes int
	hold      time.Duration
	maxConns  int
	maxPerIP  int
	lateMax   int
	lateTTL   time.Duration
	extraMin  uint32
	extraMax  int
	dht       bool
}

func inboundConfigFromEnv() (inboundConfig, bool) {
	if os.Getenv("BITMAGNET_INBOUND_PEERS") != "on" {
		return inboundConfig{}, false
	}

	holdSeconds := envInt("BITMAGNET_INBOUND_HOLD_SECONDS", 300)
	announcesPerSecond := envInt("BITMAGNET_TRACKER_ANNOUNCE_PPS", 30)

	return inboundConfig{
		maxHashes: envInt("BITMAGNET_INBOUND_MAX_HASHES", 2*announcesPerSecond*holdSeconds),
		hold:      time.Duration(holdSeconds) * time.Second,
		maxConns:  envInt("BITMAGNET_INBOUND_MAX_CONNS", 500),
		maxPerIP:  envInt("BITMAGNET_INBOUND_MAX_CONNS_PER_IP", 8),
		lateMax:   envInt("BITMAGNET_INBOUND_LATE_HASHES", 50_000),
		lateTTL:   time.Duration(envInt("BITMAGNET_INBOUND_LATE_SECONDS", 900)) * time.Second,
		extraMin:  uint32(envInt("BITMAGNET_INBOUND_EXTRA_MIN_SEEDERS", 2)),
		extraMax:  envInt("BITMAGNET_INBOUND_EXTRA_TRACKERS", 3),
		dht:       os.Getenv("BITMAGNET_INBOUND_DHT") == "on",
	}, true
}

type inboundEntry struct {
	hash      protocol.ID
	req       nodeHasPeersForHash
	seeders   uint32
	seq       uint64
	since     time.Time
	deadline  time.Time
	until     time.Time
	elem      *list.Element
	active    int
	index     int
	announced bool
	failed    bool
	extras    bool
	late      bool
	done      bool
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
	slots      chan struct{}
	ipMu       sync.Mutex
	perIP      map[netip.Addr]int
	now        func() time.Time
	mu         sync.Mutex
	entries    map[protocol.ID]*inboundEntry
	weakest    inboundHeap
	late       map[protocol.ID]*inboundEntry
	lateOrder  *list.List
	seq        uint64
	closing    bool
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
		for range inboundDHTWorkers {
			go p.runDHT(ctx)
		}
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

	in, err := p.receiver.Accept(conn, p.hashes)

	switch {
	case errors.Is(err, metainforequester.ErrUnknownInfoHash):
		c.countHash(sourceInboundPeer, stageInboundUnknownHash, 1)

		return stageInboundUnknownHash
	case err != nil:
		c.countHash(sourceInboundPeer, stageInboundHandshake, 1)

		return inboundHandshakeResult(err)
	}

	entry, kind := p.claim(in.InfoHash, in.Encrypted)
	c.countHash(sourceInboundPeer, kind, 1)

	if entry == nil {
		return kind
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

		return classifyDialError(err)
	case errors.Is(err, metainforequester.ErrIncomingHandshake):
		c.countHash(sourceInboundPeer, stageInboundHandshake, 1)

		return inboundHandshakeResult(err)
	case errors.Is(err, metainforequester.ErrPeerHasNoMetaInfo):
		c.countHash(sourceInboundPeer, stageInboundNoMetaInfo, 1)

		return "no_metadata_size"
	case errors.Is(err, metainforequester.ErrIncomingBudget):
		c.countHash(sourceInboundPeer, stageInboundFetchFailed, 1)

		return "budget"
	case err != nil:
		c.countHash(sourceInboundPeer, stageInboundFetchFailed, 1)

		return classifyDialError(err)
	}

	if banErr := c.banningChecker.Check(res.Info); banErr != nil {
		_ = c.blockingManager.Block(ctx, []protocol.ID{in.InfoHash}, false)

		p.take(entry)
		c.countHash(sourceInboundPeer, stageBanned, 1)

		return stageBanned
	}

	c.fetched.add(in.InfoHash)

	if !p.take(entry) {
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
	c.countHash(sourceInboundPeer, stageMetaInfoOK+trackerSource(entry.seeders), 1)
	c.countHash(sourceInboundPeer, stageMetaInfoOK+"_wait_seconds", int(p.now().Sub(entry.since).Seconds()))

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
			p.removeLate(late)
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

func (p *inboundPeers) removeLate(entry *inboundEntry) {
	delete(p.late, entry.hash)
	p.lateOrder.Remove(entry.elem)
}

func (p *inboundPeers) forget(hash protocol.ID) string {
	if entry, ok := p.entries[hash]; ok {
		entry.done = true
		p.leave(entry)

		return "active"
	}

	if entry, ok := p.late[hash]; ok {
		entry.done = true
		p.removeLate(entry)

		return "late"
	}

	return ""
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

	for _, entry := range entries {
		p.leave(entry)
	}

	p.mu.Unlock()

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

	p.mu.Unlock()

	p.c.countHash(sourceInboundPeer, stageInboundExpired, expired)
	p.c.countHash(sourceInboundPeer, "late_expired", lateExpired)
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
	case "late":
		p.c.countHash(sourceInboundPeer, "late_"+stage, 1)
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
		}
	}

	p.mu.Unlock()

	if first {
		p.c.countHash(sourceInboundPeer, "outgoing_failed", 1)
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

func (p *inboundPeers) hashes() []protocol.ID {
	p.mu.Lock()
	defer p.mu.Unlock()

	hashes := make([]protocol.ID, len(p.weakest))
	for i, entry := range p.weakest {
		hashes[i] = entry.hash
	}

	return hashes
}

func (p *inboundPeers) claim(hash protocol.ID, encrypted bool) (*inboundEntry, string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	kind := stageInboundActiveConn

	entry, ok := p.entries[hash]
	if !ok && !encrypted {
		kind = stageInboundLateConn
		entry, ok = p.late[hash]

		if ok && p.c.fetched.test(hash) {
			p.removeLate(entry)

			ok = false
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

func (p *inboundPeers) unclaim(entry *inboundEntry) {
	p.mu.Lock()
	entry.active--
	p.mu.Unlock()
}

func (p *inboundPeers) take(entry *inboundEntry) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if entry.done {
		return false
	}

	entry.done = true
	p.forget(entry.hash)

	return true
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
