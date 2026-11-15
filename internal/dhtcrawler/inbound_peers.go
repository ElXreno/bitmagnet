package dhtcrawler

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"net"
	"os"
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

	stageInboundRegistered   = "registered"
	stageInboundReplaced     = "replaced"
	stageInboundSkippedFull  = "skipped_full"
	stageInboundUnknownHash  = "unknown_hash"
	stageInboundHandshake    = "handshake_failed"
	stageInboundNoMetaInfo   = "peer_no_metainfo"
	stageInboundFetchFailed  = "fetch_failed"
	stageInboundBusy         = "busy"
	stageInboundConnAccepted = "conn_accepted"
	stageInboundConnRejected = "conn_rejected"
)

type inboundConfig struct {
	maxHashes int
	hold      time.Duration
	maxConns  int
}

func inboundConfigFromEnv() (inboundConfig, bool) {
	if os.Getenv("BITMAGNET_INBOUND_PEERS") != "on" {
		return inboundConfig{}, false
	}

	return inboundConfig{
		maxHashes: envInt("BITMAGNET_INBOUND_MAX_HASHES", 2000),
		hold:      time.Duration(envInt("BITMAGNET_INBOUND_HOLD_SECONDS", 300)) * time.Second,
		maxConns:  envInt("BITMAGNET_INBOUND_MAX_CONNS", 500),
	}, true
}

type inboundEntry struct {
	req       nodeHasPeersForHash
	seeders   uint32
	since     time.Time
	deadline  time.Time
	active    int
	announced bool
}

type inboundPeers struct {
	c         *crawler
	config    inboundConfig
	receiver  metainforequester.Receiver
	listeners []net.Listener
	trackers  []*trackerScraper
	slots     chan struct{}
	now       func() time.Time
	mu        sync.Mutex
	entries   map[protocol.ID]*inboundEntry
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
		now:       time.Now,
		entries:   make(map[protocol.ID]*inboundEntry),
	}

	if c.trackers.enableHolds(receiver.PeerID(), listenerPort(v4)) {
		p.trackers = append(p.trackers, c.trackers)
	}

	if v6 != nil {
		p.listeners = append(p.listeners, v6)

		if c.trackers6.enableHolds(receiver.PeerID(), listenerPort(v6)) {
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

		select {
		case p.slots <- struct{}{}:
			p.c.countHash(sourceInboundPeer, stageInboundConnAccepted, 1)

			go p.handle(ctx, conn)
		default:
			p.c.countHash(sourceInboundPeer, stageInboundConnRejected, 1)

			_ = conn.Close()
		}
	}
}

func (p *inboundPeers) handle(ctx context.Context, conn net.Conn) {
	defer func() {
		if recovered := recover(); recovered != nil {
			p.c.countHash(sourceInboundPeer, "panic", 1)
			p.c.logger.Errorw("incoming peer connection panicked", "panic", recovered, "peer", conn.RemoteAddr())
		}
	}()

	defer func() {
		<-p.slots
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

	entry, refusal := p.claim(in.InfoHash)
	if entry == nil {
		c.countHash(sourceInboundPeer, refusal, 1)

		return refusal
	}

	defer p.unclaim(entry)

	res, err := in.Fetch()

	switch {
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

	if !p.take(entry) {
		c.countHash(sourceInboundPeer, stageAlreadyFetched, 1)

		return stageAlreadyFetched
	}

	c.fetched.add(in.InfoHash)
	c.countHash(sourceInboundPeer, stageMetaInfoOK, 1)
	c.countHash(sourceInboundPeer, stageMetaInfoOK+trackerSource(entry.seeders), 1)
	c.countHash(sourceInboundPeer, stageMetaInfoOK+"_wait_seconds", int(p.now().Sub(entry.since).Seconds()))

	if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok && dht.IsIPv6(tcpAddr.AddrPort().Addr()) {
		c.countHash(sourceInboundPeer, stageMetaInfoOK+"6", 1)
	}

	select {
	case <-ctx.Done():
	case c.persistTorrents.In() <- infoHashWithMetaInfo{
		nodeHasPeersForHash: nodeHasPeersForHash{
			infoHash: in.InfoHash,
			node:     entry.req.node,
			source:   sourceInboundPeer,
			origin:   entry.req.source,
		},
		metaInfo: res.Info,
	}:
	}

	if res.Encrypted {
		return "ok_mse"
	}

	return "ok"
}

func (p *inboundPeers) register(req nodeHasPeersForHash) {
	if p == nil {
		return
	}

	seeders := uint32(0)
	for _, scraper := range p.trackers {
		seeders = max(seeders, scraper.holdableSeeders(req.infoHash))
	}

	if seeders == 0 || p.c.fetched.test(req.infoHash) {
		return
	}

	switch p.add(req, seeders) {
	case "":
		return
	case stageInboundSkippedFull:
		p.c.countHash(sourceInboundPeer, stageInboundSkippedFull, 1)

		return
	case stageInboundReplaced:
		p.c.countHash(sourceInboundPeer, stageInboundReplaced, 1)
	}

	p.c.countHash(sourceInboundPeer, stageInboundRegistered, 1)
	p.c.countHash(sourceInboundPeer, stageInboundRegistered+trackerSource(seeders), 1)
}

func (p *inboundPeers) add(req nodeHasPeersForHash, seeders uint32) string {
	now := p.now()

	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.entries[req.infoHash]; exists {
		return ""
	}

	stage := stageInboundRegistered

	if len(p.entries) >= p.config.maxHashes {
		weakest, found := p.weakest()
		if !found || p.entries[weakest].seeders >= seeders {
			return stageInboundSkippedFull
		}

		p.drop(weakest)

		stage = stageInboundReplaced
	}

	p.entries[req.infoHash] = &inboundEntry{
		req:      req,
		seeders:  seeders,
		since:    now,
		deadline: now.Add(p.config.hold),
	}

	for _, scraper := range p.trackers {
		scraper.hold(req.infoHash)
	}

	return stage
}

func (p *inboundPeers) weakest() (protocol.ID, bool) {
	var (
		weakest protocol.ID
		worst   *inboundEntry
	)

	for hash, entry := range p.entries {
		if entry.active > 0 {
			continue
		}

		if worst == nil || entry.seeders < worst.seeders ||
			(entry.seeders == worst.seeders && entry.deadline.Before(worst.deadline)) {
			weakest, worst = hash, entry
		}
	}

	return weakest, worst != nil
}

// called with mu held, so that leaving a swarm cannot overtake joining it
func (p *inboundPeers) drop(hash protocol.ID) {
	delete(p.entries, hash)

	for _, scraper := range p.trackers {
		scraper.release(hash)
	}
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

	for hash, entry := range p.entries {
		if entry.active == 0 && !now.Before(entry.deadline) {
			p.drop(hash)

			expired++
		}
	}

	p.mu.Unlock()

	p.c.countHash(sourceInboundPeer, "expired", expired)
}

func (p *inboundPeers) fetchedOutgoing(hash protocol.ID) {
	if p == nil {
		return
	}

	p.mu.Lock()

	_, registered := p.entries[hash]
	if registered {
		p.drop(hash)
	}

	p.mu.Unlock()

	if registered {
		p.c.countHash(sourceInboundPeer, "fetched_outgoing", 1)
	}
}

func (p *inboundPeers) hashes() []protocol.ID {
	p.mu.Lock()
	defer p.mu.Unlock()

	hashes := make([]protocol.ID, 0, len(p.entries))
	for hash := range p.entries {
		hashes = append(hashes, hash)
	}

	return hashes
}

func (p *inboundPeers) claim(hash protocol.ID) (*inboundEntry, string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, ok := p.entries[hash]

	switch {
	case !ok:
		return nil, stageInboundUnknownHash
	case entry.active >= inboundConnsPerHash:
		return nil, stageInboundBusy
	}

	entry.active++

	return entry, ""
}

func (p *inboundPeers) unclaim(entry *inboundEntry) {
	p.mu.Lock()
	entry.active--
	p.mu.Unlock()
}

func (p *inboundPeers) take(entry *inboundEntry) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.entries[entry.req.infoHash] != entry {
		return false
	}

	p.drop(entry.req.infoHash)

	return true
}

func (p *inboundPeers) size() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.entries)
}
