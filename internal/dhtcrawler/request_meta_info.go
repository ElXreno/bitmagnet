package dhtcrawler

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/clientid"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/server"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	"github.com/prometheus/client_golang/prometheus"
)

var errBanned = errors.New("banned")

var errNoCandidates = errors.New("no peers worth dialing")

var errClosedSwarm = errors.New("swarm of a closed network")

const (
	implSilent     = "silent"
	implRegular    = "regular"
	implClosedHint = "closed_hint"
)

var errClosedSwarmHint = errors.New("every answering peer is a node of a closed network")

var closedSwarmPolicy = os.Getenv("BITMAGNET_CLOSED_SWARM") != "off"

var closedSwarmHintPolicy = closedSwarmPolicy && os.Getenv("BITMAGNET_CLOSED_SWARM_HINT") != "off"

func (c *crawler) runRequestMetaInfo(ctx context.Context) {
	_ = c.requestMetaInfo.Run(ctx, func(req infoHashWithPeers) {
		if c.fetched.test(req.infoHash) {
			c.countHash(req.source, stageAlreadyFetched, 1)

			return
		}

		mi, reqErr := c.doRequestMetaInfo(ctx, req.source, req.infoHash, req.peers)
		if reqErr != nil {
			if errors.Is(reqErr, errBanned) {
				c.countHash(req.source, stageBanned, 1)
				c.trackerOutcome(req.infoHash, stageBanned)
				c.inbound.release(req.infoHash, "released_banned")
			} else if errors.Is(reqErr, errClosedSwarm) {
				c.countHash(req.source, "closed_swarm", 1)
				c.trackerOutcome(req.infoHash, "closed_swarm")
				c.inbound.release(req.infoHash, "released_closed_swarm")

				if closedSwarmPolicy {
					c.closedSwarms.add(req.infoHash)
				} else {
					c.giveUp(req.infoHash)
				}
			} else if errors.Is(reqErr, errClosedSwarmHint) {
				c.countHash(req.source, "closed_swarm_hint", 1)
				c.trackerOutcome(req.infoHash, "closed_swarm_hint")
				c.closedSwarms.add(req.infoHash)
				c.inbound.release(req.infoHash, "released_closed_swarm")
			} else if errors.Is(reqErr, errNoCandidates) {
				c.countHash(req.source, "no_candidates", 1)
				c.trackerOutcome(req.infoHash, "no_candidates")
				c.giveUp(req.infoHash)
				c.inbound.outgoingFailed(req.nodeHasPeersForHash)
			} else {
				c.countHash(req.source, stageMetaInfoFail, 1)
				c.trackerOutcome(req.infoHash, stageMetaInfoFail)
				c.giveUp(req.infoHash)
				c.inbound.outgoingFailed(req.nodeHasPeersForHash)
			}

			return
		}

		c.fetched.add(req.infoHash)
		c.inbound.fetchedOutgoing(req.infoHash)
		c.countHash(req.source, stageMetaInfoOK, 1)
		c.countClass(req.nodeHasPeersForHash, stageMetaInfoOK, 1)

		if req.visit != "" {
			c.countHash(req.source, stageMetaInfoOK+"_"+req.visit, 1)
		}

		c.trackerOutcome(req.infoHash, stageMetaInfoOK)

		persist := req.nodeHasPeersForHash
		if !c.persistAs(&persist, mi.Identity) {
			return
		}

		select {
		case <-ctx.Done():
		case c.persistTorrents.In() <- infoHashWithMetaInfo{
			nodeHasPeersForHash: persist,
			metaInfo:            mi.Info,
		}:
		}
	})
}

// persistAs points a fetched hash at the hash its torrent is stored under and reports whether to store it.
func (c *crawler) persistAs(req *nodeHasPeersForHash, identity metainfo.Identity) bool {
	switch identity.Kind {
	case metainfo.KindV2:
		c.countHash(req.source, stageMetaInfoOK+"_v2", 1)
	case metainfo.KindHybridV2:
		c.countHash(req.source, stageMetaInfoOK+"_hybrid_v2", 1)
		c.aliases.add(req.infoHash)

		if c.fetched.test(identity.InfoHash) {
			c.countHash(req.source, "alias_of_fetched", 1)

			return false
		}

		c.fetched.add(identity.InfoHash)
		req.infoHash = identity.InfoHash
	}

	return true
}

const metaInfoParallelDials = 4

var utpForSilentPeers = os.Getenv("BITMAGNET_UTP_SILENT_PEERS") == "1"

var dialEveryPeer = os.Getenv("BITMAGNET_DIAL_EVERY_PEER") == "1"

func (c *crawler) doRequestMetaInfo(
	ctx context.Context,
	source string,
	hash protocol.ID,
	peers []netip.AddrPort,
) (metainforequester.Response, error) {
	type dialResult struct {
		res    metainforequester.Response
		err    error
		silent bool
		addr   netip.AddrPort
		impl   string
	}

	dialCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type candidate struct {
		addr netip.AddrPort
		prev string
		ping string
		impl string
	}

	probed := make(chan candidate, len(peers))

	for _, p := range peers {
		go func() {
			cand := candidate{addr: p, prev: "new", ping: "ok", impl: implSilent}
			pingSource := "peer_ping" + c.family(p.Addr()).suffix

			if c.deadPeers.test(peerKey(p)) {
				cand.prev = "dead"
			}

			if alive, cached := c.pingCache.get(p); cached {
				if alive {
					cand.impl = implRegular

					if c.pingCache.closedHint(p) {
						cand.impl = implClosedHint
					}

					c.countHash(pingSource, "cached_ok", 1)
				} else {
					cand.ping = "fail"

					c.countHash(pingSource, "cached_fail", 1)
				}

				probed <- cand

				return
			}

			pingCtx, pingCancel := context.WithTimeout(server.WithMaxLimiterWait(ctx, lookupMaxLimiterWait), peerProbeTimeout)
			defer pingCancel()

			pong, pingErr := c.client.Ping(pingCtx, p)

			switch {
			case pingErr == nil:
				cand.impl = implRegular
				state := pingAlive

				if clientid.ClosedNetworkNode(pong.ClientID) {
					cand.impl = implClosedHint
					state = pingAliveClosedHint
				}

				c.pingCache.putState(p, state)
				c.countHash(pingSource, "sent_ok", 1)
			case errors.Is(pingErr, server.ErrLimited):
				cand.ping = "fail"

				c.countHash(pingSource, "limited", 1)
			default:
				cand.ping = "fail"

				c.pingCache.put(p, false)
				c.countHash(pingSource, "sent_fail", 1)
			}

			probed <- cand
		}()
	}

	skipSilent := trackerPolicy != "lookup" && c.unseeded(hash)
	candidates := make([]candidate, 0, len(peers))

	var unanswered []candidate

	regular, hinted := 0, 0

	for range peers {
		cand := <-probed

		switch cand.impl {
		case implRegular:
			regular++
		case implClosedHint:
			hinted++
		}

		switch {
		case cand.ping == "ok" || dialEveryPeer:
			candidates = append(candidates, cand)
		case cand.prev == "dead":
			c.probeTotal.With(prometheus.Labels{"prev": cand.prev, "ping": cand.ping, "dial": "skipped_dead"}).Inc()
		case skipSilent:
			c.probeTotal.With(prometheus.Labels{"prev": cand.prev, "ping": cand.ping, "dial": "skipped_unseeded"}).Inc()
		default:
			unanswered = append(unanswered, cand)
		}
	}

	candidates = append(candidates, unanswered...)

	swarm := "silent"

	switch {
	case hinted > 0 && regular == 0:
		swarm = "hint_only"
	case hinted > 0:
		swarm = "hint_mixed"
	case regular > 0:
		swarm = "regular"
	}

	countSwarm := func(outcome string) {
		c.swarmTotal.With(prometheus.Labels{"class": swarm, "outcome": outcome}).Inc()
	}

	if closedSwarmHintPolicy && swarm == "hint_only" && !c.seeded(hash) {
		countSwarm("skipped_hint")
		c.countHash(source, "closed_swarm_dials_skipped", len(candidates))

		return metainforequester.Response{}, errClosedSwarmHint
	}

	results := make(chan dialResult, len(candidates))
	next := 0
	inflight := 0

	var errs []error

	closedPeers := 0

	for {
		for inflight < metaInfoParallelDials && next < len(candidates) {
			cand := candidates[next]
			next++
			inflight++

			go func() {
				requestCtx := dialCtx
				if cand.ping == "ok" || utpForSilentPeers {
					requestCtx = metainforequester.WithUTPFallback(dialCtx)
				}

				res, err := c.metainfoRequester.Request(requestCtx, hash, cand.addr)
				outcome := dialOutcome(err)

				if outcome == "timeout" || outcome == "refused" || outcome == "unreachable" {
					c.deadPeers.add(peerKey(cand.addr))
				}

				dumpPeer(outcome, cand.ping, cand.addr)

				if outcome != "skipped" {
					c.probeTotal.With(prometheus.Labels{"prev": cand.prev, "ping": cand.ping, "dial": outcome}).Inc()
				}

				results <- dialResult{res: res, err: err, silent: cand.ping != "ok", addr: cand.addr, impl: cand.impl}
			}()
		}

		if inflight == 0 {
			break
		}

		r := <-results
		inflight--

		result := classifyDialError(r.err)
		switch {
		case r.err == nil && r.res.Transport == "utp" && r.silent:
			result = "ok_utp_silent"
		case r.err == nil && r.res.Transport == "utp":
			result = "ok_utp"
		case r.silent && result == "utp_dial_failed":
			result = "utp_silent_dial_failed"
		case r.err == nil && r.res.Encrypted:
			result = "ok_mse"
		}

		c.dialsTotal.With(prometheus.Labels{"source": source, "result": result}).Inc()
		c.peerDialTotal.With(prometheus.Labels{"node": r.impl, "result": result}).Inc()

		if r.err != nil {
			errs = append(errs, r.err)

			if errors.Is(r.err, metainforequester.ErrClosedNetworkPeer) {
				closedPeers++

				if closedSwarmPolicy && next < len(candidates) {
					c.countHash(source, "closed_swarm_dials_skipped", len(candidates)-next)
					next = len(candidates)
				}
			}

			continue
		}

		if banErr := c.banningChecker.Check(r.res.Info); banErr != nil {
			_ = c.blockingManager.Block(ctx, []protocol.ID{hash}, false)

			return metainforequester.Response{}, errors.Join(errBanned, banErr)
		}

		countSwarm(stageMetaInfoOK)

		if c.family(r.addr.Addr()) == c.v6 {
			c.countHash(source, stageMetaInfoOK+"6", 1)
		}

		return r.res, nil
	}

	if len(errs) == 0 {
		countSwarm("no_candidates")

		return metainforequester.Response{}, errNoCandidates
	}

	if closedPeers > 0 {
		countSwarm("closed_swarm")

		return metainforequester.Response{}, errors.Join(errClosedSwarm, errors.Join(errs...))
	}

	countSwarm(stageMetaInfoFail)

	return metainforequester.Response{}, errors.Join(errs...)
}

const peerProbeTimeout = 800 * time.Millisecond

func peerKey(p netip.AddrPort) protocol.ID {
	var id protocol.ID

	b, _ := p.MarshalBinary()
	copy(id[:], b)

	return id
}

func dialOutcome(err error) string {
	switch result := classifyDialError(err); result {
	case "dial_timeout", "utp_dial_failed":
		return "timeout"
	case "dial_refused":
		return "refused"
	case "dial_unreachable", "dial_other":
		return "unreachable"
	case "limited", "canceled":
		return "skipped"
	case "ok":
		return "ok"
	default:
		return "connected"
	}
}
