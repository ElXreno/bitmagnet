package dhtcrawler

import (
	"context"
	"errors"
	"net/netip"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	"github.com/prometheus/client_golang/prometheus"
)

var errBanned = errors.New("banned")

func (c *crawler) runRequestMetaInfo(ctx context.Context) {
	_ = c.requestMetaInfo.Run(ctx, func(req infoHashWithPeers) {
		mi, reqErr := c.doRequestMetaInfo(ctx, req.source, req.infoHash, req.peers)
		if reqErr != nil {
			if errors.Is(reqErr, errBanned) {
				c.countHash(req.source, stageBanned, 1)
			} else {
				c.countHash(req.source, stageMetaInfoFail, 1)
			}

			return
		}

		c.countHash(req.source, stageMetaInfoOK, 1)
		select {
		case <-ctx.Done():
		case c.persistTorrents.In() <- infoHashWithMetaInfo{
			nodeHasPeersForHash: req.nodeHasPeersForHash,
			metaInfo:            mi.Info,
		}:
		}
	})
}

const metaInfoParallelDials = 4

func (c *crawler) doRequestMetaInfo(
	ctx context.Context,
	source string,
	hash protocol.ID,
	peers []netip.AddrPort,
) (metainforequester.Response, error) {
	type dialResult struct {
		res metainforequester.Response
		err error
	}

	dialCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan dialResult, len(peers))
	next := 0
	inflight := 0

	var errs []error

	for {
		for inflight < metaInfoParallelDials && next < len(peers) {
			p := peers[next]
			next++
			inflight++

			go func() {
				prev := "new"
				if c.deadPeers.test(peerKey(p)) {
					prev = "dead"
				}

				pinged := make(chan string, 1)

				go func() {
					pingCtx, pingCancel := context.WithTimeout(ctx, peerProbeTimeout)
					defer pingCancel()

					if _, pingErr := c.client.Ping(pingCtx, p); pingErr != nil {
						pinged <- "fail"
					} else {
						pinged <- "ok"
					}
				}()

				res, err := c.metainfoRequester.Request(dialCtx, hash, p)
				outcome := dialOutcome(err)

				if outcome == "timeout" || outcome == "refused" || outcome == "unreachable" {
					c.deadPeers.add(peerKey(p))
				}

				if outcome != "skipped" {
					c.probeTotal.With(prometheus.Labels{"prev": prev, "ping": <-pinged, "dial": outcome}).Inc()
				}

				results <- dialResult{res: res, err: err}
			}()
		}

		if inflight == 0 {
			break
		}

		r := <-results
		inflight--

		result := classifyDialError(r.err)
		if r.err == nil && r.res.Encrypted {
			result = "ok_mse"
		}

		c.dialsTotal.With(prometheus.Labels{"source": source, "result": result}).Inc()

		if r.err != nil {
			errs = append(errs, r.err)

			continue
		}

		if banErr := c.banningChecker.Check(r.res.Info); banErr != nil {
			_ = c.blockingManager.Block(ctx, []protocol.ID{hash}, false)

			return metainforequester.Response{}, errors.Join(errBanned, banErr)
		}

		return r.res, nil
	}

	return metainforequester.Response{}, errors.Join(errs...)
}

const peerProbeTimeout = 1500 * time.Millisecond

func peerKey(p netip.AddrPort) protocol.ID {
	var id protocol.ID

	b, _ := p.MarshalBinary()
	copy(id[:], b)

	return id
}

func dialOutcome(err error) string {
	switch result := classifyDialError(err); result {
	case "dial_timeout":
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
