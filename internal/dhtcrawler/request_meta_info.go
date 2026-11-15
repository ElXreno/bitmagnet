package dhtcrawler

import (
	"context"
	"errors"
	"net/netip"
	"sync"

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

func (c *crawler) doRequestMetaInfo(
	ctx context.Context,
	source string,
	hash protocol.ID,
	peers []netip.AddrPort,
) (metainforequester.Response, error) {
	var errs []error

	errsMutex := sync.Mutex{}
	addErr := func(err error) {
		errsMutex.Lock()
		errs = append(errs, err)
		errsMutex.Unlock()
	}

	for _, p := range peers {
		res, err := c.metainfoRequester.Request(ctx, hash, p)
		result := classifyDialError(err)
		if err == nil && res.Encrypted {
			result = "ok_mse"
		}

		c.dialsTotal.With(prometheus.Labels{"source": source, "result": result}).Inc()
		if err != nil {
			addErr(err)
			continue
		}

		if banErr := c.banningChecker.Check(res.Info); banErr != nil {
			_ = c.blockingManager.Block(ctx, []protocol.ID{hash}, false)
			return metainforequester.Response{}, errors.Join(errBanned, banErr)
		}

		return res, nil
	}

	return metainforequester.Response{}, errors.Join(errs...)
}
