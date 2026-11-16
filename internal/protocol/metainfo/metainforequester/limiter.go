package metainforequester

import (
	"context"
	"errors"
	"io"
	"net/netip"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/prometheus/client_golang/prometheus"
)

const limiterMaxWait = time.Second

var ErrLimited = errors.New("per-address request limit reached")

var limiterWaitSeconds = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "limiter_wait_seconds_total",
	Help:      "Seconds meta info requests spent waiting in the per-address rate limiter.",
})

var limiterResultTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "limiter_result_total",
	Help:      "A counter of meta info requests by rate limiter outcome.",
}, []string{"result"})

type requestLimiter struct {
	requester Requester
	limiter   concurrency.KeyedLimiter
}

func (r requestLimiter) Request(ctx context.Context, infoHash protocol.ID, node netip.AddrPort) (Response, error) {
	started := time.Now()
	waitCtx, cancel := context.WithTimeout(ctx, limiterMaxWait)
	limitErr := r.limiter.Wait(waitCtx, node.Addr().String())

	cancel()

	waited := time.Since(started)
	limiterWaitSeconds.Add(waited.Seconds())

	if limitErr != nil {
		limiterResultTotal.With(prometheus.Labels{"result": "refused"}).Inc()

		return Response{}, ErrLimited
	}

	if waited > 10*time.Millisecond {
		limiterResultTotal.With(prometheus.Labels{"result": "waited"}).Inc()
	} else {
		limiterResultTotal.With(prometheus.Labels{"result": "immediate"}).Inc()
	}

	return r.requester.Request(ctx, infoHash, node)
}

var peerClientTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "peer_client_total",
	Help:      "A counter of peers by BitTorrent client: every handshake, then the one outcome it ended with.",
}, []string{"client", "stage"})

var peerBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "peer_bytes_total",
	Help:      "Bytes an incoming peer sent after its handshake, by the outcome its connection ended with; resumed and partial_* are meta info pieces kept from earlier connections: used, or dropped unused.",
}, []string{"stage"})

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)

	return n, err
}

var piecesFailedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "pieces_failed_total",
	Help:      "Incoming meta info transfers that did not finish, by reason, as connections and as the bytes they took.",
}, []string{"reason", "unit"})

var dialectDialTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "dialect_dial_total",
	Help:      "Outgoing connections retried with the other protocol string after a peer closed both handshakes.",
}, []string{"result"})

var peerExtensionTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "peer_extension_total",
	Help:      "A counter of peers by what their extended handshake offers.",
}, []string{"extension"})

func countPeerExtensions(prefix string, handshake *rootDict) {
	offered := map[string]bool{
		"any":           true,
		"ut_metadata":   handshake.M.UTMetadata > 0,
		"metadata_size": handshake.MetadataSize > 0,
		"ut_pex":        handshake.M.UTPex > 0,
		"ut_holepunch":  handshake.M.UTHolepunch > 0,
	}

	for extension, yes := range offered {
		if yes {
			peerExtensionTotal.With(prometheus.Labels{"extension": prefix + extension}).Inc()
		}
	}
}
