package metainforequester

import (
	"context"
	"errors"
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
