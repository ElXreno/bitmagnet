package server

import (
	"time"

	"context"
	"github.com/prometheus/client_golang/prometheus"
	"net/netip"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
)

type queryLimiter struct {
	server       Server
	queryLimiter concurrency.KeyedLimiter
}

func (s queryLimiter) start() error {
	return s.server.start()
}

func (s queryLimiter) stop() {
	s.server.stop()
}

func (s queryLimiter) Query(
	ctx context.Context,
	addr netip.AddrPort,
	q string,
	args dht.MsgArgs,
) (r dht.RecvMsg, err error) {
	started := time.Now()
	waitCtx := ctx

	if maxWait, ok := ctx.Value(maxLimiterWaitKey{}).(time.Duration); ok {
		var cancel context.CancelFunc

		waitCtx, cancel = context.WithTimeout(ctx, maxWait)
		defer cancel()
	}

	limitErr := s.queryLimiter.Wait(waitCtx, addr.Addr().String())
	labels := prometheus.Labels{"query": q}

	limiterWaitSeconds.With(labels).Add(time.Since(started).Seconds())
	limiterWaitTotal.With(labels).Inc()

	if limitErr != nil {
		limiterWaitErrors.With(labels).Inc()

		return r, limitErr
	}

	return s.server.Query(ctx, addr, q, args)
}

var limiterWaitSeconds = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "limiter_wait_seconds_total",
	Help:      "Seconds outgoing DHT queries spent waiting in the per-address rate limiter.",
}, []string{"query"})

var limiterWaitTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "limiter_wait_total",
	Help:      "A counter of outgoing DHT queries that passed through the rate limiter.",
}, []string{"query"})

var limiterWaitErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "limiter_wait_error_total",
	Help:      "A counter of outgoing DHT queries abandoned while waiting in the rate limiter.",
}, []string{"query"})

type maxLimiterWaitKey struct{}

// WithMaxLimiterWait makes queries sent with the returned context fail fast when the per-address limiter
// would hold them for longer than d.
func WithMaxLimiterWait(ctx context.Context, d time.Duration) context.Context {
	return context.WithValue(ctx, maxLimiterWaitKey{}, d)
}
