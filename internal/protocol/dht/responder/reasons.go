package responder

import (
	"errors"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/prometheus/client_golang/prometheus"
)

var errorReasonTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "query_error_reason_total",
	Help:      "A counter of failed DHT queries by reason.",
}, []string{labelQuery, "reason"})

var limiterRefusedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "limiter_refused_total",
	Help:      "A counter of DHT queries refused by the rate limiter.",
}, []string{"scope"})

var inboundHashTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "inbound_hash_total",
	Help:      "A counter of info hashes seen in incoming queries.",
}, []string{labelQuery, "result"})

func errorReason(err error) string {
	var dhtErr dht.Error
	if !errors.As(err, &dhtErr) {
		return "other"
	}

	switch dhtErr.Msg {
	case ErrTooManyRequests.Msg:
		return "too_many_requests"
	case ErrMissingArguments.Msg:
		return "missing_arguments"
	case ErrInvalidToken.Msg:
		return "invalid_token"
	case ErrMethodUnknown.Msg:
		return "method_unknown"
	default:
		return "other"
	}
}
