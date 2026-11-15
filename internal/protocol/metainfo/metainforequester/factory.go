package metainforequester

import (
	"net"
	"os"
	"strconv"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"
)

type Params struct {
	fx.In
	Config Config
	Logger *zap.SugaredLogger
}

type Result struct {
	fx.Out
	Requester           Requester
	RequestDuration     prometheus.Collector `group:"prometheus_collectors"`
	RequestSuccessTotal prometheus.Collector `group:"prometheus_collectors"`
	RequestErrorTotal   prometheus.Collector `group:"prometheus_collectors"`
	RequestConcurrency  prometheus.Collector `group:"prometheus_collectors"`
	LimiterWaitSeconds  prometheus.Collector `group:"prometheus_collectors"`
	LimiterResultTotal  prometheus.Collector `group:"prometheus_collectors"`
}

func New(p Params) Result {
	collector := newPrometheusCollector(requester{
		clientID: protocol.RandomPeerID(),
		timeout:  p.Config.RequestTimeout,
		dialer: &net.Dialer{
			Timeout:   dialTimeout(),
			KeepAlive: -1,
		},
	})

	return Result{
		Requester: requestLimiter{
			requester: requestLogger{
				requester: collector,
				// we make way to many requests to usefully log everything, but having a sample is
				// helpful:
				logger: p.Logger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
					return zapcore.NewSamplerWithOptions(core, time.Minute, 10, 0)
				})).Named("meta_info_requester"),
			},
			limiter: concurrency.NewKeyedLimiter(rate.Every(time.Second/8), 8, 100_000, time.Second*20),
		},
		RequestDuration:     collector.requestDuration,
		RequestSuccessTotal: collector.requestSuccessTotal,
		RequestErrorTotal:   collector.requestErrorTotal,
		RequestConcurrency:  collector.requestConcurrency,
		LimiterWaitSeconds:  limiterWaitSeconds,
		LimiterResultTotal:  limiterResultTotal,
	}
}

func dialTimeout() time.Duration {
	if ms, err := strconv.Atoi(os.Getenv("BITMAGNET_DIAL_TIMEOUT_MS")); err == nil && ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}

	return 2 * time.Second
}
