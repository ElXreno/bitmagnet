package metainforequester

import (
	"github.com/anacrolix/utp"
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
	Receiver            Receiver
	RequestDuration     prometheus.Collector `group:"prometheus_collectors"`
	RequestSuccessTotal prometheus.Collector `group:"prometheus_collectors"`
	RequestErrorTotal   prometheus.Collector `group:"prometheus_collectors"`
	RequestConcurrency  prometheus.Collector `group:"prometheus_collectors"`
	LimiterWaitSeconds  prometheus.Collector `group:"prometheus_collectors"`
	LimiterResultTotal  prometheus.Collector `group:"prometheus_collectors"`
	PeerClientTotal     prometheus.Collector `group:"prometheus_collectors"`
	DialectDialTotal    prometheus.Collector `group:"prometheus_collectors"`
	PeerBytesTotal      prometheus.Collector `group:"prometheus_collectors"`
	PiecesFailedTotal   prometheus.Collector `group:"prometheus_collectors"`
	PeerExtensionTotal  prometheus.Collector `group:"prometheus_collectors"`
}

func New(p Params) Result {
	var utpSocket *utp.Socket

	if os.Getenv("BITMAGNET_UTP") != "off" {
		if socket, err := utp.NewSocket("udp4", ":0"); err == nil {
			utpSocket = socket
		} else {
			p.Logger.Warnf("uTP is unavailable: %s", err.Error())
		}
	}

	clientID := protocol.RandomPeerID()

	collector := newPrometheusCollector(requester{
		utp:      utpSocket,
		clientID: clientID,
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
		Receiver:            NewReceiver(clientID, incomingTimeoutFromEnv()),
		RequestDuration:     collector.requestDuration,
		RequestSuccessTotal: collector.requestSuccessTotal,
		RequestErrorTotal:   collector.requestErrorTotal,
		RequestConcurrency:  collector.requestConcurrency,
		LimiterWaitSeconds:  limiterWaitSeconds,
		LimiterResultTotal:  limiterResultTotal,
		PeerClientTotal:     peerClientTotal,
		DialectDialTotal:    dialectDialTotal,
		PeerBytesTotal:      peerBytesTotal,
		PiecesFailedTotal:   piecesFailedTotal,
		PeerExtensionTotal:  peerExtensionTotal,
	}
}

func incomingTimeoutFromEnv() time.Duration {
	if seconds, err := strconv.Atoi(os.Getenv("BITMAGNET_INBOUND_FETCH_SECONDS")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}

	return IncomingTimeout
}

func dialTimeout() time.Duration {
	if ms, err := strconv.Atoi(os.Getenv("BITMAGNET_DIAL_TIMEOUT_MS")); err == nil && ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}

	return 2 * time.Second
}
