package server

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"strings"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/responder"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type Params struct {
	fx.In
	Config     Config
	Responder  responder.Responder
	Logger     *zap.SugaredLogger
	SaveNodeID func() `name:"dht_node_id_save" optional:"true"`
}

type Result struct {
	fx.Out
	Server            lazy.Lazy[Server]
	LastResponses     *concurrency.AtomicValue[LastResponses] `name:"dht_server_last_responses"`
	AppHook           fx.Hook                                 `group:"app_hooks"`
	QueryDuration     prometheus.Collector                    `group:"prometheus_collectors"`
	QuerySuccessTotal prometheus.Collector                    `group:"prometheus_collectors"`
	QueryErrorTotal   prometheus.Collector                    `group:"prometheus_collectors"`
	QueryConcurrency  prometheus.Collector                    `group:"prometheus_collectors"`
	ExitPackets       prometheus.Collector                    `group:"prometheus_collectors"`
	NodeClientTotal   prometheus.Collector                    `group:"prometheus_collectors"`
	LimiterWait       prometheus.Collector                    `group:"prometheus_collectors"`
	LimiterWaitTotal  prometheus.Collector                    `group:"prometheus_collectors"`
	LimiterWaitErrors prometheus.Collector                    `group:"prometheus_collectors"`
}

const (
	namespace = "bitmagnet"
	subsystem = "dht_server"
)

// InterfacesEnv lists the network interfaces that each get a socket of their own, comma separated.
const InterfacesEnv = "BITMAGNET_DHT_INTERFACES"

func interfacesFromEnv() []string {
	var devices []string

	for _, device := range strings.Split(os.Getenv(InterfacesEnv), ",") {
		if device = strings.TrimSpace(device); device != "" {
			devices = append(devices, device)
		}
	}

	if len(devices) == 0 {
		return []string{""}
	}

	return devices
}

func newExitPackets() *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "exit_packets_total",
		Help:      "DHT datagrams by the network interface they left or arrived through.",
	}, []string{"exit", "family", "direction"})
}

type countedSocket struct {
	Socket
	sent     prometheus.Counter
	received prometheus.Counter
}

func (c countedSocket) Send(addr netip.AddrPort, data []byte) error {
	err := c.Socket.Send(addr, data)
	if err == nil {
		c.sent.Inc()
	}

	return err
}

func (c countedSocket) Receive(data []byte) (int, netip.AddrPort, error) {
	n, from, err := c.Socket.Receive(data)
	if err == nil && n > 0 {
		c.received.Inc()
	}

	return n, from, err
}

func exitSockets(devices []string, family string, packets *prometheus.CounterVec) []Socket {
	sockets := make([]Socket, 0, len(devices))

	for _, device := range devices {
		socket := NewSocketOn(device)

		if packets != nil {
			exit := device
			if exit == "" {
				exit = "default"
			}

			socket = countedSocket{
				Socket:   socket,
				sent:     packets.WithLabelValues(exit, family, "sent"),
				received: packets.WithLabelValues(exit, family, "received"),
			}
		}

		sockets = append(sockets, socket)
	}

	return sockets
}

func newBaseServer(p Params, packets *prometheus.CounterVec) *server {
	devices := interfacesFromEnv()
	s := &server{
		stopped: make(chan struct{}),
		localAddr: netip.AddrPortFrom(
			netip.IPv4Unspecified(),
			p.Config.Port,
		),
		sockets:          exitSockets(devices, dht.FamilyIPv4, packets),
		queries:          make(map[string]chan dht.RecvMsg),
		queryTimeout:     p.Config.QueryTimeout,
		responder:        p.Responder,
		responderTimeout: time.Second * 5,
		idIssuer:         &variantIDIssuer{},
		logger:           p.Logger.Named(subsystem),
	}

	if dht.IPv6Enabled() {
		s.localAddr6 = netip.AddrPortFrom(netip.IPv6Unspecified(), p.Config.Port)
		s.sockets6 = exitSockets(devices, dht.FamilyIPv6, packets)
	}

	return s
}

func New(p Params) Result {
	lastResponses := &concurrency.AtomicValue[LastResponses]{}
	collector := newPrometheusCollector()
	exitPackets := newExitPackets()
	ls := lazy.New(func() (Server, error) {
		s := queryLimiter{
			server: prometheusServerWrapper{
				prometheusCollector: collector,
				server: healthCollector{
					baseServer:    newBaseServer(p, exitPackets),
					lastResponses: lastResponses,
				},
			},
			queryLimiter: concurrency.NewKeyedLimiter(rate.Every(time.Second/3), 4, 100_000, time.Second*20),
		}
		if err := s.start(); err != nil {
			return nil, fmt.Errorf("could not start server: %w", err)
		}

		if p.SaveNodeID != nil {
			p.SaveNodeID()
		}

		return s, nil
	})

	return Result{
		Server: ls,
		AppHook: fx.Hook{
			OnStop: func(context.Context) error {
				return ls.IfInitialized(func(s Server) error {
					s.stop()
					return nil
				})
			},
		},
		LastResponses:     lastResponses,
		QueryDuration:     collector.queryDuration,
		QuerySuccessTotal: collector.querySuccessTotal,
		QueryErrorTotal:   collector.queryErrorTotal,
		QueryConcurrency:  collector.queryConcurrency,
		ExitPackets:       exitPackets,
		NodeClientTotal:   collector.nodeClientTotal,
		LimiterWait:       limiterWaitSeconds,
		LimiterWaitTotal:  limiterWaitTotal,
		LimiterWaitErrors: limiterWaitErrors,
	}
}
