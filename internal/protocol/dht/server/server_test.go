package server

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"golang.org/x/time/rate"
)

type fakeSocket struct {
	openErr error
	mu      sync.Mutex
	sent    []netip.AddrPort
	closed  chan struct{}
	once    sync.Once
}

func newFakeSocket(openErr error) *fakeSocket {
	return &fakeSocket{openErr: openErr, closed: make(chan struct{})}
}

func (f *fakeSocket) Open(netip.AddrPort) error {
	return f.openErr
}

func (f *fakeSocket) Close() error {
	f.once.Do(func() { close(f.closed) })

	return nil
}

func (f *fakeSocket) Send(addr netip.AddrPort, _ []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.sent = append(f.sent, addr)

	return nil
}

func (f *fakeSocket) Receive([]byte) (int, netip.AddrPort, error) {
	<-f.closed

	return 0, netip.AddrPort{}, errors.New("closed")
}

func (f *fakeSocket) sentTo() []netip.AddrPort {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]netip.AddrPort(nil), f.sent...)
}

type fixedResponder struct {
	id   protocol.ID
	mu   sync.Mutex
	from []netip.AddrPort
}

func (r *fixedResponder) Respond(_ context.Context, msg dht.RecvMsg) (dht.Return, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.from = append(r.from, msg.From)

	return dht.Return{ID: r.id}, nil
}

func (r *fixedResponder) seen() []netip.AddrPort {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]netip.AddrPort(nil), r.from...)
}

func newTestServer(socket Socket, socket6 Socket, logger *zap.SugaredLogger) *server {
	return &server{
		stopped:          make(chan struct{}),
		socket:           socket,
		socket6:          socket6,
		queries:          make(map[string]chan dht.RecvMsg),
		queryTimeout:     20 * time.Millisecond,
		responder:        &fixedResponder{},
		responderTimeout: time.Second,
		idIssuer:         &variantIDIssuer{},
		logger:           logger,
	}
}

func TestQueryPicksSocketByFamily(t *testing.T) {
	t.Parallel()

	socket4, socket6 := newFakeSocket(nil), newFakeSocket(nil)
	s := newTestServer(socket4, socket6, zap.NewNop().Sugar())

	require.NoError(t, s.start())
	defer s.stop()

	require.True(t, s.IPv6())

	v4 := netip.MustParseAddrPort("192.0.2.1:6881")
	v6 := netip.MustParseAddrPort("[2001:db8::1]:6881")
	mapped := netip.MustParseAddrPort("[::ffff:192.0.2.2]:6881")

	for _, addr := range []netip.AddrPort{v4, v6, mapped} {
		_, err := s.Query(context.Background(), addr, dht.QPing, dht.MsgArgs{})
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}

	assert.Equal(t, []netip.AddrPort{v4, netip.MustParseAddrPort("192.0.2.2:6881")}, socket4.sentTo())
	assert.Equal(t, []netip.AddrPort{v6}, socket6.sentTo())

	_, err := s.Query(context.Background(), netip.AddrPort{}, dht.QPing, dht.MsgArgs{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Len(t, socket4.sentTo(), 3)
	assert.Len(t, socket6.sentTo(), 1)
}

func TestWithoutIPv6SocketOnlyIPv4IsSent(t *testing.T) {
	t.Parallel()

	socket4 := newFakeSocket(nil)
	s := newTestServer(socket4, nil, zap.NewNop().Sugar())

	require.NoError(t, s.start())
	defer s.stop()

	require.False(t, s.IPv6())

	_, err := s.Query(context.Background(), netip.MustParseAddrPort("[2001:db8::1]:6881"), dht.QPing, dht.MsgArgs{})
	require.ErrorIs(t, err, ErrNoIPv6)
	assert.Empty(t, socket4.sentTo())

	_, err = s.Query(context.Background(), netip.MustParseAddrPort("192.0.2.1:6881"), dht.QPing, dht.MsgArgs{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Len(t, socket4.sentTo(), 1)
}

func TestIPv6OpenFailureFallsBackToIPv4(t *testing.T) {
	t.Parallel()

	core, logs := observer.New(zapcore.WarnLevel)
	socket4, socket6 := newFakeSocket(nil), newFakeSocket(errors.New("address family not supported"))
	s := newTestServer(socket4, socket6, zap.New(core).Sugar())

	require.NoError(t, s.start())
	defer s.stop()

	assert.False(t, s.IPv6())
	assert.Equal(t, 1, logs.Len())
	assert.Equal(t, zapcore.WarnLevel, logs.All()[0].Level)

	_, err := s.Query(context.Background(), netip.MustParseAddrPort("[2001:db8::1]:6881"), dht.QPing, dht.MsgArgs{})
	require.ErrorIs(t, err, ErrNoIPv6)

	_, err = s.Query(context.Background(), netip.MustParseAddrPort("192.0.2.1:6881"), dht.QPing, dht.MsgArgs{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Len(t, socket4.sentTo(), 1)
	assert.Empty(t, socket6.sentTo())
	assert.Equal(t, 1, logs.Len())
}

func TestIPv4OpenFailureStillFailsTheStart(t *testing.T) {
	t.Parallel()

	s := newTestServer(newFakeSocket(errors.New("address in use")), newFakeSocket(nil), zap.NewNop().Sugar())

	require.Error(t, s.start())
}

func TestIPv6Switch(t *testing.T) {
	params := Params{Config: NewDefaultConfig(), Logger: zap.NewNop().Sugar()}

	t.Setenv(dht.IPv6Env, "off")

	off := newBaseServer(params)
	assert.Nil(t, off.socket6)
	assert.False(t, off.IPv6())
	assert.Equal(t, netip.AddrPortFrom(netip.IPv4Unspecified(), params.Config.Port), off.localAddr)

	t.Setenv(dht.IPv6Env, "")

	on := newBaseServer(params)
	assert.NotNil(t, on.socket6)
	assert.Equal(t, netip.AddrPortFrom(netip.IPv4Unspecified(), params.Config.Port), on.localAddr)
	assert.Equal(t, netip.AddrPortFrom(netip.IPv6Unspecified(), params.Config.Port), on.localAddr6)
}

func loopbackServer(t *testing.T, responder *fixedResponder) (*server, uint16) {
	t.Helper()

	probe, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)

	port := probe.LocalAddr().(*net.UDPAddr).AddrPort().Port()

	require.NoError(t, probe.Close())

	s := newTestServer(NewSocket(), NewSocket(), zap.NewNop().Sugar())
	s.localAddr = netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port)
	s.localAddr6 = netip.AddrPortFrom(netip.IPv6Loopback(), port)
	s.queryTimeout = 2 * time.Second
	s.responder = responder

	require.NoError(t, s.start())
	t.Cleanup(s.stop)

	return s, port
}

func TestDualStackOverLoopback(t *testing.T) {
	t.Parallel()

	responder := &fixedResponder{id: protocol.RandomNodeID()}
	a, portA := loopbackServer(t, responder)
	b, portB := loopbackServer(t, &fixedResponder{})

	if !a.IPv6() || !b.IPv6() {
		t.Skip("no IPv6 loopback on this host")
	}

	target4 := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), portA)
	target6 := netip.AddrPortFrom(netip.IPv6Loopback(), portA)

	res4, err := b.Query(context.Background(), target4, dht.QPing, dht.MsgArgs{})
	require.NoError(t, err)
	assert.Equal(t, target4, res4.From)
	assert.Equal(t, responder.id, res4.Msg.R.ID)

	res6, err := b.Query(context.Background(), target6, dht.QPing, dht.MsgArgs{})
	require.NoError(t, err)
	assert.Equal(t, target6, res6.From)
	assert.Equal(t, responder.id, res6.Msg.R.ID)

	assert.Equal(t, []netip.AddrPort{
		netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), portB),
		netip.AddrPortFrom(netip.IPv6Loopback(), portB),
	}, responder.seen())
}

func familyCounts(t *testing.T, collector prometheus.Collector) map[string]float64 {
	t.Helper()

	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(collector))

	families, err := registry.Gather()
	require.NoError(t, err)

	counts := make(map[string]float64)

	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == labelFamily {
					counts[label.GetValue()] += metric.GetCounter().GetValue()
				}
			}
		}
	}

	return counts
}

func TestWrappedServerCountsQueriesByFamily(t *testing.T) {
	t.Parallel()

	collector := newPrometheusCollector()
	wrapped := queryLimiter{
		server: prometheusServerWrapper{
			prometheusCollector: collector,
			server: healthCollector{
				baseServer:    newTestServer(newFakeSocket(nil), newFakeSocket(nil), zap.NewNop().Sugar()),
				lastResponses: &concurrency.AtomicValue[LastResponses]{},
			},
		},
		queryLimiter: concurrency.NewKeyedLimiter(rate.Every(time.Second), 4, 100, time.Second),
	}

	require.NoError(t, wrapped.start())
	defer wrapped.stop()

	require.True(t, wrapped.IPv6())

	for _, addr := range []string{"192.0.2.1:6881", "[2001:db8::1]:6881", "[2001:db8::2]:6881"} {
		_, err := wrapped.Query(context.Background(), netip.MustParseAddrPort(addr), dht.QPing, dht.MsgArgs{})
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}

	want := map[string]float64{dht.FamilyIPv4: 1, dht.FamilyIPv6: 2}
	assert.Equal(t, want, familyCounts(t, collector.queryErrorTotal))

	limited := familyCounts(t, limiterWaitTotal)
	assert.GreaterOrEqual(t, limited[dht.FamilyIPv4], 1.0)
	assert.GreaterOrEqual(t, limited[dht.FamilyIPv6], 2.0)
}
