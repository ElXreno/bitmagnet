//go:build !windows

package server

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func socketOption(t *testing.T, fd int, level int, option int) int {
	t.Helper()

	value, err := unix.GetsockoptInt(fd, level, option)
	require.NoError(t, err)

	return value
}

func TestIPv6SocketIsV6OnlyBeforeBind(t *testing.T) {
	t.Parallel()

	fd, err := newSocketFD(true)
	if err != nil {
		t.Skipf("no IPv6 sockets on this host: %s", err)
	}

	defer func() { _ = unix.Close(fd) }()

	assert.Equal(t, 1, socketOption(t, fd, unix.IPPROTO_IPV6, unix.IPV6_V6ONLY))
	assert.Equal(t, unix.AF_INET6, socketOption(t, fd, unix.SOL_SOCKET, unix.SO_DOMAIN))
	assert.Equal(t, unix.SOCK_DGRAM, socketOption(t, fd, unix.SOL_SOCKET, unix.SO_TYPE))
}

func TestIPv4SocketFamily(t *testing.T) {
	t.Parallel()

	fd, err := newSocketFD(false)
	require.NoError(t, err)

	defer func() { _ = unix.Close(fd) }()

	assert.Equal(t, unix.AF_INET, socketOption(t, fd, unix.SOL_SOCKET, unix.SO_DOMAIN))
	assert.Equal(t, unix.SOCK_DGRAM, socketOption(t, fd, unix.SOL_SOCKET, unix.SO_TYPE))
}

func TestSocketOpenByFamily(t *testing.T) {
	t.Parallel()

	unopened, ok := newSocket("").(*socket)
	require.True(t, ok)
	require.NoError(t, unopened.Close())

	s4, ok := newSocket("").(*socket)
	require.True(t, ok)
	require.NoError(t, s4.Open(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), 0)))

	defer func() { _ = s4.Close() }()

	assert.Equal(t, unix.AF_INET, socketOption(t, s4.fd, unix.SOL_SOCKET, unix.SO_DOMAIN))

	s6, ok := newSocket("").(*socket)
	require.True(t, ok)

	if err := s6.Open(netip.AddrPortFrom(netip.IPv6Loopback(), 0)); err != nil {
		t.Skipf("no IPv6 loopback on this host: %s", err)
	}

	defer func() { _ = s6.Close() }()

	assert.Equal(t, unix.AF_INET6, socketOption(t, s6.fd, unix.SOL_SOCKET, unix.SO_DOMAIN))
}

func TestSocketBoundToAnInterface(t *testing.T) {
	t.Parallel()

	bound, ok := newSocket("lo").(*socket)
	require.True(t, ok)

	if err := bound.Open(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), 0)); err != nil {
		t.Skipf("this host does not let the test bind to lo: %s", err)
	}

	defer func() { _ = bound.Close() }()

	device, err := unix.GetsockoptString(bound.fd, unix.SOL_SOCKET, unix.SO_BINDTODEVICE)
	require.NoError(t, err)
	assert.Equal(t, "lo", device)

	missing := newSocket("no-such-dev0")
	require.Error(t, missing.Open(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), 0)))
}
