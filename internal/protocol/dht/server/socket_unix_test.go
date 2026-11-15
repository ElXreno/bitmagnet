//go:build !windows

package server

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestIPv6SocketIsV6Only(t *testing.T) {
	t.Parallel()

	s, ok := newSocket().(*socket)
	require.True(t, ok)
	require.NoError(t, s.Close())

	if err := s.Open(netip.AddrPortFrom(netip.IPv6Loopback(), 0)); err != nil {
		t.Skipf("no IPv6 loopback on this host: %s", err)
	}

	defer func() { _ = s.Close() }()

	v6only, err := unix.GetsockoptInt(s.fd, unix.IPPROTO_IPV6, unix.IPV6_V6ONLY)
	require.NoError(t, err)
	assert.Equal(t, 1, v6only)

	domain, err := unix.GetsockoptInt(s.fd, unix.SOL_SOCKET, unix.SO_DOMAIN)
	require.NoError(t, err)
	assert.Equal(t, unix.AF_INET6, domain)
}

func TestIPv4SocketFamily(t *testing.T) {
	t.Parallel()

	s, ok := newSocket().(*socket)
	require.True(t, ok)
	require.NoError(t, s.Open(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), 0)))

	defer func() { _ = s.Close() }()

	domain, err := unix.GetsockoptInt(s.fd, unix.SOL_SOCKET, unix.SO_DOMAIN)
	require.NoError(t, err)
	assert.Equal(t, unix.AF_INET, domain)

	kind, err := unix.GetsockoptInt(s.fd, unix.SOL_SOCKET, unix.SO_TYPE)
	require.NoError(t, err)
	assert.Equal(t, unix.SOCK_DGRAM, kind)
}
