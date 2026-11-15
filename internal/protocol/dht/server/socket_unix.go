//go:build !windows

package server

import (
	"errors"
	"fmt"
	"net/netip"

	"golang.org/x/sys/unix"
)

func newSocket() Socket {
	return &socket{fd: -1}
}

type socket struct {
	fd int
}

func (s *socket) Open(localAddr netip.AddrPort) error {
	sAddr, addrErr := addrPortToSockaddr(localAddr)
	if addrErr != nil {
		return addrErr
	}

	fd, sockErr := newSocketFD(localAddr.Addr().Is6())
	if sockErr != nil {
		return sockErr
	}

	if bindErr := unix.Bind(fd, sAddr); bindErr != nil {
		_ = unix.Close(fd)

		return bindErr
	}

	s.fd = fd

	return nil
}

func newSocketFD(ipv6 bool) (int, error) {
	family := unix.AF_INET
	if ipv6 {
		family = unix.AF_INET6
	}

	fd, sockErr := unix.Socket(family, unix.SOCK_DGRAM, 0)
	if sockErr != nil {
		return -1, fmt.Errorf("error creating socket: %w", sockErr)
	}

	if ipv6 {
		if optErr := unix.SetsockoptInt(fd, unix.IPPROTO_IPV6, unix.IPV6_V6ONLY, 1); optErr != nil {
			_ = unix.Close(fd)

			return -1, fmt.Errorf("error setting IPV6_V6ONLY: %w", optErr)
		}
	}

	return fd, nil
}

func (s *socket) Close() error {
	if s.fd < 0 {
		return nil
	}

	return unix.Close(s.fd)
}

func (s *socket) Send(remoteAddr netip.AddrPort, data []byte) error {
	sAddr, addrErr := addrPortToSockaddr(remoteAddr)
	if addrErr != nil {
		return addrErr
	}

	return unix.Sendto(s.fd, data, 0, sAddr)
}

func (s *socket) Receive(data []byte) (int, netip.AddrPort, error) {
	n, sAddr, recvErr := unix.Recvfrom(s.fd, data, 0)
	if recvErr != nil {
		return n, netip.AddrPort{}, recvErr
	}

	addr, addrErr := sockaddrToAddrPort(sAddr)
	if addrErr != nil {
		return n, netip.AddrPort{}, addrErr
	}

	return n, addr, nil
}

func addrPortToSockaddr(addr netip.AddrPort) (unix.Sockaddr, error) {
	if addr.Addr().Is4() {
		return &unix.SockaddrInet4{
			Addr: addr.Addr().As4(),
			Port: int(addr.Port()),
		}, nil
	}

	if addr.Addr().Is6() {
		return &unix.SockaddrInet6{
			Addr: addr.Addr().As16(),
			Port: int(addr.Port()),
		}, nil
	}

	return nil, errors.New("invalid address")
}

func sockaddrToAddrPort(addr unix.Sockaddr) (netip.AddrPort, error) {
	switch addr := addr.(type) {
	case *unix.SockaddrInet4:
		return netip.AddrPortFrom(netip.AddrFrom4(addr.Addr), uint16(addr.Port)), nil
	case *unix.SockaddrInet6:
		return netip.AddrPortFrom(netip.AddrFrom16(addr.Addr), uint16(addr.Port)), nil
	default:
		return netip.AddrPort{}, fmt.Errorf("unsupported sockaddr type: %T", addr)
	}
}
