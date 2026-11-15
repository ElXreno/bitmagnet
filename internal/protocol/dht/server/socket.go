package server

import (
	"errors"
	"net/netip"
)

// ErrSendBufferFull reports a datagram the kernel had no room for; it is lost like any other dropped packet.
var ErrSendBufferFull = errors.New("socket send buffer is full")

type Socket interface {
	Open(localAddr netip.AddrPort) error
	Close() error
	Send(netip.AddrPort, []byte) error
	Receive([]byte) (int, netip.AddrPort, error)
}

func NewSocket() Socket {
	return newSocket()
}
