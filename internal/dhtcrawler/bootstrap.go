package dhtcrawler

import (
	"context"
	"net"
	"net/netip"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

func (c *crawler) reseedBootstrapNodes(ctx context.Context) {
	interval := time.Duration(0)

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			for _, strAddr := range c.bootstrapNodes {
				addr, err := net.ResolveUDPAddr("udp", strAddr)
				if err != nil {
					c.logger.Warnf("failed to resolve bootstrap node address: %s", err)
					continue
				}

				addrPort := netip.AddrPortFrom(addr.AddrPort().Addr().Unmap(), addr.AddrPort().Port())
				addrPorts := append([]netip.AddrPort{addrPort}, c.bootstrapAddrs6(ctx, strAddr)...)

				for _, addrPort := range addrPorts {
					select {
					case <-ctx.Done():
						return
					case c.family(addrPort.Addr()).nodesForPing.In() <- ktable.NewNode(ktable.ID{}, addrPort):
					}
				}
			}
		}

		interval = c.reseedBootstrapNodesInterval
	}
}

func (c *crawler) bootstrapAddrs6(ctx context.Context, strAddr string) []netip.AddrPort {
	if c.v6 == nil {
		return nil
	}

	host, strPort, err := net.SplitHostPort(strAddr)
	if err != nil {
		return nil
	}

	port, err := net.DefaultResolver.LookupPort(ctx, "udp", strPort)
	if err != nil {
		return nil
	}

	addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip6", host)
	if err != nil {
		c.logger.Debugf("no IPv6 address for bootstrap node %s: %s", strAddr, err)

		return nil
	}

	addrPorts := make([]netip.AddrPort, 0, len(addrs))

	for _, addr := range addrs {
		if dht.IsIPv6(addr) {
			addrPorts = append(addrPorts, netip.AddrPortFrom(addr.WithZone(""), uint16(port)))
		}
	}

	return addrPorts
}
