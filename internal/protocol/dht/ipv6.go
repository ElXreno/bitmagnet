package dht

import (
	"net/netip"
	"os"
)

const (
	IPv6Env    = "BITMAGNET_DHT_IPV6"
	FamilyIPv4 = "ipv4"
	FamilyIPv6 = "ipv6"
)

func IPv6Enabled() bool {
	return os.Getenv(IPv6Env) != "off"
}

func IsIPv6(addr netip.Addr) bool {
	return addr.Is6() && !addr.Is4In6()
}

func Family(addr netip.Addr) string {
	if IsIPv6(addr) {
		return FamilyIPv6
	}

	return FamilyIPv4
}
