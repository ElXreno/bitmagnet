package dhtcrawler

import (
	"net/netip"
	"sync"
)

const selfPeersMax = 64

type selfPeers struct {
	mu    sync.Mutex
	addrs map[netip.AddrPort]struct{}
}

func (s *selfPeers) add(addr netip.AddrPort) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.addrs == nil {
		s.addrs = make(map[netip.AddrPort]struct{})
	}

	if len(s.addrs) < selfPeersMax {
		s.addrs[addr] = struct{}{}
	}
}

func (s *selfPeers) without(peers []netip.AddrPort) []netip.AddrPort {
	if s == nil {
		return peers
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.addrs) == 0 {
		return peers
	}

	kept := make([]netip.AddrPort, 0, len(peers))

	for _, peer := range peers {
		if _, ours := s.addrs[peer]; !ours {
			kept = append(kept, peer)
		}
	}

	return kept
}
