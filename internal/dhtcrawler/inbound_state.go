package dhtcrawler

import (
	"encoding/hex"
	"encoding/json"
	"net/netip"
	"os"
	"path/filepath"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

const inboundStateFile = "inbound_late_hashes.json"

type savedHash struct {
	Hash    string `json:"hash"`
	Seeders uint32 `json:"seeders"`
	Source  string `json:"source,omitempty"`
	Node    string `json:"node,omitempty"`
}

func savedFrom(entry *inboundEntry) savedHash {
	saved := savedHash{Hash: hex.EncodeToString(entry.hash[:]), Seeders: entry.seeders, Source: entry.req.source}

	if entry.req.node.IsValid() {
		saved.Node = entry.req.node.String()
	}

	return saved
}

// wanted lists the held hashes, then the late ones from the newest; it needs mu
func (p *inboundPeers) wanted(held []*inboundEntry) []savedHash {
	saved := make([]savedHash, 0, len(held)+len(p.late))

	for _, entry := range held {
		if !entry.done {
			saved = append(saved, savedFrom(entry))
		}
	}

	for elem := p.lateOrder.Back(); elem != nil; elem = elem.Prev() {
		saved = append(saved, savedFrom(elem.Value.(*inboundEntry)))
	}

	return saved[:min(len(saved), max(p.config.lateMax, 0))]
}

func (p *inboundPeers) saveState(saved []savedHash) {
	if p.config.stateFile == "" || len(saved) == 0 {
		return
	}

	data, err := json.Marshal(saved)
	if err != nil {
		return
	}

	file, err := os.CreateTemp(filepath.Dir(p.config.stateFile), "tmp-*")
	if err != nil {
		p.c.logger.Warnf("could not save the hashes that wait for incoming peers: %s", err.Error())

		return
	}

	_, writeErr := file.Write(data)
	closeErr := file.Close()

	if writeErr != nil || closeErr != nil || os.Rename(file.Name(), p.config.stateFile) != nil {
		_ = os.Remove(file.Name())
	}
}

func (p *inboundPeers) loadState() {
	if p.config.stateFile == "" || p.config.lateMax <= 0 {
		return
	}

	data, err := os.ReadFile(p.config.stateFile)
	if err != nil {
		return
	}

	_ = os.Remove(p.config.stateFile)

	var saved []savedHash

	if json.Unmarshal(data, &saved) != nil {
		return
	}

	now := p.now()
	loaded := 0

	p.mu.Lock()

	for i := min(len(saved), p.config.lateMax) - 1; i >= 0; i-- {
		raw, hexErr := hex.DecodeString(saved[i].Hash)
		if hexErr != nil || len(raw) != len(protocol.ID{}) {
			continue
		}

		entry := &inboundEntry{
			hash:    protocol.ID(raw),
			seeders: saved[i].Seeders,
			since:   now,
			until:   now.Add(p.config.lateTTL),
			late:    true,
		}

		if _, known := p.late[entry.hash]; known {
			continue
		}

		entry.req = nodeHasPeersForHash{infoHash: entry.hash, source: saved[i].Source}
		entry.req.node, _ = netip.ParseAddrPort(saved[i].Node)
		entry.elem = p.lateOrder.PushBack(entry)
		p.late[entry.hash] = entry
		p.indexMSE(entry.hash)
		loaded++
	}

	p.mu.Unlock()

	p.c.countHash(sourceInboundPeer, "late_loaded", loaded)
}
