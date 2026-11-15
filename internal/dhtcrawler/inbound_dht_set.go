package dhtcrawler

import (
	"bytes"
	"encoding/binary"
	"net/netip"
	"os"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

const (
	dhtHeldMagic      = "BMDHT02\n"
	dhtHeldRecordSize = 20 + 4 + 1 + 1 + 18
)

const (
	dhtClassNone uint8 = iota
	dhtClassSeeded
	dhtClassLeechers
	dhtClassEmpty
	dhtClassUnknown
)

var dhtClassNames = [...]string{"", "_seeded", "_leechers", "_empty", "_unknown"}

var dhtHeldOrigins = [...]string{"", sourceSample, sourceSample6, sourceSampleLookup, sourceSampleLookup6}

// dhtHeld has no pointers: 300,000 of them are one 14 MB block the garbage collector never scans
type dhtHeld struct {
	hash   protocol.ID
	node   [18]byte
	since  uint32
	origin uint8
	active uint8
	live   bool
	class  uint8
}

func newDHTHeld(req nodeHasPeersForHash, since uint32) dhtHeld {
	held := dhtHeld{hash: req.infoHash, since: since, live: true}

	for code, source := range dhtHeldOrigins {
		if code > 0 && source == req.source {
			held.origin = uint8(code)
		}
	}

	if req.node.IsValid() {
		ip := req.node.Addr().As16()
		copy(held.node[:16], ip[:])
		binary.BigEndian.PutUint16(held.node[16:], req.node.Port())
	}

	return held
}

func (h *dhtHeld) request() nodeHasPeersForHash {
	req := nodeHasPeersForHash{infoHash: h.hash, source: dhtHeldOrigins[h.origin]}

	if h.node != ([18]byte{}) {
		req.node = netip.AddrPortFrom(
			netip.AddrFrom16([16]byte(h.node[:16])).Unmap(), binary.BigEndian.Uint16(h.node[16:]))
	}

	return req
}

// dhtHeldSet is a ring in arrival order with an index: O(1), oldest out first
type dhtHeldSet struct {
	onAdd func(protocol.ID)
	slots []dhtHeld
	head  int
	used  int
	ttl   uint32
	index map[protocol.ID]uint32
}

func newDHTHeldSet(capacity int, ttl time.Duration) *dhtHeldSet {
	return &dhtHeldSet{
		slots: make([]dhtHeld, capacity),
		ttl:   uint32(ttl / time.Second),
		index: make(map[protocol.ID]uint32),
	}
}

func (s *dhtHeldSet) size() int {
	if s == nil {
		return 0
	}

	return len(s.index)
}

func (s *dhtHeldSet) get(hash protocol.ID) *dhtHeld {
	if s == nil {
		return nil
	}

	slot, ok := s.index[hash]
	if !ok {
		return nil
	}

	return &s.slots[slot]
}

func (s *dhtHeldSet) pop() bool {
	oldest := &s.slots[s.head]
	live := oldest.live

	if live {
		delete(s.index, oldest.hash)
	}

	*oldest = dhtHeld{}
	s.head = (s.head + 1) % len(s.slots)
	s.used--

	return live
}

func (s *dhtHeldSet) add(held dhtHeld) (added bool, evicted bool) {
	if _, known := s.index[held.hash]; known || held.since+s.ttl < held.since {
		return false, false
	}

	if s.used == len(s.slots) {
		evicted = s.pop()
	}

	slot := (s.head + s.used) % len(s.slots)
	s.slots[slot] = held
	s.index[held.hash] = uint32(slot)
	s.used++

	if s.onAdd != nil {
		s.onAdd(held.hash)
	}

	return true, evicted
}

func (s *dhtHeldSet) remove(hash protocol.ID) bool {
	if s == nil {
		return false
	}

	slot, ok := s.index[hash]
	if ok {
		s.slots[slot].live = false
		delete(s.index, hash)
	}

	return ok
}

func (s *dhtHeldSet) expire(now uint32) int {
	if s == nil {
		return 0
	}

	expired := 0

	for s.used > 0 {
		oldest := &s.slots[s.head]
		if oldest.live && oldest.since+s.ttl > now {
			break
		}

		if s.pop() {
			expired++
		}
	}

	return expired
}

func (s *dhtHeldSet) encode() []byte {
	data := make([]byte, 0, len(dhtHeldMagic)+len(s.index)*dhtHeldRecordSize)
	data = append(data, dhtHeldMagic...)

	for n := range s.used {
		held := &s.slots[(s.head+n)%len(s.slots)]
		if !held.live {
			continue
		}

		data = append(data, held.hash[:]...)
		data = binary.BigEndian.AppendUint32(data, held.since)
		data = append(data, held.origin, held.class)
		data = append(data, held.node[:]...)
	}

	return data
}

func (s *dhtHeldSet) load(path string, now uint32) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}

	_ = os.Remove(path)

	records, ok := bytes.CutPrefix(data, []byte(dhtHeldMagic))
	if !ok || len(records)%dhtHeldRecordSize != 0 {
		return 0
	}

	loaded := 0

	for ; len(records) > 0; records = records[dhtHeldRecordSize:] {
		held := dhtHeld{
			hash:   protocol.ID(records[:20]),
			since:  binary.BigEndian.Uint32(records[20:24]),
			origin: records[24],
			class:  records[25],
			live:   true,
		}
		copy(held.node[:], records[26:])

		held.since = min(held.since, now)

		if int(held.origin) >= len(dhtHeldOrigins) || int(held.class) >= len(dhtClassNames) ||
			held.since+s.ttl <= now {
			continue
		}

		if added, _ := s.add(held); added {
			loaded++
		}
	}

	return loaded
}
