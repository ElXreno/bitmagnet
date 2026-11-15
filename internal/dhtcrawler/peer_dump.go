package dhtcrawler

import (
	"fmt"
	"net/netip"
	"os"
	"sync"
)

const peerDumpLimit = 6000

var peerDump = struct {
	mu    sync.Mutex
	file  *os.File
	count int
}{}

// dumpPeer writes the outcome of the first dials to the file named by BITMAGNET_DUMP_PEERS, for checking the
// same addresses from another network path.
func dumpPeer(outcome string, ping string, addr netip.AddrPort) {
	path := os.Getenv("BITMAGNET_DUMP_PEERS")
	if path == "" || outcome == "skipped" {
		return
	}

	peerDump.mu.Lock()
	defer peerDump.mu.Unlock()

	if peerDump.count >= peerDumpLimit {
		return
	}

	if peerDump.file == nil {
		file, err := os.Create(path)
		if err != nil {
			peerDump.count = peerDumpLimit

			return
		}

		peerDump.file = file
	}

	peerDump.count++

	_, _ = fmt.Fprintf(peerDump.file, "%s %s %s\n", outcome, ping, addr)
}
