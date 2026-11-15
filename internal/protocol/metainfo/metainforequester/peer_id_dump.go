package metainforequester

import (
	"fmt"
	"net/netip"
	"os"
	"sync"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

const (
	peerIDDumpLimit      = 300_000
	peerIDDumpVersionLen = 40
)

var peerIDDumpPath = os.Getenv("BITMAGNET_DUMP_PEER_IDS")

var peerIDDump = struct {
	mu    sync.Mutex
	file  *os.File
	count int
}{}

func dumpPeerID(
	addr netip.AddrPort,
	infoHash protocol.ID,
	handshake HandshakeInfo,
	client string,
	outcome string,
	version string,
) {
	if peerIDDumpPath == "" {
		return
	}

	peerIDDump.mu.Lock()
	defer peerIDDump.mu.Unlock()

	if peerIDDump.count >= peerIDDumpLimit {
		return
	}

	if peerIDDump.file == nil {
		file, err := os.Create(peerIDDumpPath)
		if err != nil {
			peerIDDump.count = peerIDDumpLimit

			return
		}

		peerIDDump.file = file
	}

	peerIDDump.count++

	if len(version) > peerIDDumpVersionLen {
		version = version[:peerIDDumpVersionLen]
	}

	_, _ = fmt.Fprintf(peerIDDump.file, "%s %x %x %x %s %s %q\n",
		addr, infoHash[:], handshake.PeerID[:], handshake.PeerExtensionBits[:], client, outcome, version)
}
