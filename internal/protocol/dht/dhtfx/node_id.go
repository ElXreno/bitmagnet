package dhtfx

import (
	"os"
	"strings"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

const nodeIDFile = "dht_node_id"

func persistentNodeID() protocol.ID {
	if data, err := os.ReadFile(nodeIDFile); err == nil {
		if id, parseErr := protocol.ParseID(strings.TrimSpace(string(data))); parseErr == nil {
			return id
		}
	}

	id := protocol.RandomNodeIDWithClientSuffix()
	_ = os.WriteFile(nodeIDFile, []byte(id.String()+"\n"), 0o600)

	return id
}
