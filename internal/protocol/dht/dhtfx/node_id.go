package dhtfx

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"go.uber.org/fx"
)

const nodeIDFile = "dht_node_id"

type nodeIDResult struct {
	fx.Out
	ID   protocol.ID `name:"dht_node_id"`
	Save func()      `name:"dht_node_id_save"`
}

func persistentNodeID() nodeIDResult {
	id, stored := loadNodeID(nodeIDFile)

	return nodeIDResult{
		ID: id,
		Save: func() {
			if !stored {
				saveNodeID(nodeIDFile, id)
			}
		},
	}
}

func loadNodeID(path string) (id protocol.ID, stored bool) {
	if data, err := os.ReadFile(path); err == nil {
		if parsed, parseErr := protocol.ParseID(strings.TrimSpace(string(data))); parseErr == nil {
			return parsed, true
		}
	}

	return protocol.RandomNodeIDWithClientSuffix(), false
}

func saveNodeID(path string, id protocol.ID) {
	_ = writeFileAtomic(path, []byte(id.String()+"\n"))
}

func writeFileAtomic(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), "tmp-*")
	if err != nil {
		return err
	}

	_, err = file.Write(data)
	if err == nil {
		err = file.Sync()
	}

	if closeErr := file.Close(); err == nil {
		err = closeErr
	}

	if err == nil {
		err = os.Rename(file.Name(), path)
	}

	if err != nil {
		_ = os.Remove(file.Name())
	}

	return err
}
