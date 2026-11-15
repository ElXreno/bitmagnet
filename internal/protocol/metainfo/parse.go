package metainfo

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/anacrolix/torrent/bencode"
	mi "github.com/anacrolix/torrent/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

const (
	KindV1       = "v1"
	KindV2       = "v2"
	KindHybridV2 = "hybrid_v2"
)

// Identity tells how a meta info relates to the info hash it was fetched for. A BEP 52 torrent is also known
// by the truncated SHA-256 of its info dict; a hybrid one fetched under that hash is stored under its SHA-1.
type Identity struct {
	InfoHash protocol.ID
	Kind     string
}

func ParseMetaInfoBytes(infoHash protocol.ID, metaInfoBytes []byte) (Info, error) {
	info, _, err := ParseMetaInfo(infoHash, metaInfoBytes)

	return info, err
}

func ParseMetaInfo(infoHash protocol.ID, metaInfoBytes []byte) (Info, Identity, error) {
	v1Hash := protocol.ID(mi.HashBytes(metaInfoBytes))
	identity := Identity{InfoHash: infoHash, Kind: KindV1}

	if v1Hash != infoHash {
		v2Hash := sha256.Sum256(metaInfoBytes)
		if protocol.ID(v2Hash[:20]) != infoHash {
			return Info{}, Identity{}, errors.New("info bytes have wrong hash")
		}

		identity.Kind = KindV2
	}

	var info Info
	if unmarshalErr := bencode.Unmarshal(metaInfoBytes, &info); unmarshalErr != nil {
		return Info{}, Identity{}, fmt.Errorf("error unmarshaling info bytes: %w", unmarshalErr)
	}

	if identity.Kind == KindV2 {
		if !info.HasV2() {
			return Info{}, Identity{}, errors.New("info bytes match the v2 hash without being a v2 meta info")
		}

		if len(info.Pieces) > 0 {
			identity = Identity{InfoHash: v1Hash, Kind: KindHybridV2}
		}
	}

	return info, identity, nil
}
