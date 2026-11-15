package metainfo

import (
	"crypto/sha1" //nolint:gosec
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

const (
	testV1Info = "d6:lengthi30e4:name4:test12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaae"
	testV2Tree = "9:file treed1:ad0:d6:lengthi10e11:pieces root32:" + "rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr" +
		"ee3:dird1:bd0:d6:lengthi20e11:pieces root32:" + "ssssssssssssssssssssssssssssssss" + "eeee"
	testV2Info     = "d" + testV2Tree + "12:meta versioni2e4:name4:test12:piece lengthi16384ee"
	testHybridInfo = "d" + testV2Tree +
		"5:filesld6:lengthi10e4:pathl1:aeed6:lengthi20e4:pathl3:dir1:beee12:meta versioni2e4:name4:test" +
		"12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaae"
)

func v1Hash(info string) protocol.ID {
	return sha1.Sum([]byte(info)) //nolint:gosec
}

func v2Hash(info string) protocol.ID {
	sum := sha256.Sum256([]byte(info))

	return protocol.ID(sum[:20])
}

func TestParseMetaInfoIdentity(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		info string
		hash protocol.ID
		want Identity
	}{
		"v1 by its hash":          {testV1Info, v1Hash(testV1Info), Identity{v1Hash(testV1Info), KindV1}},
		"hybrid by its v1 hash":   {testHybridInfo, v1Hash(testHybridInfo), Identity{v1Hash(testHybridInfo), KindV1}},
		"hybrid by its v2 hash":   {testHybridInfo, v2Hash(testHybridInfo), Identity{v1Hash(testHybridInfo), KindHybridV2}},
		"v2 only by its v2 hash":  {testV2Info, v2Hash(testV2Info), Identity{v2Hash(testV2Info), KindV2}},
		"v2 only by its v1 style": {testV2Info, v1Hash(testV2Info), Identity{v1Hash(testV2Info), KindV1}},
	} {
		info, identity, err := ParseMetaInfo(test.hash, []byte(test.info))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		if identity != test.want {
			t.Errorf("%s: got %+v, want %+v", name, identity, test.want)
		}

		if info.BestName() != "test" || info.PieceLength != 16384 || info.TotalLength() != 30 {
			t.Errorf("%s: name %q, piece length %d, total %d", name, info.BestName(), info.PieceLength, info.TotalLength())
		}
	}
}

func TestParseMetaInfoRefusesForeignBytes(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		info string
		hash protocol.ID
	}{
		"unrelated hash":                 {testV1Info, protocol.ID{1}},
		"v1 info under a sha256 of it":   {testV1Info, v2Hash(testV1Info)},
		"other bytes under a known hash": {strings.Replace(testV2Info, "test", "tesT", 1), v2Hash(testV2Info)},
	} {
		if _, _, err := ParseMetaInfo(test.hash, []byte(test.info)); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}
