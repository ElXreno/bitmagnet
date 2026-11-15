package dht

import (
	"testing"

	"github.com/anacrolix/torrent/bencode"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnmarshalNodeAddr(t *testing.T) {
	t.Parallel()

	var na NodeAddr

	require.NoError(t, na.UnmarshalBinary([]byte("\x01\x02\x03\x04\x05\x06")))
	assert.Equal(t, "1.2.3.4", na.IP.String())
}

func TestUnmarshalShortNodeAddr(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		"d1:rd2:id20:aaaaaaaaaaaaaaaaaaaa6:valuesl0:ee1:t1:a1:y1:re",
		"d1:rd2:id20:aaaaaaaaaaaaaaaaaaaa6:valuesl1:xee1:t1:a1:y1:re",
		"d2:ip1:x1:rd2:id20:aaaaaaaaaaaaaaaaaaaae1:t1:a1:y1:re",
	} {
		var msg Msg

		require.NoError(t, bencode.Unmarshal([]byte(raw), &msg))

		for _, v := range msg.R.Values {
			assert.False(t, v.ToAddrPort().IsValid())
		}
	}
}
