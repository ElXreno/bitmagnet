package clientid

import "testing"

func id(s string) (out [20]byte) {
	copy(out[:], s)

	return out
}

func TestPeer(t *testing.T) {
	t.Parallel()

	for peerID, want := range map[string]string{
		"-qB4650-abcdefghijkl": "qbittorrent",
		"-UT355W-abcdefghijkl": "utorrent",
		"-TR4060-abcdefghijkl": "transmission",
		"-lt0D80-abcdefghijkl": "rtorrent",
		"-BM0001-abcdefghijkl": Bitmagnet,
		"M7-10-5--abcdefghijk": "mainline",
		"TIX0325-abcdefghijkl": "tixati",
		"A2-1-37-0-abcdefghij": "aria2",
		"-ZZ0000-abcdefghijkl": Other,
		"abcdefghijklmnopqrst": Other,
	} {
		if got := Peer(id(peerID)); got != want {
			t.Errorf("%q: want %s, got %s", peerID, want, got)
		}
	}
}

func TestNode(t *testing.T) {
	t.Parallel()

	for v, want := range map[string]string{"LT\x01\x02": "libtorrent", "UT\x00\x00": "utorrent", "": None, "x": None, "zz00": Other} {
		if got := Node(v); got != want {
			t.Errorf("%q: want %s, got %s", v, want, got)
		}
	}

	if !IsBitmagnetNode(id("abcdefghijkl-BM0001-")) || IsBitmagnetNode(id("-BM0001-abcdefghijkl")) {
		t.Error("bitmagnet nodes carry the marker at the end of the id")
	}
}
