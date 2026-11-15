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
		"-XF9990-abcdefghijkl": "xfplay",
		"-XT1000-04d9f58343e2": XT,
		"-CC1000-c87f546c72f4": CC,
		"-BN0001-abcdefghijkl": "baidu-netdisk",
		"-BL246326abcdefghijk": "bitlord",
		"-SD0100-abcdefghijkl": "xunlei",
		"aria2-abcdefghijklmn": "aria2",
		"QVOD0054abcdefghijkl": "qvod",
		"abcdefghijklmnopqrst": Other,
	} {
		if got := Peer(id(peerID)); got != want {
			t.Errorf("%q: want %s, got %s", peerID, want, got)
		}
	}
}

func TestClosedNetwork(t *testing.T) {
	t.Parallel()

	if !ClosedNetwork(Peer(id("-XT1000-04d9f58343e2"))) || !ClosedNetwork(Peer(id("-CC1000-c87f546c72f4"))) ||
		ClosedNetwork(Peer(id("-qB4650-abcdefghijkl"))) || ClosedNetwork(Other) {
		t.Fatal("only the XT and CC networks are closed")
	}

	if !ClosedNetworkNode("LT\x01\x00") || ClosedNetworkNode("LT\x01\x02") || ClosedNetworkNode("") {
		t.Fatal("only libtorrent 1.0.0 nodes carry the hint")
	}
}

func TestPeerKey(t *testing.T) {
	t.Parallel()

	for peerID, want := range map[string]string{
		"-XT1000-04d9f58343e2":             "-XT1000-",
		"-qB5300-abcdefghijkl":             "-qB5300-",
		"TIX0325-abcdefghijkl":             "TIX0",
		"M7-10-5--abcdefghijk":             "M7-1",
		"\x01\x02\x03\x04abcdefghijklmnop": "binary",
		"-X\x00T100-abcdefghijkl":          "binary",
	} {
		if got := PeerKey(id(peerID)); got != want {
			t.Errorf("%q: want %q, got %q", peerID, want, got)
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
