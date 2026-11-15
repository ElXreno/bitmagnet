// Package clientid names the software behind a BitTorrent peer id or a DHT message.
package clientid

const (
	Other     = "other"
	None      = "none"
	Bitmagnet = "bitmagnet"
)

var peerCodes = map[string]string{
	"7T": "atorrent",
	"AG": "ares",
	"AZ": "vuze",
	"BC": "bitcomet",
	"BE": "bittorrent-sdk",
	"BI": "biglybt",
	"BL": "bitlord",
	"BM": Bitmagnet,
	"BT": "bittorrent",
	"DE": "deluge",
	"FD": "free-download-manager",
	"FG": "flashget",
	"FL": "folx",
	"FW": "frostwire",
	"GT": "anacrolix",
	"KT": "ktorrent",
	"LT": "libtorrent",
	"MG": "mediaget",
	"PI": "picotorrent",
	"QD": "qqdownload",
	"SD": "xunlei",
	"SP": "bitspirit",
	"TL": "tribler",
	"TR": "transmission",
	"TS": "torrentstorm",
	"UM": "utorrent-mac",
	"UT": "utorrent",
	"UW": "utorrent-web",
	"WD": "webtorrent",
	"WW": "webtorrent",
	"XL": "xunlei",
	"ZO": "zona",
	"lt": "rtorrent",
	"qB": "qbittorrent",
	"tT": "ttorrent",
}

var nodeCodes = map[string]string{
	"AZ": "vuze",
	"LT": "libtorrent",
	"TR": "transmission",
	"UT": "utorrent",
	"lt": "rtorrent",
	"ml": "mldht",
}

// Peer names the client of a peer id (BEP 20 conventions).
func Peer(id [20]byte) string {
	switch {
	case id[0] == '-' && id[7] == '-':
		if name, ok := peerCodes[string(id[1:3])]; ok {
			return name
		}
	case id[0] == 'M' && id[2] == '-':
		return "mainline"
	case string(id[:3]) == "TIX":
		return "tixati"
	case string(id[:3]) == "A2-":
		return "aria2"
	case string(id[:4]) == "exbc":
		return "bitcomet"
	}

	return Other
}

// Node names the DHT implementation announced in the "v" field of a KRPC message.
func Node(v string) string {
	if len(v) < 2 {
		return None
	}

	if name, ok := nodeCodes[v[:2]]; ok {
		return name
	}

	return Other
}

// IsBitmagnetNode reports whether a DHT node id carries the suffix stock bitmagnet appends to its random id.
func IsBitmagnetNode(id [20]byte) bool {
	return string(id[12:]) == "-BM0001-"
}
