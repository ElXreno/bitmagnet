// Package clientid names the software behind a BitTorrent peer id or a DHT message.
package clientid

const (
	Other     = "other"
	None      = "none"
	Bitmagnet = "bitmagnet"
)

var peerPrefixes = []struct{ prefix, name string }{
	{"TIX", "tixati"},
	{"A2-", "aria2"},
	{"aria2-", "aria2"},
	{"exbc", "bitcomet"},
	{"FD6", "free-download-manager"},
	{"QVOD", "qvod"},
	{"XBT", "xbt"},
	{"DNA", "bittorrent-dna"},
	{"LIME", "limewire"},
	{"btpd", "btpd"},
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
	if id[0] == '-' {
		if name, ok := peerCodes[string(id[1:3])]; ok {
			return name
		}

		return Other
	}

	if id[0] == 'M' && id[2] == '-' {
		return "mainline"
	}

	for _, known := range peerPrefixes {
		if string(id[:len(known.prefix)]) == known.prefix {
			return known.name
		}
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
