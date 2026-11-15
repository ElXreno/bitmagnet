package tracker

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

// fakeTracker answers connect and scrape requests the way BEP 15 describes: seeders = first byte of the info
// hash, leechers = second byte. With refuse set it answers every scrape with an error packet.
func fakeTracker(t *testing.T, refuse bool) string {
	t.Helper()

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	const connectionID = 0x1122334455667788

	go func() {
		buffer := make([]byte, 2048)

		for {
			n, from, readErr := conn.ReadFromUDP(buffer)
			if readErr != nil {
				return
			}

			packet := buffer[:n]
			if n < 16 {
				continue
			}

			action := binary.BigEndian.Uint32(packet[8:12])
			transactionID := packet[12:16]

			switch {
			case action == actionConnect && binary.BigEndian.Uint64(packet[0:8]) == protocolID:
				response := make([]byte, 16)
				copy(response[4:8], transactionID)
				binary.BigEndian.PutUint64(response[8:16], connectionID)
				_, _ = conn.WriteToUDP(response, from)
			case action == actionScrape && binary.BigEndian.Uint64(packet[0:8]) == connectionID && refuse:
				response := make([]byte, 8)
				binary.BigEndian.PutUint32(response[0:4], actionError)
				copy(response[4:8], transactionID)
				_, _ = conn.WriteToUDP(append(response, "go away"...), from)
			case action == actionScrape && binary.BigEndian.Uint64(packet[0:8]) == connectionID:
				hashes := packet[16:]
				response := make([]byte, 8, 8+scrapeEntryLength*len(hashes)/20)
				binary.BigEndian.PutUint32(response[0:4], actionScrape)
				copy(response[4:8], transactionID)

				for i := 0; i+20 <= len(hashes); i += 20 {
					entry := make([]byte, scrapeEntryLength)
					binary.BigEndian.PutUint32(entry[0:4], uint32(hashes[i]))
					binary.BigEndian.PutUint32(entry[8:12], uint32(hashes[i+1]))
					response = append(response, entry...)
				}

				_, _ = conn.WriteToUDP(response, from)
			}
		}
	}()

	return conn.LocalAddr().String()
}

func TestScrape(t *testing.T) {
	t.Parallel()

	client, err := NewClient([]string{fakeTracker(t, false)}, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = client.Close() })

	hashes := make([]protocol.ID, MaxScrapeHashes)
	for i := range hashes {
		hashes[i] = protocol.ID{byte(i), byte(2 * i)}
	}

	for range 2 {
		results, scrapeErr := client.Scrape(context.Background(), 0, hashes)
		if scrapeErr != nil {
			t.Fatal(scrapeErr)
		}

		if len(results) != len(hashes) {
			t.Fatalf("want %d results, got %d", len(hashes), len(results))
		}

		for i, r := range results {
			if r.Seeders != uint32(i) || r.Leechers != uint32(byte(2*i)) {
				t.Fatalf("hash %d: got %+v", i, r)
			}
		}
	}

	if _, tooMany := client.Scrape(context.Background(), 0, make([]protocol.ID, MaxScrapeHashes+1)); tooMany == nil {
		t.Fatal("more hashes than fit into a packet must be refused")
	}
}

func TestScrapeErrors(t *testing.T) {
	t.Parallel()

	silent, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = silent.Close() })

	client, err := NewClient([]string{silent.LocalAddr().String(), fakeTracker(t, true)}, 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = client.Close() })

	if _, scrapeErr := client.Scrape(context.Background(), 0, []protocol.ID{{1}}); !errors.Is(scrapeErr, ErrTimeout) {
		t.Fatalf("silent tracker: want ErrTimeout, got %v", scrapeErr)
	}

	if _, scrapeErr := client.Scrape(context.Background(), 1, []protocol.ID{{1}}); !errors.Is(scrapeErr, ErrTracker) {
		t.Fatalf("refusing tracker: want ErrTracker, got %v", scrapeErr)
	}
}
