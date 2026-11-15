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
	addr, _ := fakeTrackerWithEvents(t, refuse)

	return addr
}

type seenAnnounce struct {
	event uint32
	hash  protocol.ID
	left  uint64
	port  uint16
}

func fakeTrackerWithEvents(t *testing.T, refuse bool) (string, chan seenAnnounce) {
	t.Helper()

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	const connectionID = 0x1122334455667788

	events := make(chan seenAnnounce, 16)

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
			case action == actionAnnounce && binary.BigEndian.Uint64(packet[0:8]) == connectionID && n == 98:
				event := binary.BigEndian.Uint32(packet[80:84])
				events <- seenAnnounce{
					event: event,
					hash:  protocol.ID(packet[16:36]),
					left:  binary.BigEndian.Uint64(packet[64:72]),
					port:  binary.BigEndian.Uint16(packet[96:98]),
				}

				if event != eventStarted {
					continue
				}

				response := make([]byte, 20, 20+12)
				binary.BigEndian.PutUint32(response[0:4], actionAnnounce)
				copy(response[4:8], transactionID)
				binary.BigEndian.PutUint32(response[8:12], 1800)
				binary.BigEndian.PutUint32(response[12:16], 3)
				binary.BigEndian.PutUint32(response[16:20], 7)
				response = append(response, 192, 0, 2, 1, 0x1a, 0xe1, 192, 0, 2, 2, 0, 0)
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

	return conn.LocalAddr().String(), events
}

func TestScrape(t *testing.T) {
	t.Parallel()

	client, err := NewClient("udp4", []string{fakeTracker(t, false)}, time.Second)
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

	client, err := NewClient("udp4", []string{silent.LocalAddr().String(), fakeTracker(t, true)}, 200*time.Millisecond)
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

func TestAnnounce(t *testing.T) {
	t.Parallel()

	addr, events := fakeTrackerWithEvents(t, false)

	client, err := NewClient("udp4", []string{addr}, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = client.Close() })

	result, err := client.Announce(context.Background(), 0, protocol.ID{9}, AnnounceParams{Port: 6881, NumWant: 30})
	if err != nil {
		t.Fatal(err)
	}

	if result.Seeders != 7 || result.Leechers != 3 || result.Interval != 1800 {
		t.Fatalf("got %+v", result)
	}

	if len(result.Peers) != 1 || result.Peers[0].String() != "192.0.2.1:6881" {
		t.Fatalf("want the one peer with a usable port, got %v", result.Peers)
	}

	for _, want := range []uint32{eventStarted, eventStopped} {
		select {
		case got := <-events:
			if got.event != want {
				t.Fatalf("want event %d, got %d", want, got.event)
			}
		case <-time.After(time.Second):
			t.Fatalf("the tracker never saw event %d", want)
		}
	}
}

func TestAnnounceStartAndStop(t *testing.T) {
	t.Parallel()

	addr, events := fakeTrackerWithEvents(t, false)

	client, err := NewClient("udp4", []string{addr}, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = client.Close() })

	hash := protocol.ID{9}
	params := AnnounceParams{Port: 3334}

	result, err := client.AnnounceStart(context.Background(), 0, hash, params)
	if err != nil {
		t.Fatal(err)
	}

	if result.Seeders != 7 || result.Leechers != 3 || len(result.Peers) != 1 {
		t.Fatalf("got %+v", result)
	}

	select {
	case got := <-events:
		if got.event != eventStarted || got.hash != hash || got.port != 3334 || got.left == 0 {
			t.Fatalf("want a started announce as a leecher on port 3334, got %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("the tracker never saw the started event")
	}

	select {
	case got := <-events:
		t.Fatalf("AnnounceStart must stay in the swarm, the tracker saw %+v", got)
	case <-time.After(300 * time.Millisecond):
	}

	if err := client.AnnounceStop(context.Background(), 0, hash, params); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-events:
		if got.event != eventStopped || got.hash != hash || got.port != 3334 {
			t.Fatalf("want a stopped announce for the same hash and port, got %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("the tracker never saw the stopped event")
	}
}

func TestAnnounceStartErrors(t *testing.T) {
	t.Parallel()

	silent, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = silent.Close() })

	client, err := NewClient("udp4", []string{silent.LocalAddr().String()}, 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = client.Close() })

	if _, startErr := client.AnnounceStart(context.Background(), 0, protocol.ID{1}, AnnounceParams{}); !errors.Is(startErr, ErrTimeout) {
		t.Fatalf("silent tracker: want ErrTimeout from AnnounceStart, got %v", startErr)
	}

	if stopErr := client.AnnounceStop(context.Background(), 0, protocol.ID{1}, AnnounceParams{}); !errors.Is(stopErr, ErrTimeout) {
		t.Fatalf("silent tracker: want ErrTimeout from AnnounceStop, got %v", stopErr)
	}
}
