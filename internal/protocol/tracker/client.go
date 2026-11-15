package tracker

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

// MaxScrapeHashes is how many info hashes fit into one UDP tracker scrape request (BEP 15).
const MaxScrapeHashes = 74

const (
	protocolID        = 0x41727101980
	actionConnect     = 0
	actionScrape      = 2
	actionError       = 3
	connectionIDTTL   = 50 * time.Second
	resolveTTL        = 10 * time.Minute
	scrapeEntryLength = 12
)

var (
	ErrTimeout  = errors.New("tracker did not answer in time")
	ErrTracker  = errors.New("tracker returned an error")
	ErrResponse = errors.New("malformed tracker response")
)

type ScrapeResult struct {
	Seeders   uint32
	Completed uint32
	Leechers  uint32
}

// Client speaks the UDP tracker protocol (BEP 15) to a fixed set of trackers over one socket.
type Client struct {
	conn     *net.UDPConn
	timeout  time.Duration
	trackers []*trackerState
	mu       sync.Mutex
	pending  map[uint32]chan []byte
}

type trackerState struct {
	host         string
	mu           sync.Mutex
	addr         *net.UDPAddr
	resolvedAt   time.Time
	connectionID uint64
	connectedAt  time.Time
}

func NewClient(hosts []string, timeout time.Duration) (*Client, error) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{})
	if err != nil {
		return nil, err
	}

	c := &Client{
		conn:    conn,
		timeout: timeout,
		pending: make(map[uint32]chan []byte),
	}

	for _, host := range hosts {
		c.trackers = append(c.trackers, &trackerState{host: host})
	}

	go c.read()

	return c, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Trackers() []string {
	hosts := make([]string, 0, len(c.trackers))
	for _, t := range c.trackers {
		hosts = append(hosts, t.host)
	}

	return hosts
}

func (c *Client) read() {
	buffer := make([]byte, 2048)

	for {
		n, _, err := c.conn.ReadFromUDP(buffer)
		if err != nil {
			return
		}

		if n < 8 {
			continue
		}

		transactionID := binary.BigEndian.Uint32(buffer[4:8])

		c.mu.Lock()
		ch, ok := c.pending[transactionID]
		c.mu.Unlock()

		if ok {
			packet := make([]byte, n)
			copy(packet, buffer[:n])

			select {
			case ch <- packet:
			default:
			}
		}
	}
}

func (c *Client) roundTrip(ctx context.Context, addr *net.UDPAddr, transactionID uint32, request []byte) ([]byte, error) {
	ch := make(chan []byte, 1)

	c.mu.Lock()
	c.pending[transactionID] = ch
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, transactionID)
		c.mu.Unlock()
	}()

	if _, err := c.conn.WriteToUDP(request, addr); err != nil {
		return nil, err
	}

	timer := time.NewTimer(c.timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, ErrTimeout
	case packet := <-ch:
		if binary.BigEndian.Uint32(packet[0:4]) == actionError {
			return nil, fmt.Errorf("%w: %s", ErrTracker, string(packet[8:]))
		}

		return packet, nil
	}
}

func newTransactionID() uint32 {
	var b [4]byte

	_, _ = rand.Read(b[:])

	return binary.BigEndian.Uint32(b[:])
}

func (c *Client) connection(ctx context.Context, t *trackerState) (*net.UDPAddr, uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.addr == nil || time.Since(t.resolvedAt) > resolveTTL {
		addr, err := net.ResolveUDPAddr("udp4", t.host)
		if err != nil {
			return nil, 0, err
		}

		t.addr = addr
		t.resolvedAt = time.Now()
		t.connectedAt = time.Time{}
	}

	if time.Since(t.connectedAt) < connectionIDTTL {
		return t.addr, t.connectionID, nil
	}

	transactionID := newTransactionID()
	request := make([]byte, 16)
	binary.BigEndian.PutUint64(request[0:8], protocolID)
	binary.BigEndian.PutUint32(request[8:12], actionConnect)
	binary.BigEndian.PutUint32(request[12:16], transactionID)

	response, err := c.roundTrip(ctx, t.addr, transactionID, request)
	if err != nil {
		return nil, 0, err
	}

	if len(response) < 16 || binary.BigEndian.Uint32(response[0:4]) != actionConnect {
		return nil, 0, ErrResponse
	}

	t.connectionID = binary.BigEndian.Uint64(response[8:16])
	t.connectedAt = time.Now()

	return t.addr, t.connectionID, nil
}

// Scrape asks tracker number index for the swarm sizes of up to MaxScrapeHashes info hashes.
func (c *Client) Scrape(ctx context.Context, index int, hashes []protocol.ID) ([]ScrapeResult, error) {
	if len(hashes) == 0 || len(hashes) > MaxScrapeHashes {
		return nil, fmt.Errorf("scrape takes 1 to %d info hashes, got %d", MaxScrapeHashes, len(hashes))
	}

	t := c.trackers[index]

	addr, connectionID, err := c.connection(ctx, t)
	if err != nil {
		return nil, err
	}

	transactionID := newTransactionID()
	request := make([]byte, 16, 16+20*len(hashes))
	binary.BigEndian.PutUint64(request[0:8], connectionID)
	binary.BigEndian.PutUint32(request[8:12], actionScrape)
	binary.BigEndian.PutUint32(request[12:16], transactionID)

	for _, hash := range hashes {
		request = append(request, hash[:]...)
	}

	response, err := c.roundTrip(ctx, addr, transactionID, request)
	if err != nil {
		if errors.Is(err, ErrTracker) {
			t.mu.Lock()
			t.connectedAt = time.Time{}
			t.mu.Unlock()
		}

		return nil, err
	}

	if binary.BigEndian.Uint32(response[0:4]) != actionScrape || len(response) != 8+scrapeEntryLength*len(hashes) {
		return nil, ErrResponse
	}

	results := make([]ScrapeResult, len(hashes))

	for i := range hashes {
		entry := response[8+scrapeEntryLength*i:]
		results[i] = ScrapeResult{
			Seeders:   binary.BigEndian.Uint32(entry[0:4]),
			Completed: binary.BigEndian.Uint32(entry[4:8]),
			Leechers:  binary.BigEndian.Uint32(entry[8:12]),
		}
	}

	return results, nil
}
