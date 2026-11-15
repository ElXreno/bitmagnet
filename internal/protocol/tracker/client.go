package tracker

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

// MaxScrapeHashes is how many info hashes fit into one UDP tracker scrape request (BEP 15).
const MaxScrapeHashes = 74

const (
	protocolID        = 0x41727101980
	actionConnect     = 0
	actionAnnounce    = 1
	actionScrape      = 2
	actionError       = 3
	connectionIDTTL   = 50 * time.Second
	resolveTTL        = 10 * time.Minute
	scrapeEntryLength = 12
	eventStarted      = 2
	eventStopped      = 3
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
	network  string
	conn     *net.UDPConn
	timeout  time.Duration
	trackers []*trackerState
	mu       sync.Mutex
	pending  map[uint32]chan []byte
	// OnSend sees every packet sent; set it before the first request
	OnSend func(index int, kind string)
}

const (
	SentConnect = "connect"
	SentScrape  = "scrape"
	SentStarted = "started"
	SentStopped = "stopped"
)

func (c *Client) sent(index int, kind string) {
	if c.OnSend != nil {
		c.OnSend(index, kind)
	}
}

type trackerState struct {
	index        int
	host         string
	mu           sync.Mutex
	addr         *net.UDPAddr
	resolvedAt   time.Time
	connectionID uint64
	connectedAt  time.Time
}

// NewClient opens one socket of the given network, "udp4" or "udp6"; trackers are resolved for that family.
func NewClient(network string, hosts []string, timeout time.Duration) (*Client, error) {
	conn, err := net.ListenUDP(network, &net.UDPAddr{})
	if err != nil {
		return nil, err
	}

	c := &Client{
		network: network,
		conn:    conn,
		timeout: timeout,
		pending: make(map[uint32]chan []byte),
	}

	for index, host := range hosts {
		c.trackers = append(c.trackers, &trackerState{index: index, host: host})
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
		addr, err := net.ResolveUDPAddr(c.network, t.host)
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

	c.sent(t.index, SentConnect)

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

	c.sent(index, SentScrape)

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

type AnnounceResult struct {
	Interval uint32
	Leechers uint32
	Seeders  uint32
	Peers    []netip.AddrPort
}

// AnnounceParams describes how the client presents itself to a tracker.
type AnnounceParams struct {
	PeerID  protocol.ID
	Key     uint32
	Port    uint16
	NumWant int32
}

func (c *Client) announceRequest(
	connectionID uint64,
	transactionID uint32,
	hash protocol.ID,
	params AnnounceParams,
	event uint32,
	numWant int32,
) []byte {
	request := make([]byte, 98)
	binary.BigEndian.PutUint64(request[0:8], connectionID)
	binary.BigEndian.PutUint32(request[8:12], actionAnnounce)
	binary.BigEndian.PutUint32(request[12:16], transactionID)
	copy(request[16:36], hash[:])
	copy(request[36:56], params.PeerID[:])
	binary.BigEndian.PutUint64(request[64:72], 1<<30)
	binary.BigEndian.PutUint32(request[80:84], event)
	binary.BigEndian.PutUint32(request[88:92], params.Key)
	binary.BigEndian.PutUint32(request[92:96], uint32(numWant))
	binary.BigEndian.PutUint16(request[96:98], params.Port)

	return request
}

// Announce joins the swarm of hash on tracker number index as a leecher, takes the peers the tracker returns
// and leaves the swarm again at once with a stopped event, which is sent without waiting for an answer.
func (c *Client) Announce(
	ctx context.Context,
	index int,
	hash protocol.ID,
	params AnnounceParams,
) (AnnounceResult, error) {
	response, addr, connectionID, err := c.started(ctx, index, hash, params)
	if err != nil {
		return AnnounceResult{}, err
	}

	c.sent(index, SentStopped)

	_, _ = c.conn.WriteToUDP(
		c.announceRequest(connectionID, newTransactionID(), hash, params, eventStopped, 0), addr)

	return c.announceResult(response)
}

func (c *Client) AnnounceStart(
	ctx context.Context,
	index int,
	hash protocol.ID,
	params AnnounceParams,
) (AnnounceResult, error) {
	response, _, _, err := c.started(ctx, index, hash, params)
	if err != nil {
		return AnnounceResult{}, err
	}

	return c.announceResult(response)
}

func (c *Client) AnnounceStop(ctx context.Context, index int, hash protocol.ID, params AnnounceParams) error {
	addr, connectionID, err := c.connection(ctx, c.trackers[index])
	if err != nil {
		return err
	}

	c.sent(index, SentStopped)

	_, err = c.conn.WriteToUDP(
		c.announceRequest(connectionID, newTransactionID(), hash, params, eventStopped, 0), addr)

	return err
}

func (c *Client) started(
	ctx context.Context,
	index int,
	hash protocol.ID,
	params AnnounceParams,
) ([]byte, *net.UDPAddr, uint64, error) {
	t := c.trackers[index]

	addr, connectionID, err := c.connection(ctx, t)
	if err != nil {
		return nil, nil, 0, err
	}

	transactionID := newTransactionID()

	c.sent(index, SentStarted)

	response, err := c.roundTrip(
		ctx, addr, transactionID,
		c.announceRequest(connectionID, transactionID, hash, params, eventStarted, params.NumWant),
	)
	if err != nil {
		if errors.Is(err, ErrTracker) {
			t.mu.Lock()
			t.connectedAt = time.Time{}
			t.mu.Unlock()
		}

		return nil, nil, 0, err
	}

	return response, addr, connectionID, nil
}

func (c *Client) announceResult(response []byte) (AnnounceResult, error) {
	entry := 6
	if c.network == "udp6" {
		entry = 18
	}

	if binary.BigEndian.Uint32(response[0:4]) != actionAnnounce || len(response) < 20 || (len(response)-20)%entry != 0 {
		return AnnounceResult{}, ErrResponse
	}

	result := AnnounceResult{
		Interval: binary.BigEndian.Uint32(response[8:12]),
		Leechers: binary.BigEndian.Uint32(response[12:16]),
		Seeders:  binary.BigEndian.Uint32(response[16:20]),
	}

	for i := 20; i+entry <= len(response); i += entry {
		ip, _ := netip.AddrFromSlice(response[i : i+entry-2])
		port := binary.BigEndian.Uint16(response[i+entry-2 : i+entry])

		if port != 0 && ip.IsValid() && !ip.IsUnspecified() {
			result.Peers = append(result.Peers, netip.AddrPortFrom(ip, port))
		}
	}

	return result, nil
}
