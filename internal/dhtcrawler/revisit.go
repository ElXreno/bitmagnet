package dhtcrawler

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

const (
	revisitInterval = time.Minute
	revisitQueue    = 500_000
	revisitMinFresh = 5
)

var (
	revisitConcurrency = envInt("BITMAGNET_REVISIT_CONCURRENCY", 1000)
	revisitMaxVisits   = envInt("BITMAGNET_REVISIT_MAX_VISITS", 10)
	revisitEnabled     = os.Getenv("BITMAGNET_REVISIT") == "on"
)

type revisit struct {
	node ktable.Node
	due  time.Time
}

// revisits brings the crawler back to nodes that store more info hashes than one sample carries, once a
// minute, for as long as a visit still yields new hashes. The routing table is too small to do that: it keeps
// a few thousand nodes while sampling meets hundreds of new ones every second.
type revisits struct {
	queue  chan revisit
	stage  concurrency.BufferedConcurrentChannel[ktable.Node]
	mu     sync.Mutex
	visits map[protocol.ID]int
}

func newRevisits() *revisits {
	if !revisitEnabled {
		return nil
	}

	return newRevisitQueue()
}

func newRevisitQueue() *revisits {
	return &revisits{
		queue:  make(chan revisit, revisitQueue),
		stage:  concurrency.NewBufferedConcurrentChannel[ktable.Node](revisitConcurrency, revisitConcurrency),
		visits: make(map[protocol.ID]int),
	}
}

func (r *revisits) forget(id protocol.ID) {
	if r == nil {
		return
	}

	r.mu.Lock()
	delete(r.visits, id)
	r.mu.Unlock()
}

// plan decides after a successful sample whether the node is worth another visit and reports why not.
func (r *revisits) plan(node ktable.Node, stored int, samples int, fresh int) string {
	if r == nil {
		return "off"
	}

	id := node.ID()

	r.mu.Lock()
	defer r.mu.Unlock()

	visits := r.visits[id] + 1

	switch {
	case stored <= samples:
		delete(r.visits, id)

		return "done_drained"
	case fresh < revisitMinFresh:
		delete(r.visits, id)

		return "done_low_yield"
	case visits >= revisitMaxVisits:
		delete(r.visits, id)

		return "done_max_visits"
	}

	select {
	case r.queue <- revisit{node: ktable.NewNode(id, node.Addr()), due: time.Now().Add(revisitInterval)}:
		r.visits[id] = visits

		return "scheduled"
	default:
		delete(r.visits, id)

		return "queue_full"
	}
}

func (c *crawler) runRevisits(ctx context.Context, f *nodeFamily) {
	if f.revisits == nil {
		return
	}

	go func() {
		_ = f.revisits.stage.Run(ctx, func(n ktable.Node) {
			c.sampleNode(ctx, f, n)
		})
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case r := <-f.revisits.queue:
			if wait := time.Until(r.due); wait > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(wait):
				}
			}

			select {
			case <-ctx.Done():
				return
			case f.revisits.stage.In() <- r.node:
			}
		}
	}
}
