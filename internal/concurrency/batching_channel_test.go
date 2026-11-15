package concurrency

import (
	"context"
	"testing"
	"time"
)

func TestBatchingChannelDrain(t *testing.T) {
	t.Parallel()

	ch := NewBatchingChannel[int](10, 3, time.Hour)

	for i := range 7 {
		ch.In() <- i
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	seen := map[int]bool{}

	for _, batch := range ch.Drain(ctx) {
		for _, v := range batch {
			seen[v] = true
		}
	}

	if len(seen) != 7 {
		t.Fatalf("want all 7 items, got %v", seen)
	}

	if again := ch.Drain(ctx); len(again) != 0 {
		t.Fatalf("a drained channel must be empty, got %v", again)
	}
}

func TestBatchingChannelDrainGivesUpWithItsContext(t *testing.T) {
	t.Parallel()

	ch := NewBatchingChannel[int](1, 1, time.Hour)

	ch.In() <- 1

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})

	go func() {
		ch.Drain(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Drain ignored its cancelled context")
	}
}
