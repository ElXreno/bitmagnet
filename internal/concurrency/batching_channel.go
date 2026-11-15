package concurrency

import (
	"context"
	"time"
)

type BatchingChannel[T any] interface {
	In() chan<- T
	Out() <-chan []T
	Drain(ctx context.Context) [][]T
}

type batchingChannel[T any] struct {
	input        chan T
	output       chan []T
	drains       chan chan []T
	buffer       []T
	maxBatchSize int
	maxWaitTime  time.Duration
	ticker       *time.Ticker
}

func NewBatchingChannel[T any](capacity int, maxBatchSize int, maxWaitTime time.Duration) BatchingChannel[T] {
	ch := &batchingChannel[T]{
		input:        make(chan T, capacity),
		output:       make(chan []T, 1),
		drains:       make(chan chan []T),
		maxBatchSize: maxBatchSize,
		maxWaitTime:  maxWaitTime,
		ticker:       time.NewTicker(maxWaitTime),
	}
	go ch.batch()

	return ch
}

func (ch *batchingChannel[T]) In() chan<- T {
	return ch.input
}

func (ch *batchingChannel[T]) Out() <-chan []T {
	return ch.output
}

// Drain takes every batch that is already flushed and everything that is still buffered.
func (ch *batchingChannel[T]) Drain(ctx context.Context) [][]T {
	var batches [][]T

	reply := make(chan []T, 1)

	for {
		select {
		case batch := <-ch.output:
			batches = append(batches, batch)
		case ch.drains <- reply:
			if rest := <-reply; len(rest) > 0 {
				batches = append(batches, rest)
			}

			for {
				select {
				case batch := <-ch.output:
					batches = append(batches, batch)
				default:
					return batches
				}
			}
		case <-ctx.Done():
			return batches
		}
	}
}

func (ch *batchingChannel[T]) batch() {
	var next T

	var ok bool

	defer close(ch.output)

	for {
		select {
		case next, ok = <-ch.input:
			if !ok {
				break
			}

			ch.buffer = append(ch.buffer, next)
			if len(ch.buffer) >= ch.maxBatchSize {
				ch.flush()
			}
		case <-ch.ticker.C:
			if len(ch.buffer) > 0 {
				ch.flush()
			}
		case reply := <-ch.drains:
			ch.collect()

			rest := ch.buffer
			ch.buffer = nil
			reply <- rest
		}
	}
}

func (ch *batchingChannel[T]) collect() {
	for {
		select {
		case next := <-ch.input:
			ch.buffer = append(ch.buffer, next)
		default:
			return
		}
	}
}

func (ch *batchingChannel[T]) flush() {
	ch.ticker.Stop()
	batch := ch.buffer
	ch.buffer = nil
	ch.ticker.Reset(ch.maxWaitTime)
	ch.output <- batch
}
