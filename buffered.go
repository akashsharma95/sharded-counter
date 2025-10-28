package shardedcounter

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// BufferedCounter wraps a Counter and provides in-memory write buffering
// to dramatically increase throughput by batching many increments into fewer S3 operations.
type BufferedCounter struct {
	*Counter

	logger        *log.Logger
	buffers       map[string]map[int]*shardBuffer // counter name -> shard -> buffer
	stopCh        chan struct{}
	stoppedCh     chan struct{}
	flushInterval time.Duration
	maxBufferSize int64 // flush when a shard buffer reaches this size

	mu sync.Mutex
}

type shardBuffer struct {
	epoch int
	delta int64
}

// NewBuffered creates a BufferedCounter that accumulates increments in memory
// and flushes them periodically or when buffers reach maxBufferSize.
func NewBuffered(counter *Counter, flushInterval time.Duration, maxBufferSize int64, logger *log.Logger) *BufferedCounter {
	if maxBufferSize <= 0 {
		maxBufferSize = 1000
	}
	bc := &BufferedCounter{
		Counter:       counter,
		buffers:       make(map[string]map[int]*shardBuffer),
		flushInterval: flushInterval,
		maxBufferSize: maxBufferSize,
		stopCh:        make(chan struct{}),
		stoppedCh:     make(chan struct{}),
		logger:        logger,
	}
	return bc
}

// Start begins the background flusher goroutine.
func (bc *BufferedCounter) Start() {
	go bc.flusher()
}

// Stop gracefully stops the background flusher and flushes remaining data.
func (bc *BufferedCounter) Stop(ctx context.Context) error {
	close(bc.stopCh)
	<-bc.stoppedCh
	return bc.Flush(ctx)
}

// IncrementBuffered adds delta to an in-memory buffer without immediately writing to S3.
// The delta will be flushed during the next periodic flush or when Flush is called explicitly.
func (bc *BufferedCounter) IncrementBuffered(ctx context.Context, name string, delta int64) error {
	if delta == 0 {
		return nil
	}

	meta, err := bc.getEpochCached(ctx, name)
	if err != nil {
		return err
	}

	// pick a shard to increment, uses round-robin instead of random.
	shard := bc.pickShard(max(1, meta.ShardCount), nil)

	bc.mu.Lock()
	defer bc.mu.Unlock()

	if bc.buffers[name] == nil {
		bc.buffers[name] = make(map[int]*shardBuffer)
	}
	buf, ok := bc.buffers[name][shard]
	if !ok {
		buf = &shardBuffer{epoch: meta.Epoch, delta: 0}
		bc.buffers[name][shard] = buf
	}

	// If epoch changed, flush this shard immediately before adding new delta
	if buf.epoch != meta.Epoch {
		bc.mu.Unlock()
		if _, _, err := bc.Increment(ctx, name, buf.delta, &shard); err != nil {
			bc.logf("flush on epoch change: %v", err)
		}
		bc.mu.Lock()
		buf.epoch = meta.Epoch
		buf.delta = 0
	}

	buf.delta += delta

	// Auto-flush if buffer is large
	if buf.delta >= bc.maxBufferSize || buf.delta <= -bc.maxBufferSize {
		toFlush := buf.delta
		buf.delta = 0
		bc.mu.Unlock()
		if _, _, err := bc.Increment(ctx, name, toFlush, &shard); err != nil {
			bc.logf("auto-flush: %v", err)
		}
		bc.mu.Lock() // Relock before the deferred unlock
		return nil
	}

	return nil
}

// Flush writes all buffered increments to S3.
func (bc *BufferedCounter) Flush(ctx context.Context) error {
	bc.mu.Lock()
	toFlush := bc.buffers
	bc.buffers = make(map[string]map[int]*shardBuffer)
	bc.mu.Unlock()

	var g errgroup.Group

	for name, shards := range toFlush {
		for shard, buf := range shards {
			delta := buf.delta
			if delta == 0 {
				continue
			}
			g.Go(func() error {
				if _, _, err := bc.Increment(ctx, name, delta, &shard); err != nil {
					wrapped := fmt.Errorf("flush %s shard %d: %w", name, shard, err)
					bc.logf("%v", wrapped)
					return wrapped
				}
				return nil
			})
		}
	}
	return g.Wait()
}

func (bc *BufferedCounter) flusher() {
	defer close(bc.stoppedCh)

	if bc.flushInterval <= 0 {
		return // no periodic flushing
	}

	ticker := time.NewTicker(bc.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bc.stopCh:
			return
		case <-ticker.C:
			if err := bc.Flush(context.Background()); err != nil {
				bc.logf("periodic flush: %v", err)
			}
		}
	}
}

func (bc *BufferedCounter) logf(format string, args ...interface{}) {
	if bc.logger != nil {
		bc.logger.Printf(format, args...)
	}
}
