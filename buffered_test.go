package shardedcounter

import (
	"context"
	"testing"
)

func TestIncrementBufferedFlushOnEpochChange(t *testing.T) {
	ctx := context.Background()
	s3stub := newStubS3()
	counter := New(s3stub, "bucket", WithDefaultShards(1))
	if err := counter.Ensure(ctx, "epoch-shift", 1); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	buffered := NewBuffered(counter, 0, 100, nil)
	if err := buffered.IncrementBuffered(ctx, "epoch-shift", 3); err != nil {
		t.Fatalf("IncrementBuffered first: %v", err)
	}

	if total, err := counter.GetExact(ctx, "epoch-shift"); err != nil || total != 0 {
		t.Fatalf("GetExact before flush=%d, err=%v want 0 nil", total, err)
	}

	if _, _, err := counter.Compact(ctx, "epoch-shift"); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if err := buffered.IncrementBuffered(ctx, "epoch-shift", 2); err != nil {
		t.Fatalf("IncrementBuffered second: %v", err)
	}

	total, err := counter.GetExact(ctx, "epoch-shift")
	if err != nil {
		t.Fatalf("GetExact after epoch change: %v", err)
	}
	if total != 3 {
		t.Fatalf("GetExact=%d want 3 (flushed previous buffer)", total)
	}

	if err := buffered.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if total, err := counter.GetExact(ctx, "epoch-shift"); err != nil || total != 5 {
		t.Fatalf("GetExact after final flush=%d err=%v want 5 nil", total, err)
	}
}

func TestIncrementBufferedAutoFlush(t *testing.T) {
	ctx := context.Background()
	s3stub := newStubS3()
	counter := New(s3stub, "bucket", WithDefaultShards(1))
	if err := counter.Ensure(ctx, "auto", 1); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	buffered := NewBuffered(counter, 0, 5, nil)

	if err := buffered.IncrementBuffered(ctx, "auto", 3); err != nil {
		t.Fatalf("IncrementBuffered first: %v", err)
	}
	if err := buffered.IncrementBuffered(ctx, "auto", 3); err != nil {
		t.Fatalf("IncrementBuffered second: %v", err)
	}

	total, err := counter.GetExact(ctx, "auto")
	if err != nil {
		t.Fatalf("GetExact: %v", err)
	}
	if total != 6 {
		t.Fatalf("GetExact=%d want 6 after auto flush", total)
	}

	// Buffers should be empty after auto-flush
	if err := buffered.Flush(ctx); err != nil {
		t.Fatalf("Flush on empty buffers: %v", err)
	}
}
