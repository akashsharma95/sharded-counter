package shardedcounter

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestCounterOptionMutators(t *testing.T) {
	ctx := context.Background()
	s3stub := newStubS3()

	counter := New(
		s3stub,
		"bucket",
		WithPrefix("/custom/"),
		WithParallelism(7),
		WithBackoff(10*time.Millisecond, 40*time.Millisecond, 0.5),
	)
	if counter.cfg.Prefix != "custom" {
		t.Fatalf("Prefix=%q want custom", counter.cfg.Prefix)
	}
	if counter.cfg.MaxParallel != 7 {
		t.Fatalf("MaxParallel=%d want 7", counter.cfg.MaxParallel)
	}
	if counter.cfg.RetryBase != 10*time.Millisecond {
		t.Fatalf("RetryBase=%v want 10ms", counter.cfg.RetryBase)
	}
	if counter.cfg.RetryMax != 40*time.Millisecond {
		t.Fatalf("RetryMax=%v want 40ms", counter.cfg.RetryMax)
	}
	if counter.cfg.RetryJitter != 0.5 {
		t.Fatalf("RetryJitter=%v want 0.5", counter.cfg.RetryJitter)
	}

	// Coverage for WithDefaultShards already exists but double check config still sensible.
	if counter.cfg.DefaultShards != 32 {
		t.Fatalf("DefaultShards unexpectedly changed: %d", counter.cfg.DefaultShards)
	}

	// Ensure Ensure still works with mutated config.
	if err := counter.Ensure(ctx, "opts", 2); err != nil {
		t.Fatalf("Ensure opts: %v", err)
	}
}

func TestCounterBackoffBounds(t *testing.T) {
	s3stub := newStubS3()
	counter := New(s3stub, "bucket", WithBackoff(10*time.Millisecond, 40*time.Millisecond, 0))

	if got, want := counter.backoff(0), 10*time.Millisecond; got != want {
		t.Fatalf("backoff(0)=%v want %v", got, want)
	}

	if got, want := counter.backoff(3), 40*time.Millisecond; got != want {
		t.Fatalf("backoff(3)=%v want %v", got, want)
	}

	// Force default fallbacks when config values are non-positive.
	counter.cfg.RetryBase = 0
	counter.cfg.RetryMax = 0
	if got, want := counter.backoff(0), 5*time.Millisecond; got != want {
		t.Fatalf("backoff with zero cfg=%v want %v", got, want)
	}
	if got := counter.backoff(10); got > 250*time.Millisecond {
		t.Fatalf("backoff clamped max, got %v", got)
	}
}

func TestCompactorTriggerRunsCompaction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s3stub := newStubS3()
	counter := New(s3stub, "bucket")
	if err := counter.Ensure(ctx, "triggered", 4); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	for i := 0; i < 25; i++ {
		if _, _, err := counter.Increment(ctx, "triggered", 1, nil); err != nil {
			t.Fatalf("Increment: %v", err)
		}
	}

	var logs lockedBuffer
	logger := log.New(&logs, "", 0)

	comp := NewCompactor(counter, "triggered", 10*time.Millisecond, 0, 4, logger)
	comp.Start(ctx)
	defer comp.Stop()

	comp.Trigger()

	waitUntil(t, 2*time.Second, func() bool {
		return s3stub.mustRead("counters/triggered/base_total.txt") == "25"
	})

	if logs.Len() == 0 {
		t.Fatalf("expected compactor to log output, got none")
	}
}

func TestCompactorThresholdSampling(t *testing.T) {
	ctx := context.Background()
	s3stub := newStubS3()
	counter := New(s3stub, "bucket")
	if err := counter.Ensure(ctx, "threshold", 4); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	var logs lockedBuffer
	logger := log.New(&logs, "", 0)
	comp := NewCompactor(counter, "threshold", 0, 50, 4, logger)

	comp.maybeCompact(ctx)
	if got := s3stub.mustRead("counters/threshold/base_total.txt"); got != "0" {
		t.Fatalf("unexpected base_total before threshold: %q", got)
	}

	for i := 0; i < 60; i++ {
		if _, _, err := counter.Increment(ctx, "threshold", 1, nil); err != nil {
			t.Fatalf("Increment: %v", err)
		}
	}

	comp.maybeCompact(ctx)

	waitUntil(t, time.Second, func() bool {
		return s3stub.mustRead("counters/threshold/base_total.txt") == "60"
	})

	if logs.Len() == 0 {
		t.Fatalf("expected compactor logs for threshold run")
	}
}

func TestCompactorNextTick(t *testing.T) {
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()

	comp := NewCompactor(New(newStubS3(), "bucket"), "next", 0, 0, 1, nil)
	select {
	case <-comp.nextTick(nil):
		t.Fatalf("nextTick(nil) unexpectedly delivered")
	case <-time.After(10 * time.Millisecond):
		// success: nil ticker channel should not produce values
	}

	select {
	case <-comp.nextTick(ticker):
	case <-time.After(10 * time.Millisecond):
		t.Fatalf("nextTick(ticker) did not deliver tick")
	}
}

func TestReleaseLockNotFound(t *testing.T) {
	ctx := context.Background()
	s3stub := &deleteMissingStub{stubS3: newStubS3()}
	counter := New(s3stub, "bucket")
	if err := counter.releaseLock(ctx, "missing"); err != nil {
		t.Fatalf("releaseLock: %v", err)
	}
}

func TestStatusHelpers(t *testing.T) {
	if !isPreconditionFailed(stubError{code: http.StatusPreconditionFailed}) {
		t.Fatalf("expected precondition failed match")
	}
	if !isConflict(stubError{code: http.StatusConflict}) {
		t.Fatalf("expected conflict match")
	}
	if !isNotFound(stubError{code: http.StatusNotFound}) {
		t.Fatalf("expected not found match")
	}
	if isNotFound(stubError{code: http.StatusTeapot}) {
		t.Fatalf("unexpected not found match")
	}
}

func TestBufferedLogf(t *testing.T) {
	var logs bytes.Buffer
	bc := NewBuffered(New(newStubS3(), "bucket"), 0, 1, log.New(&logs, "", 0))
	bc.logf("hello %s", "buffer")
	if logs.Len() == 0 {
		t.Fatalf("expected log output")
	}
}

func TestAddToBaseRetriesOnConflict(t *testing.T) {
	ctx := context.Background()
	s3stub := &flakyCASStub{stubS3: newStubS3(), failNext: true}
	counter := New(s3stub, "bucket")
	if err := counter.Ensure(ctx, "cas", 1); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	if err := counter.addToBase(ctx, "cas", 7); err != nil {
		t.Fatalf("addToBase: %v", err)
	}
	if got := s3stub.mustRead("counters/cas/base_total.txt"); got != "7" {
		t.Fatalf("base_total=%q want 7", got)
	}
	if s3stub.failures != 1 {
		t.Fatalf("expected exactly one CAS failure, got %d", s3stub.failures)
	}
}

type deleteMissingStub struct {
	*stubS3
}

func (s *deleteMissingStub) DeleteObject(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return nil, stubError{code: http.StatusNotFound}
}

type flakyCASStub struct {
	*stubS3
	mu       sync.Mutex
	failNext bool
	failures int
}

func (s *flakyCASStub) PutObject(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if input != nil && input.IfMatch != nil && *input.IfMatch != "" {
		s.mu.Lock()
		if s.failNext {
			s.failNext = false
			s.failures++
			s.mu.Unlock()
			return nil, stubError{code: http.StatusPreconditionFailed}
		}
		s.mu.Unlock()
	}
	return s.stubS3.PutObject(ctx, input, opts...)
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

type lockedBuffer struct {
	buf bytes.Buffer
	mu  sync.Mutex
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}
