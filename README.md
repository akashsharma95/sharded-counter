# sharded-counter

An S3-backed counter that scales by sharding updates across many small objects
and periodically compacting them into a single base total. The design makes all
operations optimistic and cheap—perfect for high write, low read workloads such
as analytics counters, rate limits, or metering.

## Installation

```
go get github.com/akashsharma95/sharded-counter
```

## Quick start

```go
ctx := context.Background()
svc := s3.NewFromConfig(cfg) // any implementation of the Client interface

counter := s3counter.New(svc, "my-bucket",
	s3counter.WithPrefix("metrics"),
	s3counter.WithDefaultShards(64),
	s3counter.WithParallelism(32),           // max concurrent shard reads
	s3counter.WithEpochCacheTTL(time.Minute), // cache epoch metadata
)

// Ensure the counter exists (idempotent).
if err := counter.Ensure(ctx, "pageviews", 0); err != nil {
	log.Fatal(err)
}

// Record some events. Shard selection is random unless you override it.
if _, _, err := counter.Increment(ctx, "pageviews", 1, nil); err != nil {
	log.Fatal(err)
}

// Read the exact value (base_total + current epoch shards).
total, err := counter.GetExact(ctx, "pageviews")
if err != nil {
	log.Fatal(err)
}
fmt.Println("pageviews:", total)
```

## Background compaction

Sharded writes remain indefinitely until compaction folds them into the base
total. Use the provided `Compactor` helper to run this in the background.

```go
logger := log.New(os.Stdout, "compact ", log.LstdFlags)
compactor := s3counter.NewCompactor(counter, "pageviews",
	time.Minute,    // compact at least once per minute
	10_000,         // or sooner if the current epoch holds >= 10k events
	8,              // sample 8 shards to estimate the delta
	logger,
)
compactor.Start(ctx)
defer compactor.Stop()
```

You can also trigger compaction manually via `Compactor.Trigger()` (for example
after a burst of writes) or by calling `counter.Compact` yourself.

## High-throughput writes with buffering

For workloads with very high write rates, use `BufferedCounter` to batch
increments in memory before flushing to S3:

```go
buffered := s3counter.NewBuffered(
	counter,
	5*time.Second, // flush interval
	1000,          // auto-flush when buffer reaches this size
	logger,
)
buffered.Start()
defer buffered.Stop(ctx)

// These accumulate in memory and flush periodically
for i := 0; i < 100000; i++ {
	if err := buffered.IncrementBuffered(ctx, "pageviews", 1); err != nil {
		log.Fatal(err)
	}
}

// Force immediate flush
buffered.Flush(ctx)
```

**Throughput gain**: 10-100x depending on workload. Best for analytics,
event counting, and other scenarios where eventual consistency is acceptable.

## Testing

The package ships with an in-memory stub that demonstrates how to satisfy the
`Client` interface. When writing your own tests, follow the same pattern:

* Honour S3 conditional headers (`If-Match` / `If-None-Match`) to exercise
  optimistic updates.
* Provide deterministic behaviour for listing and deleting to model compaction.

See [`counter_test.go`](counter_test.go) for concrete examples that cover
`Ensure`, `Increment`, approximations, and compaction.
