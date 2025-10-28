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

**Note**: Since `BufferedCounter` accumulates increments in-memory, any data not yet flushed to S3 will be lost if the service crashes.
This is acceptable for approximate metrics (analytics, rate limits, etc.) but not for scenarios requiring guaranteed durability.

Data is only written to S3 during flushes: The buffered increments are held in memory until:
- The periodic flush timer triggers (based on flushInterval)
- A buffer reaches maxBufferSize and auto-flushes
- Flush() is called explicitly
- Stop() is called for graceful shutdown

## Why S3 Instead of Redis?

Redis is a popular choice for counters, but sharded-counter uses S3 for fundamentally different workload characteristics:

| Aspect | S3 | Redis |
|--------|----|----|
| **Durability** | 11 nines (99.999999999%) | In-memory; loses data on restart unless persisted |
| **Scalability** | Automatic, unlimited | Requires horizontal scaling & complex sharding |
| **Cost per write** | $0.000005 per operation | Hourly instance cost ($0.20+/hour minimum) |
| **Storage persistence** | Permanent at minimal cost | Memory expensive (~$0.05/GB-month) |
| **Best for** | Append-heavy, high-volume, low-read workloads | Real-time access, frequently-read data |

**When to use S3-backed counters:**
- Analytics, metrics, and event counting
- Rate limiting and quota tracking
- Audit logs and activity tracking
- Workloads where approximate eventual consistency is acceptable

**When to use Redis:**
- Low-latency counters requiring sub-millisecond reads
- Session state and caching
- Real-time leaderboards or rankings

## AWS S3 Pricing & Cost Scenarios

Current S3 Standard pricing (Oct 2025):

| Component | Cost |
|-----------|------|
| **Storage** | $0.023/GB/month |
| **PUT/POST/LIST** | $0.005 per 1,000 requests |
| **GET/SELECT** | $0.0004 per 1,000 requests |
| **Data Transfer Out** | $0.09/GB (first 10TB), $0.085/GB (next 40TB) |

### Scenario 1: Small Analytics Counter
100 million events/month distributed across 50 shards, with daily compaction.

```
Storage: 50 shard objects @ 2KB each = 100 KB
  → ~0.0001 GB storage = $0.000002/month

Writes: 100M events with sharded buffering (batched 100x)
  → ~1M PUT requests = 1,000 * $0.005 = $5.00/month

Reads: Daily compaction = 30 reads/month
  → ~50 GET requests = $0.00002/month

Total: ~$5.00/month
```

### Scenario 2: High-Volume Event Stream
1 billion events/month (12K events/second), 64 shards, buffered with 5s flush.

```
Storage: 64 shard objects @ 10KB each = 640 KB + base object
  → ~0.001 GB storage = $0.000023/month

Writes: 1B events batched 100x (BufferedCounter)
  → ~10M PUT requests = 10,000 * $0.005 = $50.00/month

Reads: Hourly compaction sampling + GetExact calls
  → ~2K GET requests/month = $0.0008/month

Equivalent Redis Cost:
  → cache.t3.medium: $0.067/hour = $48.50/month (just for uptime)
  → Plus: data transfer, backups, high availability setup

Total S3: ~$50.00/month
Total Redis: $48.50+ (+ operational overhead)
```

**Key Insight:** Even with high volume, S3 is cost-competitive with Redis while providing unlimited persistence and durability.

## Testing

The package ships with an in-memory stub that demonstrates how to satisfy the
`Client` interface. When writing your own tests, follow the same pattern:

* Honour S3 conditional headers (`If-Match` / `If-None-Match`) to exercise
  optimistic updates.
* Provide deterministic behaviour for listing and deleting to model compaction.

See [`counter_test.go`](counter_test.go) for concrete examples that cover
`Ensure`, `Increment`, approximations, and compaction.
