package shardedcounter

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Counter is the main entry point for S3 sharded counters.
// It is safe for concurrent use and cheap to reuse across logical counters.
type Counter struct {
	cfg Config
	s3  Client

	mu         sync.RWMutex // guards epochCache
	epochCache map[string]cachedEpoch
	shardRobin atomic.Uint64 // for lock-free round-robin shard selection
}

type cachedEpoch struct {
	meta      epochMeta
	expiresAt time.Time
}

// Pool for reusing byte buffers to reduce allocations
var bufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 32) // enough for any int64 string
		return &b
	},
}

// Config holds the tunable parameters for a Counter.
type Config struct {
	Bucket        string
	Prefix        string
	DefaultShards int
	MaxParallel   int
	LockTTL       time.Duration
	RetryBase     time.Duration
	RetryMax      time.Duration
	RetryJitter   float64
	EpochCacheTTL time.Duration
}

// Option mutates a Config when constructing a Counter.
type Option func(*Config)

// New creates a Counter with sensible defaults.
func New(client Client, bucket string, opts ...Option) *Counter {
	cfg := defaultConfig(bucket)
	for _, o := range opts {
		o(&cfg)
	}
	cfg.Prefix = strings.Trim(cfg.Prefix, "/")
	c := &Counter{
		cfg:        cfg,
		s3:         client,
		epochCache: make(map[string]cachedEpoch),
	}
	c.shardRobin.Store(rand.Uint64()) // seed with random value
	return c
}

// WithPrefix sets the S3 prefix used when storing counter state.
func WithPrefix(prefix string) Option {
	return func(c *Config) { c.Prefix = strings.Trim(prefix, "/") }
}

// WithDefaultShards overrides the default shard count used by Ensure.
func WithDefaultShards(n int) Option { return func(c *Config) { c.DefaultShards = n } }

// WithParallelism sets the max fan-out used for shard reads.
func WithParallelism(n int) Option { return func(c *Config) { c.MaxParallel = n } }

// WithBackoff configures the retry behavior used during optimistic updates.
func WithBackoff(base, maxDelay time.Duration, jitter float64) Option {
	return func(c *Config) {
		c.RetryBase = base
		c.RetryMax = maxDelay
		c.RetryJitter = jitter
	}
}

// WithEpochCacheTTL sets how long epoch metadata is cached before re-fetching from S3.
func WithEpochCacheTTL(ttl time.Duration) Option {
	return func(c *Config) { c.EpochCacheTTL = ttl }
}

func defaultConfig(bucket string) Config {
	return Config{
		Bucket:        bucket,
		Prefix:        "counters",
		DefaultShards: 32,
		MaxParallel:   16,
		LockTTL:       10 * time.Minute,
		RetryBase:     5 * time.Millisecond,
		RetryMax:      250 * time.Millisecond,
		RetryJitter:   0.2,
		EpochCacheTTL: 30 * time.Second,
	}
}

// Ensure creates base_total.txt and epoch.json if they don't exist.
func (c *Counter) Ensure(ctx context.Context, name string, shardCount int) error {
	if shardCount <= 0 {
		if shardCount == 0 {
			shardCount = c.cfg.DefaultShards
		} else {
			return fmt.Errorf("invalid shardCount: %d", shardCount)
		}
	}

	if err := c.putTextConditional(ctx, c.keyBaseTotal(name), []byte("0"), "If-None-Match", "*"); err != nil {
		if !statusIs(err, http.StatusPreconditionFailed) && !statusIs(err, http.StatusConflict) {
			return err
		}
	}

	_, etag, err := c.getJSON(ctx, c.keyEpoch(name))
	if err != nil && !isNotFound(err) {
		return err
	}
	if etag == "" {
		body := fmt.Appendf(nil, `{"epoch":%d,"shardCount":%d}`, 0, shardCount)
		if err := c.putJSON(ctx, c.keyEpoch(name), body, ""); err != nil {
			return err
		}
		_ = c.putText(ctx, c.keyShard(name, 0, 0), []byte("0"))
	}
	return nil
}

// Increment adds delta to a shard in the current epoch with an optimistic CAS loop.
func (c *Counter) Increment(ctx context.Context, name string, delta int64, shardOverride *int) (epoch int, shard int, err error) {
	if delta == 0 {
		return 0, 0, nil
	}

	meta, err := c.getEpochCached(ctx, name)
	if err != nil {
		return 0, 0, err
	}

	epoch = meta.Epoch
	shards := meta.ShardCount
	if shards <= 0 {
		shards = c.cfg.DefaultShards
	}
	shard = c.pickShard(shards, shardOverride)
	key := c.keyShard(name, epoch, shard)

	for attempt := 0; ; attempt++ {
		cur, etag, err := c.getText(ctx, key)
		notFound := false
		if err != nil {
			if !isNotFound(err) {
				return epoch, shard, err
			}
			notFound = true
		}

		current := parseInt64(cur)
		next := current + delta

		// Use buffer pool to avoid allocations
		bufPtr := bufferPool.Get().(*[]byte)
		buf := (*bufPtr)[:0]
		buf = strconv.AppendInt(buf, next, 10)

		var putErr error
		switch {
		case notFound:
			putErr = c.putTextConditional(ctx, key, buf, "If-None-Match", "*")
		case etag != "":
			putErr = c.putTextConditional(ctx, key, buf, "If-Match", etag)
		default:
			putErr = c.putText(ctx, key, buf)
		}
		bufferPool.Put(bufPtr)
		if putErr == nil {
			return epoch, shard, nil
		}
		if isPreconditionFailed(putErr) || isConflict(putErr) {
			select {
			case <-ctx.Done():
				return epoch, shard, ctx.Err()
			case <-time.After(c.backoff(attempt)):
			}
			continue
		}
		return epoch, shard, putErr
	}
}

// GetExact returns base_total plus the sum of all shards in the current epoch.
func (c *Counter) GetExact(ctx context.Context, name string) (int64, error) {
	var base int64
	var meta epochMeta
	var baseErr, metaErr error

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		base, baseErr = c.getBase(ctx, name)
	}()

	go func() {
		defer wg.Done()
		meta, _, metaErr = c.getEpoch(ctx, name)
	}()

	wg.Wait()

	if baseErr != nil {
		return 0, baseErr
	}
	if metaErr != nil {
		return 0, metaErr
	}

	sum, err := c.sumShards(ctx, name, meta.Epoch, max(1, meta.ShardCount))
	if err != nil {
		return 0, err
	}
	return base + sum, nil
}

// GetApprox samples k shards and scales the result to estimate the total.
func (c *Counter) GetApprox(ctx context.Context, name string, k int) (int64, error) {
	var base int64
	var meta epochMeta
	var baseErr, metaErr error

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		base, baseErr = c.getBase(ctx, name)
	}()

	go func() {
		defer wg.Done()
		meta, _, metaErr = c.getEpoch(ctx, name)
	}()

	wg.Wait()

	if baseErr != nil {
		return 0, baseErr
	}
	if metaErr != nil {
		return 0, metaErr
	}

	N := max(1, meta.ShardCount)
	if k <= 0 || k > N {
		k = min(8, N)
	}
	ids := c.sampleDistinct(N, k)
	partial, err := c.sumShardIDs(ctx, name, meta.Epoch, ids)
	if err != nil {
		return 0, err
	}
	est := float64(partial) * float64(N) / float64(k)
	return base + int64(math.Round(est)), nil
}

// Compact folds the current epoch into the base total, rotates epochs, and deletes stale shards.
func (c *Counter) Compact(ctx context.Context, name string) (folded int64, newEpoch int, err error) {
	if err := c.acquireLock(ctx, name); err != nil {
		return 0, 0, fmt.Errorf("compaction already in progress or lock failed: %w", err)
	}
	defer c.releaseLock(ctx, name)

	meta, etag, err := c.getEpoch(ctx, name)
	if err != nil {
		return 0, 0, err
	}
	N := max(1, meta.ShardCount)
	delta, err := c.sumShards(ctx, name, meta.Epoch, N)
	if err != nil {
		return 0, 0, err
	}

	if err := c.addToBase(ctx, name, delta); err != nil {
		return 0, 0, err
	}

	newEpoch = meta.Epoch + 1
	body := fmt.Appendf(nil, `{"epoch":%d,"shardCount":%d}`, newEpoch, N)
	if err := c.putJSON(ctx, c.keyEpoch(name), body, etag); err != nil && !isPreconditionFailed(err) {
		return delta, newEpoch, err
	}
	_ = c.putText(ctx, c.keyShard(name, newEpoch, 0), []byte("0"))
	_ = c.deletePrefix(ctx, c.dirEpoch(name, meta.Epoch)+"/shards/")

	// Invalidate epoch cache since we just rotated to a new epoch
	c.invalidateEpochCache(name)

	return delta, newEpoch, nil
}

func (c *Counter) addToBase(ctx context.Context, name string, add int64) error {
	if add == 0 {
		return nil
	}
	key := c.keyBaseTotal(name)
	for attempt := 0; ; attempt++ {
		cur, etag, err := c.getText(ctx, key)
		if err != nil && !isNotFound(err) {
			return err
		}
		next := parseInt64(cur) + add

		// Use buffer pool to avoid allocations
		bufPtr := bufferPool.Get().(*[]byte)
		buf := (*bufPtr)[:0]
		buf = strconv.AppendInt(buf, next, 10)

		var putErr error
		if etag == "" {
			putErr = c.putText(ctx, key, buf)
			bufferPool.Put(bufPtr)
			if putErr != nil {
				return putErr
			}
			return nil
		}
		putErr = c.putTextConditional(ctx, key, buf, "If-Match", etag)
		bufferPool.Put(bufPtr)

		if putErr == nil {
			return nil
		}
		if isPreconditionFailed(putErr) || isConflict(putErr) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.backoff(attempt)):
			}
			continue
		}
		return putErr
	}
}

func (c *Counter) getBase(ctx context.Context, name string) (int64, error) {
	text, _, err := c.getText(ctx, c.keyBaseTotal(name))
	if err != nil && !isNotFound(err) {
		return 0, err
	}
	return parseInt64(text), nil
}

func (c *Counter) getEpoch(ctx context.Context, name string) (epochMeta, string, error) {
	meta, etag, err := c.getJSON(ctx, c.keyEpoch(name))
	if err != nil {
		return epochMeta{}, "", err
	}
	if meta.ShardCount <= 0 {
		meta.ShardCount = c.cfg.DefaultShards
	}
	return meta, etag, nil
}

// getEpochCached returns epoch metadata from cache if available and fresh, otherwise fetches from S3.
func (c *Counter) getEpochCached(ctx context.Context, name string) (epochMeta, error) {
	// Try cache first with read lock
	c.mu.RLock()
	cached, ok := c.epochCache[name]
	c.mu.RUnlock()

	if ok && time.Now().Before(cached.expiresAt) {
		return cached.meta, nil
	}

	// Cache miss or expired - fetch from S3
	meta, _, err := c.getEpoch(ctx, name)
	if err != nil {
		return epochMeta{}, err
	}

	// Update cache with write lock
	ttl := c.cfg.EpochCacheTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	c.mu.Lock()
	c.epochCache[name] = cachedEpoch{
		meta:      meta,
		expiresAt: time.Now().Add(ttl),
	}
	c.mu.Unlock()

	return meta, nil
}

// invalidateEpochCache removes cached epoch metadata for a counter.
func (c *Counter) invalidateEpochCache(name string) {
	c.mu.Lock()
	delete(c.epochCache, name)
	c.mu.Unlock()
}

func (c *Counter) sumShards(ctx context.Context, name string, epoch, shardCount int) (int64, error) {
	ids := make([]int, shardCount)
	for i := range ids {
		ids[i] = i
	}
	return c.sumShardIDs(ctx, name, epoch, ids)
}

func (c *Counter) sumShardIDs(ctx context.Context, name string, epoch int, ids []int) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, max(1, c.cfg.MaxParallel))
	var wg sync.WaitGroup
	var sum atomic.Int64
	var firstErr atomic.Value // stores error

	for _, id := range ids {
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Check for early abort
			select {
			case <-ctx.Done():
				return
			case sem <- struct{}{}:
			}
			defer func() { <-sem }()

			text, _, err := c.getText(ctx, c.keyShard(name, epoch, id))
			if err != nil && !isNotFound(err) {
				// Store first error and cancel context to abort other goroutines
				if firstErr.CompareAndSwap(nil, err) {
					cancel()
				}
				return
			}
			sum.Add(parseInt64(text))
		}()
	}
	wg.Wait()

	if err := firstErr.Load(); err != nil {
		return 0, err.(error)
	}
	return sum.Load(), nil
}

func (c *Counter) pickShard(n int, override *int) int {
	if override != nil && *override >= 0 && *override < n {
		return *override
	}

	return int(c.shardRobin.Add(1) % uint64(n))
}

func (c *Counter) sampleDistinct(n, k int) []int {
	// For sampling, we want true randomness, not round-robin
	// does true randomness even exists in the universe?
	seen := make(map[int]struct{}, k)
	for len(seen) < k {
		seen[rand.Intn(n)] = struct{}{}
	}
	out := make([]int, 0, k)
	for j := range seen {
		out = append(out, j)
	}
	return out
}

func parseInt64(s string) int64 {
	if s == "" {
		return 0
	}
	i, _ := strconv.ParseInt(s, 10, 64)
	return i
}

func (c *Counter) backoff(attempt int) time.Duration {
	base, maxd := c.cfg.RetryBase, c.cfg.RetryMax
	if base <= 0 {
		base = 5 * time.Millisecond
	}
	if maxd <= 0 {
		maxd = 250 * time.Millisecond
	}
	mult := math.Pow(2, float64(attempt))
	d := min(time.Duration(float64(base)*mult), maxd)
	jit := 1.0 + (c.cfg.RetryJitter * (rand.Float64()*2 - 1))
	if jit < 0 {
		jit = 0
	}
	return time.Duration(float64(d) * jit)
}
