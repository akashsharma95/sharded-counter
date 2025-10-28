//go:build integration
// +build integration

package shardedcounter_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	shardedcounter "github.com/akashsharma95/sharded-counter"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	testBucket    = "test-counter-bucket"
	localstackURL = "http://localhost:4566"
	awsRegion     = "us-east-1"
)

// TestMain sets up and tears down the test environment
func TestMain(m *testing.M) {
	// Give LocalStack time to fully start
	time.Sleep(2 * time.Second)

	// Run tests
	code := m.Run()

	os.Exit(code)
}

// newLocalStackS3Client creates an S3 client configured for LocalStack
func newLocalStackS3Client(t *testing.T) *s3.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(awsRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"test", // access key
			"test", // secret key
			"",     // session token
		)),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(localstackURL)
		o.UsePathStyle = true // LocalStack requires path-style addressing
	})
}

// setupTestBucket creates a test bucket and returns a cleanup function
func setupTestBucket(t *testing.T, client *s3.Client) func() {
	t.Helper()
	ctx := context.Background()

	// Create bucket
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(testBucket),
	})
	if err != nil {
		t.Fatalf("failed to create test bucket: %v", err)
	}

	// Return cleanup function
	return func() {
		// List and delete all objects
		paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
			Bucket: aws.String(testBucket),
		})

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				t.Logf("failed to list objects for cleanup: %v", err)
				return
			}

			if len(page.Contents) > 0 {
				var objects []s3Types.ObjectIdentifier
				for _, obj := range page.Contents {
					objects = append(objects, s3Types.ObjectIdentifier{
						Key: obj.Key,
					})
				}

				_, err = client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
					Bucket: aws.String(testBucket),
					Delete: &s3Types.Delete{
						Objects: objects,
						Quiet:   aws.Bool(true),
					},
				})
				if err != nil {
					t.Logf("failed to delete objects: %v", err)
				}
			}
		}

		// Delete bucket
		_, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(testBucket),
		})
		if err != nil {
			t.Logf("failed to delete test bucket: %v", err)
		}
	}
}

// TestIntegrationEnsure verifies counter initialization
func TestIntegrationEnsure(t *testing.T) {
	client := newLocalStackS3Client(t)
	cleanup := setupTestBucket(t, client)
	defer cleanup()

	counter := shardedcounter.New(client, testBucket,
		shardedcounter.WithPrefix("counters"),
		shardedcounter.WithDefaultShards(8),
	)

	ctx := context.Background()
	counterName := "test-ensure-counter"

	// First ensure should succeed
	err := counter.Ensure(ctx, counterName, 8)
	if err != nil {
		t.Fatalf("first Ensure failed: %v", err)
	}

	// Second ensure should be idempotent
	err = counter.Ensure(ctx, counterName, 8)
	if err != nil {
		t.Fatalf("second Ensure failed: %v", err)
	}

	// Verify initial value is 0
	total, err := counter.GetExact(ctx, counterName)
	if err != nil {
		t.Fatalf("GetExact failed: %v", err)
	}
	if total != 0 {
		t.Errorf("expected initial value 0, got %d", total)
	}
}

// TestIntegrationIncrement tests basic increment operations
func TestIntegrationIncrement(t *testing.T) {
	client := newLocalStackS3Client(t)
	cleanup := setupTestBucket(t, client)
	defer cleanup()

	counter := shardedcounter.New(client, testBucket,
		shardedcounter.WithPrefix("counters"),
		shardedcounter.WithDefaultShards(4),
	)

	ctx := context.Background()
	counterName := "test-increment"

	// Initialize counter
	if err := counter.Ensure(ctx, counterName, 4); err != nil {
		t.Fatalf("Ensure failed: %v", err)
	}

	// Test single increment
	epoch, shard, err := counter.Increment(ctx, counterName, 1, nil)
	if err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if epoch != 0 {
		t.Errorf("expected epoch 0, got %d", epoch)
	}
	if shard < 0 || shard >= 4 {
		t.Errorf("invalid shard %d, expected 0-3", shard)
	}

	// Test multiple increments
	for i := 0; i < 10; i++ {
		_, _, err := counter.Increment(ctx, counterName, 5, nil)
		if err != nil {
			t.Fatalf("Increment %d failed: %v", i, err)
		}
	}

	// Verify total
	total, err := counter.GetExact(ctx, counterName)
	if err != nil {
		t.Fatalf("GetExact failed: %v", err)
	}
	expected := int64(1 + 10*5)
	if total != expected {
		t.Errorf("expected total %d, got %d", expected, total)
	}
}

// TestIntegrationConcurrentIncrements tests concurrent increment operations
func TestIntegrationConcurrentIncrements(t *testing.T) {
	client := newLocalStackS3Client(t)
	cleanup := setupTestBucket(t, client)
	defer cleanup()

	counter := shardedcounter.New(client, testBucket,
		shardedcounter.WithPrefix("counters"),
		shardedcounter.WithDefaultShards(16),
		shardedcounter.WithParallelism(8),
	)

	ctx := context.Background()
	counterName := "test-concurrent"

	if err := counter.Ensure(ctx, counterName, 16); err != nil {
		t.Fatalf("Ensure failed: %v", err)
	}

	// Concurrent increments
	const numGoroutines = 50
	const incrementsPerGoroutine = 20

	var wg sync.WaitGroup
	var errors atomic.Int64

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerGoroutine; j++ {
				if _, _, err := counter.Increment(ctx, counterName, 1, nil); err != nil {
					errors.Add(1)
					t.Logf("concurrent increment error: %v", err)
				}
			}
		}()
	}

	wg.Wait()

	if errors.Load() > 0 {
		t.Fatalf("encountered %d errors during concurrent increments", errors.Load())
	}

	// Verify final total
	total, err := counter.GetExact(ctx, counterName)
	if err != nil {
		t.Fatalf("GetExact failed: %v", err)
	}

	expected := int64(numGoroutines * incrementsPerGoroutine)
	if total != expected {
		t.Errorf("expected total %d, got %d", expected, total)
	}
}

// TestIntegrationGetApprox tests approximate counter reads
func TestIntegrationGetApprox(t *testing.T) {
	client := newLocalStackS3Client(t)
	cleanup := setupTestBucket(t, client)
	defer cleanup()

	counter := shardedcounter.New(client, testBucket,
		shardedcounter.WithPrefix("counters"),
		shardedcounter.WithDefaultShards(32),
	)

	ctx := context.Background()
	counterName := "test-approx"

	if err := counter.Ensure(ctx, counterName, 32); err != nil {
		t.Fatalf("Ensure failed: %v", err)
	}

	// Add increments distributed across shards
	const totalIncrements = 1000
	for i := 0; i < totalIncrements; i++ {
		shard := i % 32
		if _, _, err := counter.Increment(ctx, counterName, 1, &shard); err != nil {
			t.Fatalf("Increment failed: %v", err)
		}
	}

	// Get exact value
	exact, err := counter.GetExact(ctx, counterName)
	if err != nil {
		t.Fatalf("GetExact failed: %v", err)
	}

	// Get approximate value (sampling 8 shards)
	approx, err := counter.GetApprox(ctx, counterName, 8)
	if err != nil {
		t.Fatalf("GetApprox failed: %v", err)
	}

	t.Logf("Exact: %d, Approx: %d", exact, approx)

	// Approximation should be within reasonable range
	// Since we distributed evenly, it should be quite accurate
	diff := float64(approx-exact) / float64(exact)
	if diff < 0 {
		diff = -diff
	}

	// Allow 20% margin for approximation with even distribution
	if diff > 0.2 {
		t.Errorf("approximation too far off: exact=%d, approx=%d, diff=%.2f%%",
			exact, approx, diff*100)
	}
}

// TestIntegrationCompact tests the compaction feature
func TestIntegrationCompact(t *testing.T) {
	client := newLocalStackS3Client(t)
	cleanup := setupTestBucket(t, client)
	defer cleanup()

	counter := shardedcounter.New(client, testBucket,
		shardedcounter.WithPrefix("counters"),
		shardedcounter.WithDefaultShards(8),
	)

	ctx := context.Background()
	counterName := "test-compact"

	if err := counter.Ensure(ctx, counterName, 8); err != nil {
		t.Fatalf("Ensure failed: %v", err)
	}

	// Add some increments in epoch 0
	for i := 0; i < 100; i++ {
		if _, _, err := counter.Increment(ctx, counterName, 1, nil); err != nil {
			t.Fatalf("Increment failed: %v", err)
		}
	}

	// Get value before compaction
	beforeCompact, err := counter.GetExact(ctx, counterName)
	if err != nil {
		t.Fatalf("GetExact before compact failed: %v", err)
	}

	// Perform compaction
	folded, newEpoch, err := counter.Compact(ctx, counterName)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	t.Logf("Compacted %d into base, new epoch: %d", folded, newEpoch)

	if folded != 100 {
		t.Errorf("expected to fold 100, got %d", folded)
	}
	if newEpoch != 1 {
		t.Errorf("expected new epoch 1, got %d", newEpoch)
	}

	// Get value after compaction
	afterCompact, err := counter.GetExact(ctx, counterName)
	if err != nil {
		t.Fatalf("GetExact after compact failed: %v", err)
	}

	if beforeCompact != afterCompact {
		t.Errorf("value changed after compact: before=%d, after=%d",
			beforeCompact, afterCompact)
	}

	// Add more increments in new epoch
	for i := 0; i < 50; i++ {
		if _, _, err := counter.Increment(ctx, counterName, 1, nil); err != nil {
			t.Fatalf("Increment in new epoch failed: %v", err)
		}
	}

	// Verify new total
	finalTotal, err := counter.GetExact(ctx, counterName)
	if err != nil {
		t.Fatalf("GetExact final failed: %v", err)
	}

	expected := int64(150)
	if finalTotal != expected {
		t.Errorf("expected final total %d, got %d", expected, finalTotal)
	}
}

// TestIntegrationMultipleCompactions tests multiple sequential compactions
func TestIntegrationMultipleCompactions(t *testing.T) {
	client := newLocalStackS3Client(t)
	cleanup := setupTestBucket(t, client)
	defer cleanup()

	counter := shardedcounter.New(client, testBucket,
		shardedcounter.WithPrefix("counters"),
		shardedcounter.WithDefaultShards(4),
	)

	ctx := context.Background()
	counterName := "test-multi-compact"

	if err := counter.Ensure(ctx, counterName, 4); err != nil {
		t.Fatalf("Ensure failed: %v", err)
	}

	expectedTotal := int64(0)

	// Perform multiple cycles of increment + compact
	for cycle := 0; cycle < 5; cycle++ {
		// Add increments
		incrementsThisCycle := int64(20 + cycle*10)
		for i := int64(0); i < incrementsThisCycle; i++ {
			if _, _, err := counter.Increment(ctx, counterName, 1, nil); err != nil {
				t.Fatalf("Increment in cycle %d failed: %v", cycle, err)
			}
		}
		expectedTotal += incrementsThisCycle

		// Compact
		folded, newEpoch, err := counter.Compact(ctx, counterName)
		if err != nil {
			t.Fatalf("Compact cycle %d failed: %v", cycle, err)
		}

		if folded != incrementsThisCycle {
			t.Errorf("cycle %d: expected to fold %d, got %d",
				cycle, incrementsThisCycle, folded)
		}
		if newEpoch != cycle+1 {
			t.Errorf("cycle %d: expected epoch %d, got %d",
				cycle, cycle+1, newEpoch)
		}

		// Verify total
		total, err := counter.GetExact(ctx, counterName)
		if err != nil {
			t.Fatalf("GetExact cycle %d failed: %v", cycle, err)
		}
		if total != expectedTotal {
			t.Errorf("cycle %d: expected total %d, got %d",
				cycle, expectedTotal, total)
		}
	}
}

// TestIntegrationBufferedCounter tests the BufferedCounter functionality
func TestIntegrationBufferedCounter(t *testing.T) {
	client := newLocalStackS3Client(t)
	cleanup := setupTestBucket(t, client)
	defer cleanup()

	counter := shardedcounter.New(client, testBucket,
		shardedcounter.WithPrefix("counters"),
		shardedcounter.WithDefaultShards(8),
	)

	ctx := context.Background()
	counterName := "test-buffered"

	if err := counter.Ensure(ctx, counterName, 8); err != nil {
		t.Fatalf("Ensure failed: %v", err)
	}

	logger := log.New(os.Stdout, "[BUFFERED-TEST] ", log.LstdFlags)
	buffered := shardedcounter.NewBuffered(
		counter,
		100*time.Millisecond, // short flush interval for testing
		100,                  // small buffer size for testing
		logger,
	)
	buffered.Start()
	defer func() {
		if err := buffered.Stop(ctx); err != nil {
			t.Fatalf("Stop failed: %v", err)
		}
	}()

	// Add many buffered increments
	const numIncrements = 500
	for i := 0; i < numIncrements; i++ {
		if err := buffered.IncrementBuffered(ctx, counterName, 1); err != nil {
			t.Fatalf("IncrementBuffered %d failed: %v", i, err)
		}
	}

	// Immediate read might not show all increments (they're buffered)
	intermediate, err := counter.GetExact(ctx, counterName)
	if err != nil {
		t.Fatalf("GetExact intermediate failed: %v", err)
	}
	t.Logf("Intermediate value (before flush): %d", intermediate)

	// Explicitly flush
	if err := buffered.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Give a moment for flush to complete
	time.Sleep(100 * time.Millisecond)

	// Now all increments should be visible
	total, err := counter.GetExact(ctx, counterName)
	if err != nil {
		t.Fatalf("GetExact failed: %v", err)
	}

	if total != numIncrements {
		t.Errorf("expected total %d, got %d", numIncrements, total)
	}
}

// TestIntegrationBufferedAutoFlush tests automatic flush on buffer size
func TestIntegrationBufferedAutoFlush(t *testing.T) {
	client := newLocalStackS3Client(t)
	cleanup := setupTestBucket(t, client)
	defer cleanup()

	counter := shardedcounter.New(client, testBucket,
		shardedcounter.WithPrefix("counters"),
		shardedcounter.WithDefaultShards(4),
	)

	ctx := context.Background()
	counterName := "test-buffered-autoflush"

	if err := counter.Ensure(ctx, counterName, 4); err != nil {
		t.Fatalf("Ensure failed: %v", err)
	}

	logger := log.New(os.Stdout, "[AUTOFLUSH-TEST] ", log.LstdFlags)
	buffered := shardedcounter.NewBuffered(
		counter,
		0,  // no periodic flush
		10, // very small buffer - auto flush every 10 increments
		logger,
	)
	buffered.Start()
	defer func() {
		if err := buffered.Stop(ctx); err != nil {
			t.Fatalf("Stop failed: %v", err)
		}
	}()

	// Add increments that should trigger multiple auto-flushes
	const numIncrements = 100
	for i := 0; i < numIncrements; i++ {
		if err := buffered.IncrementBuffered(ctx, counterName, 1); err != nil {
			t.Fatalf("IncrementBuffered %d failed: %v", i, err)
		}
	}

	// Give time for auto-flushes to complete
	time.Sleep(500 * time.Millisecond)

	total, err := counter.GetExact(ctx, counterName)
	if err != nil {
		t.Fatalf("GetExact failed: %v", err)
	}

	// Should be close to expected even without explicit flush
	// Allow some variance due to timing
	if total < numIncrements-20 {
		t.Errorf("expected total close to %d, got %d", numIncrements, total)
	}

	// Final flush to ensure everything is committed
	if err := buffered.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	finalTotal, err := counter.GetExact(ctx, counterName)
	if err != nil {
		t.Fatalf("GetExact final failed: %v", err)
	}

	if finalTotal != numIncrements {
		t.Errorf("expected final total %d, got %d", numIncrements, finalTotal)
	}
}

// TestIntegrationNegativeIncrements tests decrement operations
func TestIntegrationNegativeIncrements(t *testing.T) {
	client := newLocalStackS3Client(t)
	cleanup := setupTestBucket(t, client)
	defer cleanup()

	counter := shardedcounter.New(client, testBucket,
		shardedcounter.WithPrefix("counters"),
		shardedcounter.WithDefaultShards(4),
	)

	ctx := context.Background()
	counterName := "test-negative"

	if err := counter.Ensure(ctx, counterName, 4); err != nil {
		t.Fatalf("Ensure failed: %v", err)
	}

	// Add positive increments
	for i := 0; i < 100; i++ {
		if _, _, err := counter.Increment(ctx, counterName, 1, nil); err != nil {
			t.Fatalf("Positive increment failed: %v", err)
		}
	}

	// Subtract some
	for i := 0; i < 30; i++ {
		if _, _, err := counter.Increment(ctx, counterName, -1, nil); err != nil {
			t.Fatalf("Negative increment failed: %v", err)
		}
	}

	total, err := counter.GetExact(ctx, counterName)
	if err != nil {
		t.Fatalf("GetExact failed: %v", err)
	}

	expected := int64(70)
	if total != expected {
		t.Errorf("expected total %d, got %d", expected, total)
	}
}

// TestIntegrationMultipleCounters tests multiple independent counters
func TestIntegrationMultipleCounters(t *testing.T) {
	client := newLocalStackS3Client(t)
	cleanup := setupTestBucket(t, client)
	defer cleanup()

	counter := shardedcounter.New(client, testBucket,
		shardedcounter.WithPrefix("counters"),
		shardedcounter.WithDefaultShards(4),
	)

	ctx := context.Background()

	counters := []struct {
		name     string
		expected int64
	}{
		{"counter-a", 100},
		{"counter-b", 200},
		{"counter-c", 50},
	}

	// Initialize all counters
	for _, c := range counters {
		if err := counter.Ensure(ctx, c.name, 4); err != nil {
			t.Fatalf("Ensure %s failed: %v", c.name, err)
		}
	}

	// Increment each counter independently
	for _, c := range counters {
		for i := int64(0); i < c.expected; i++ {
			if _, _, err := counter.Increment(ctx, c.name, 1, nil); err != nil {
				t.Fatalf("Increment %s failed: %v", c.name, err)
			}
		}
	}

	// Verify each counter independently
	for _, c := range counters {
		total, err := counter.GetExact(ctx, c.name)
		if err != nil {
			t.Fatalf("GetExact %s failed: %v", c.name, err)
		}
		if total != c.expected {
			t.Errorf("counter %s: expected %d, got %d", c.name, c.expected, total)
		}
	}
}

// TestIntegrationCounterWithPrefix tests counter isolation with prefixes
func TestIntegrationCounterWithPrefix(t *testing.T) {
	client := newLocalStackS3Client(t)
	cleanup := setupTestBucket(t, client)
	defer cleanup()

	ctx := context.Background()

	// Create two counters with different prefixes
	counter1 := shardedcounter.New(client, testBucket,
		shardedcounter.WithPrefix("app1/metrics"),
		shardedcounter.WithDefaultShards(4),
	)

	counter2 := shardedcounter.New(client, testBucket,
		shardedcounter.WithPrefix("app2/metrics"),
		shardedcounter.WithDefaultShards(4),
	)

	counterName := "requests"

	// Initialize both
	if err := counter1.Ensure(ctx, counterName, 4); err != nil {
		t.Fatalf("Ensure counter1 failed: %v", err)
	}
	if err := counter2.Ensure(ctx, counterName, 4); err != nil {
		t.Fatalf("Ensure counter2 failed: %v", err)
	}

	// Increment counter1
	for i := 0; i < 100; i++ {
		if _, _, err := counter1.Increment(ctx, counterName, 1, nil); err != nil {
			t.Fatalf("Increment counter1 failed: %v", err)
		}
	}

	// Increment counter2 differently
	for i := 0; i < 50; i++ {
		if _, _, err := counter2.Increment(ctx, counterName, 1, nil); err != nil {
			t.Fatalf("Increment counter2 failed: %v", err)
		}
	}

	// Verify they're independent
	total1, err := counter1.GetExact(ctx, counterName)
	if err != nil {
		t.Fatalf("GetExact counter1 failed: %v", err)
	}
	total2, err := counter2.GetExact(ctx, counterName)
	if err != nil {
		t.Fatalf("GetExact counter2 failed: %v", err)
	}

	if total1 != 100 {
		t.Errorf("counter1: expected 100, got %d", total1)
	}
	if total2 != 50 {
		t.Errorf("counter2: expected 50, got %d", total2)
	}
}

// BenchmarkIntegrationIncrement benchmarks increment performance
func BenchmarkIntegrationIncrement(b *testing.B) {
	client := newLocalStackS3Client(&testing.T{})
	ctx := context.Background()

	// Setup
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(testBucket),
	})
	if err != nil {
		b.Fatalf("failed to create bucket: %v", err)
	}

	counter := shardedcounter.New(client, testBucket,
		shardedcounter.WithPrefix("bench"),
		shardedcounter.WithDefaultShards(32),
	)

	counterName := fmt.Sprintf("bench-%d", time.Now().Unix())
	if err := counter.Ensure(ctx, counterName, 32); err != nil {
		b.Fatalf("Ensure failed: %v", err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, _, err := counter.Increment(ctx, counterName, 1, nil); err != nil {
				b.Logf("increment error: %v", err)
			}
		}
	})
}
