package shardedcounter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestCounterLifecycle(t *testing.T) {
	ctx := context.Background()
	s3stub := newStubS3()
	counter := New(s3stub, "test-bucket")

	if err := counter.Ensure(ctx, "widgets", 8); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	for i := 0; i < 63; i++ {
		if _, _, err := counter.Increment(ctx, "widgets", 1, nil); err != nil {
			t.Fatalf("Increment(%d): %v", i, err)
		}
	}

	total, err := counter.GetExact(ctx, "widgets")
	if err != nil {
		t.Fatalf("GetExact: %v", err)
	}
	if total != 63 {
		t.Fatalf("GetExact= %d, want 63", total)
	}

	folded, newEpoch, err := counter.Compact(ctx, "widgets")
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if folded != 63 {
		t.Fatalf("folded=%d want 63", folded)
	}
	if newEpoch != 1 {
		t.Fatalf("newEpoch=%d want 1", newEpoch)
	}

	totalAfter, err := counter.GetExact(ctx, "widgets")
	if err != nil {
		t.Fatalf("GetExact post-compact: %v", err)
	}
	if totalAfter != 63 {
		t.Fatalf("GetExact post-compact=%d want 63", totalAfter)
	}

	shards := s3stub.listKeysWithPrefix("counters/widgets/epochs/0/shards/")
	if len(shards) != 0 {
		t.Fatalf("expected epoch 0 shards deleted, found: %v", shards)
	}
}

func TestEnsureIdempotentAndDefaults(t *testing.T) {
	ctx := context.Background()
	s3stub := newStubS3()
	counter := New(s3stub, "test-bucket")

	if err := counter.Ensure(ctx, "likes", 4); err != nil {
		t.Fatalf("Ensure first: %v", err)
	}
	if err := counter.Ensure(ctx, "likes", 0); err != nil {
		t.Fatalf("Ensure second: %v", err)
	}

	meta, _, err := counter.getEpoch(ctx, "likes")
	if err != nil {
		t.Fatalf("getEpoch: %v", err)
	}
	if meta.ShardCount != 4 {
		t.Fatalf("ShardCount=%d want 4", meta.ShardCount)
	}

	base := s3stub.mustRead("counters/likes/base_total.txt")
	if base != "0" {
		t.Fatalf("base_total=%q want 0", base)
	}
}

func TestGetApproxUsesSamples(t *testing.T) {
	ctx := context.Background()
	s3stub := newStubS3()
	counter := New(s3stub, "test-bucket", WithDefaultShards(8))

	if err := counter.Ensure(ctx, "traffic", 0); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	// Write to all 8 shards so sampling will always find data
	for i := 0; i < 80; i++ {
		override := i % 8
		if _, _, err := counter.Increment(ctx, "traffic", 5, &override); err != nil {
			t.Fatalf("Increment override=%d: %v", override, err)
		}
	}

	// Sample 4 shards - should get roughly 50% of total
	total, err := counter.GetApprox(ctx, "traffic", 4)
	if err != nil {
		t.Fatalf("GetApprox: %v", err)
	}
	// Exact total is 400, sampling half the shards should give us ~400
	// Allow wide margin since sampling is probabilistic
	if total < 200 || total > 600 {
		t.Fatalf("GetApprox=%d, expected roughly 400", total)
	}
}

func TestBufferedCounter(t *testing.T) {
	ctx := context.Background()
	s3stub := newStubS3()
	counter := New(s3stub, "test-bucket")

	if err := counter.Ensure(ctx, "events", 8); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	buffered := NewBuffered(counter, 100*time.Millisecond, 50, nil)
	buffered.Start()
	defer func() {
		if err := buffered.Stop(ctx); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	}()

	// Buffer many increments
	for i := 0; i < 200; i++ {
		if err := buffered.IncrementBuffered(ctx, "events", 1); err != nil {
			t.Fatalf("IncrementBuffered: %v", err)
		}
	}

	// Flush and verify
	if err := buffered.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	total, err := counter.GetExact(ctx, "events")
	if err != nil {
		t.Fatalf("GetExact: %v", err)
	}
	if total != 200 {
		t.Fatalf("GetExact=%d want 200", total)
	}
}

func TestEpochCaching(t *testing.T) {
	ctx := context.Background()
	s3stub := newStubS3()
	counter := New(s3stub, "test-bucket", WithEpochCacheTTL(1*time.Second))

	if err := counter.Ensure(ctx, "cached", 4); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	// First increment - should fetch epoch
	initialReads := s3stub.readCount("counters/cached/epoch.json")
	_, _, err := counter.Increment(ctx, "cached", 1, nil)
	if err != nil {
		t.Fatalf("Increment: %v", err)
	}
	afterFirstRead := s3stub.readCount("counters/cached/epoch.json")
	if afterFirstRead <= initialReads {
		t.Fatalf("Expected epoch read on first increment")
	}

	// Second increment - should use cache
	_, _, err = counter.Increment(ctx, "cached", 1, nil)
	if err != nil {
		t.Fatalf("Increment: %v", err)
	}
	afterSecondRead := s3stub.readCount("counters/cached/epoch.json")
	if afterSecondRead != afterFirstRead {
		t.Fatalf("Expected cached epoch, but got another read")
	}
}

// --- stub S3 client -------------------------------------------------------

type stubS3 struct {
	mu        sync.Mutex
	objects   map[string]stubObject
	revision  int64
	readStats map[string]int
}

type stubObject struct {
	body []byte
	etag string
}

func newStubS3() *stubS3 {
	return &stubS3{
		objects:   make(map[string]stubObject),
		readStats: make(map[string]int),
	}
}

func (s *stubS3) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if input == nil || input.Key == nil {
		return nil, fmt.Errorf("missing key")
	}

	key := aws.ToString(input.Key)
	body, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	obj, ok := s.objects[key]

	if v := aws.ToString(input.IfMatch); v != "" {
		if !ok || obj.etag != v {
			return nil, stubError{code: http.StatusPreconditionFailed}
		}
	}
	if v := aws.ToString(input.IfNoneMatch); v != "" {
		if v == "*" && ok {
			return nil, stubError{code: http.StatusPreconditionFailed}
		}
	}

	s.revision++
	newObj := stubObject{
		body: body,
		etag: fmt.Sprintf(`"rev-%d"`, s.revision),
	}
	s.objects[key] = newObj

	return &s3.PutObjectOutput{
		ETag: aws.String(newObj.etag),
	}, nil
}

func (s *stubS3) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if input == nil || input.Key == nil {
		return nil, fmt.Errorf("missing key")
	}

	key := aws.ToString(input.Key)

	s.mu.Lock()
	obj, ok := s.objects[key]
	s.readStats[key]++
	s.mu.Unlock()

	if !ok {
		return nil, stubError{code: http.StatusNotFound}
	}

	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(obj.body)),
		ETag: aws.String(obj.etag),
	}, nil
}

func (s *stubS3) DeleteObject(_ context.Context, input *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if input == nil || input.Key == nil {
		return nil, fmt.Errorf("missing key")
	}
	key := aws.ToString(input.Key)

	s.mu.Lock()
	delete(s.objects, key)
	s.mu.Unlock()

	return &s3.DeleteObjectOutput{}, nil
}

func (s *stubS3) DeleteObjects(_ context.Context, input *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	if input == nil || input.Delete == nil {
		return nil, fmt.Errorf("missing delete payload")
	}

	s.mu.Lock()
	for _, obj := range input.Delete.Objects {
		if obj.Key != nil {
			delete(s.objects, aws.ToString(obj.Key))
		}
	}
	s.mu.Unlock()

	return &s3.DeleteObjectsOutput{}, nil
}

func (s *stubS3) ListObjectsV2(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	prefix := ""
	if input != nil && input.Prefix != nil {
		prefix = aws.ToString(input.Prefix)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	contents := make([]types.Object, 0, len(keys))
	for _, key := range keys {
		obj := s.objects[key]
		contents = append(contents, types.Object{
			Key:          aws.String(key),
			LastModified: aws.Time(time.Now()),
			ETag:         aws.String(obj.etag),
			Size:         aws.Int64(int64(len(obj.body))),
		})
	}

	return &s3.ListObjectsV2Output{
		Contents:              contents,
		NextContinuationToken: nil,
		IsTruncated:           aws.Bool(false),
		KeyCount:              aws.Int32(int32(len(contents))),
	}, nil
}

func (s *stubS3) listKeysWithPrefix(prefix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

func (s *stubS3) mustRead(key string) string {
	s.mu.Lock()
	obj, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return ""
	}
	return string(obj.body)
}

func (s *stubS3) readCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readStats[key]
}

type stubError struct {
	code int
}

func (e stubError) Error() string {
	return fmt.Sprintf("stub http error %d", e.code)
}

func (e stubError) StatusCode() int {
	return e.code
}
