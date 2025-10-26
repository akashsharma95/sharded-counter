package s3counter

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
	counter := New(s3stub, "test-bucket", WithDefaultShards(16))

	if err := counter.Ensure(ctx, "traffic", 0); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	for i := 0; i < 100; i++ {
		override := i % 3
		if _, _, err := counter.Increment(ctx, "traffic", 5, &override); err != nil {
			t.Fatalf("Increment override=%d: %v", override, err)
		}
	}

	total, err := counter.GetApprox(ctx, "traffic", 4)
	if err != nil {
		t.Fatalf("GetApprox: %v", err)
	}
	if total == 0 {
		t.Fatalf("GetApprox returned zero unexpectedly")
	}
}

// --- stub S3 client -------------------------------------------------------

type stubS3 struct {
	mu       sync.Mutex
	objects  map[string]stubObject
	revision int64
}

type stubObject struct {
	body []byte
	etag string
}

func newStubS3() *stubS3 {
	return &stubS3{
		objects: make(map[string]stubObject),
	}
}

func (s *stubS3) PutObject(ctx context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
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

func (s *stubS3) GetObject(ctx context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if input == nil || input.Key == nil {
		return nil, fmt.Errorf("missing key")
	}

	key := aws.ToString(input.Key)

	s.mu.Lock()
	obj, ok := s.objects[key]
	s.mu.Unlock()

	if !ok {
		return nil, stubError{code: http.StatusNotFound}
	}

	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(obj.body)),
		ETag: aws.String(obj.etag),
	}, nil
}

func (s *stubS3) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if input == nil || input.Key == nil {
		return nil, fmt.Errorf("missing key")
	}
	key := aws.ToString(input.Key)

	s.mu.Lock()
	delete(s.objects, key)
	s.mu.Unlock()

	return &s3.DeleteObjectOutput{}, nil
}

func (s *stubS3) DeleteObjects(ctx context.Context, input *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
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

func (s *stubS3) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
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

type stubError struct {
	code int
}

func (e stubError) Error() string {
	return fmt.Sprintf("stub http error %d", e.code)
}

func (e stubError) StatusCode() int {
	return e.code
}
