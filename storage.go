package shardedcounter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Client is the minimal S3 interface required by Counter. Helps in tests.
type Client interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

type epochMeta struct {
	Epoch      int `json:"epoch"`
	ShardCount int `json:"shardCount"`
}

func (c *Counter) base(name string) string {
	if c.cfg.Prefix == "" {
		return name
	}
	return c.cfg.Prefix + "/" + name
}
func (c *Counter) keyEpoch(name string) string     { return c.base(name) + "/epoch.json" }
func (c *Counter) keyBaseTotal(name string) string { return c.base(name) + "/base_total.txt" }
func (c *Counter) dirEpoch(name string, e int) string {
	return fmt.Sprintf("%s/epochs/%d", c.base(name), e)
}
func (c *Counter) keyShard(name string, e, shard int) string {
	return fmt.Sprintf("%s/shards/%d.txt", c.dirEpoch(name, e), shard)
}
func (c *Counter) keyLock(name string) string { return c.base(name) + "/locks/compaction.lock" }

func (c *Counter) acquireLock(ctx context.Context, name string) error {
	body := fmt.Appendf(nil, `{"ts":%d}`, time.Now().Unix())
	return c.putTextConditional(ctx, c.keyLock(name), body, "If-None-Match", "*")
}

func (c *Counter) releaseLock(ctx context.Context, name string) {
	_ = c.deleteObject(ctx, c.keyLock(name))
}

func (c *Counter) putText(ctx context.Context, key string, body []byte) error {
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.cfg.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("text/plain"),
	})
	return err
}

func (c *Counter) putTextConditional(ctx context.Context, key string, body []byte, condHeader, condValue string) error {
	input := &s3.PutObjectInput{
		Bucket:      aws.String(c.cfg.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("text/plain"),
	}

	switch strings.ToLower(condHeader) {
	case "if-match":
		input.IfMatch = aws.String(condValue)
	case "if-none-match":
		input.IfNoneMatch = aws.String(condValue)
	default:
		return fmt.Errorf("unsupported conditional header %q", condHeader)
	}

	_, err := c.s3.PutObject(ctx, input)
	return err
}

func (c *Counter) putJSON(ctx context.Context, key string, body []byte, ifMatchETag string) error {
	if ifMatchETag == "" {
		return c.putText(ctx, key, body)
	}
	return c.putTextConditional(ctx, key, body, "If-Match", ifMatchETag)
}

func (c *Counter) getText(ctx context.Context, key string) (string, string, error) {
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", "", err
	}
	defer func() { _ = out.Body.Close() }()

	b, err := io.ReadAll(bufio.NewReader(out.Body))
	if err != nil {
		return "", "", err
	}
	return string(bytes.TrimSpace(b)), aws.ToString(out.ETag), nil
}

func (c *Counter) getJSON(ctx context.Context, key string) (epochMeta, string, error) {
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return epochMeta{}, "", err
	}
	defer func() { _ = out.Body.Close() }()

	var m epochMeta
	if err := json.NewDecoder(out.Body).Decode(&m); err != nil {
		return epochMeta{}, "", err
	}
	return m, aws.ToString(out.ETag), nil
}

func (c *Counter) deleteObject(ctx context.Context, key string) error {
	_, err := c.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(c.cfg.Bucket),
		Key:    aws.String(key),
	})
	return err
}

func (c *Counter) deletePrefix(ctx context.Context, prefix string) error {
	var token *string
	for {
		out, err := c.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(c.cfg.Bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return err
		}
		if len(out.Contents) == 0 {
			return nil
		}

		objs := make([]types.ObjectIdentifier, 0, min(1000, len(out.Contents)))
		for _, o := range out.Contents {
			if o.Key == nil {
				continue
			}
			objs = append(objs, types.ObjectIdentifier{Key: o.Key})
			if len(objs) == 1000 {
				if err := c.flushDeletes(ctx, objs); err != nil {
					return err
				}
				objs = objs[:0]
			}
		}
		if len(objs) > 0 {
			if err := c.flushDeletes(ctx, objs); err != nil {
				return err
			}
		}

		token = out.NextContinuationToken
		if token == nil {
			return nil
		}
	}
}

func (c *Counter) flushDeletes(ctx context.Context, objs []types.ObjectIdentifier) error {
	_, err := c.s3.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(c.cfg.Bucket),
		Delete: &types.Delete{
			Objects: objs,
			Quiet:   aws.Bool(true),
		},
	})
	return err
}

type apiErr interface{ StatusCode() int }

func statusIs(err error, code int) bool {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode() == code
	}
	var ae apiErr
	if errors.As(err, &ae) {
		return ae.StatusCode() == code
	}
	return false
}

func isPreconditionFailed(err error) bool { return statusIs(err, http.StatusPreconditionFailed) }
func isConflict(err error) bool           { return statusIs(err, http.StatusConflict) }

func isNotFound(err error) bool {
	if statusIs(err, http.StatusNotFound) {
		return true
	}
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *types.NotFound
	return errors.As(err, &nf)
}
