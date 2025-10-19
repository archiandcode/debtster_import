package opener

import (
	"context"
	"errors"
	"io"
	"net/url"
	"path"
	"strings"

	"debtster_import/internal/ports"
)

type CompoundOpener struct {
	HTTP *HTTPOpener
	S3   *S3Opener

	DefaultBucket string
}

func NewCompoundOpener(httpOp *HTTPOpener, s3Op *S3Opener, defaultBucket string) *CompoundOpener {
	return &CompoundOpener{
		HTTP:          httpOp,
		S3:            s3Op,
		DefaultBucket: defaultBucket,
	}
}

func (c *CompoundOpener) Open(ctx context.Context, filePath string) (io.ReadCloser, ports.Meta, error) {
	fp := strings.TrimSpace(filePath)

	switch {
	case strings.HasPrefix(fp, "http://") || strings.HasPrefix(fp, "https://"):
		if c.HTTP == nil {
			return nil, ports.Meta{}, errors.New("http opener not configured")
		}
		return c.HTTP.Open(ctx, fp)

	case strings.HasPrefix(fp, "s3://"):
		if c.S3 == nil {
			return nil, ports.Meta{}, errors.New("s3 opener not configured")
		}
		bkt, key, err := parseS3URL(fp)
		if err != nil {
			return nil, ports.Meta{}, err
		}
		return c.S3.Open(ctx, bkt, key)

	default:
		if c.S3 == nil || c.DefaultBucket == "" {
			return nil, ports.Meta{}, errors.New("missing bucket: pass s3://bucket/key or https url")
		}
		return c.S3.Open(ctx, c.DefaultBucket, fp)
	}
}

func parseS3URL(raw string) (bucket, key string, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", err
	}
	if u.Scheme != "s3" {
		return "", "", errors.New("scheme must be s3")
	}
	bucket = u.Host
	key = strings.TrimPrefix(u.Path, "/")
	key = path.Clean(key)
	if bucket == "" || key == "" || key == "." || key == "/" {
		return "", "", errors.New("empty bucket or key")
	}
	return bucket, key, nil
}
