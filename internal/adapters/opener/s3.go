package opener

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/minio/minio-go/v7"
	"debtster_import/internal/ports"
)

type S3Client interface {
	StatObject(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
	GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error)
}

type S3Opener struct{ Client S3Client }

func NewS3Opener(cli S3Client) *S3Opener { return &S3Opener{Client: cli} }

func (s *S3Opener) Open(ctx context.Context, bucket, key string) (io.ReadCloser, ports.Meta, error) {
	log.Printf("[OPENER][S3][START] bucket=%q key=%q", bucket, key)
	st, err := s.Client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		log.Printf("[OPENER][S3][ERR] stat: %v", err)
		return nil, ports.Meta{}, fmt.Errorf("s3 stat: %w", err)
	}
	obj, err := s.Client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		log.Printf("[OPENER][S3][ERR] get: %v", err)
		return nil, ports.Meta{}, fmt.Errorf("s3 get: %w", err)
	}
	log.Printf("[OPENER][S3][OK] content_type=%q size=%d etag=%q", st.ContentType, st.Size, st.ETag)
	return obj, ports.Meta{
		Source:      "s3",
		ContentType: st.ContentType,
		Size:        st.Size,
		Bucket:      bucket,
		Key:         key,
	}, nil
}
