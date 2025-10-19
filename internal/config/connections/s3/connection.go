package s3

import (
	"context"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type ConnectionInfo struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Region    string
	Bucket    string
	UseSSL    bool
}

type S3 struct {
	Client *minio.Client
	Bucket string
}

func NewConnection(info ConnectionInfo) (*S3, error) {
	client, err := minio.New(info.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(info.AccessKey, info.SecretKey, ""),
		Secure: info.UseSSL,
		Region: info.Region,
	})
	if err != nil {
		return nil, err
	}

	return &S3{Client: client, Bucket: info.Bucket}, nil
}

func (s *S3) EnsureBucket(ctx context.Context) error {
	exists, err := s.Client.BucketExists(ctx, s.Bucket)
	if err != nil {
		return err
	}
	if !exists {
		return s.Client.MakeBucket(ctx, s.Bucket, minio.MakeBucketOptions{})
	}
	return nil
}
