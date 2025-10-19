package ports

import (
	"context"
	"io"
)

type Meta struct {
	Source      string
	ContentType string
	Size        int64
	Bucket      string
	Key         string
}

type FileOpener interface {
	Open(ctx context.Context, filePath string) (io.ReadCloser, Meta, error)
}
