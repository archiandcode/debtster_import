package config

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"debtster_import/internal/config/connections/mongo"
	"debtster_import/internal/config/connections/postgres"
	"debtster_import/internal/config/connections/s3"

	"github.com/joho/godotenv"
)

type Config struct {
	Port     string
	S3       *s3.S3
	Mongo    *mongo.Mongo
	Postgres *postgres.Postgres
}

func Init(ctx context.Context) *Config {
	_ = godotenv.Load()
	port := getenv("SERVER_PORT", "8070")

	s3c, err := s3.NewConnection(s3.ConnectionInfo{
		Endpoint:  getenv("AWS_ENDPOINT", "http://localhost:9000"),
		AccessKey: getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
		SecretKey: getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
		Region:    getenv("AWS_DEFAULT_REGION", "us-east-1"),
		Bucket:    getenv("AWS_BUCKET", "exports"),
		UseSSL:    getenv("AWS_USE_SSL", "false") == "true",
	})
	if err != nil {
		log.Fatal("S3 connect error:", err)
	}

	mg, err := mongo.NewConnection(ctx, mongo.ConnectionInfo{
		Scheme:     getenv("MONGO_SCHEME", "mongodb"),
		User:       getenv("MONGO_USER", "root"),
		Password:   getenv("MONGO_PASSWORD", "secret"),
		Host:       getenv("MONGO_HOST", "127.0.0.1"),
		Port:       getenv("MONGO_PORT", "27017"),
		DB:         getenv("MONGO_DB", "import_db"),
		AuthSource: getenv("MONGO_AUTH_SOURCE", "admin"),
	})
	if err != nil {
		log.Fatal("Mongo connect error:", err)
	}

	pg, err := postgres.NewConnection(ctx, postgres.ConnectionInfo{
		Host:     getenv("PG_HOST", "127.0.0.1"),
		Port:     getenv("PG_PORT", "5432"),
		User:     getenv("PG_USER", "root"),
		Password: getenv("PG_PASSWORD", "hello-world"),
		DB:       getenv("PG_DB", "debtster"),
		SSLMode:  getenv("PG_SSLMODE", "disable"),
	})
	if err != nil {
		log.Fatal("Postgres connect error:", err)
	}

	return &Config{
		S3:       s3c,
		Mongo:    mg,
		Postgres: pg,
		Port:     port,
	}
}

func (c *Config) CheckConnections(ctx context.Context) error {
	var errs []error

	if c.Postgres == nil || c.Postgres.Pool == nil {
		errs = append(errs, errors.New("postgres not initialized"))
	} else if err := c.Postgres.Pool.Ping(ctx); err != nil {
		errs = append(errs, fmt.Errorf("postgres ping failed: %w", err))
	}

	if c.Mongo == nil || c.Mongo.Client == nil {
		errs = append(errs, errors.New("mongo not initialized"))
	} else if err := c.Mongo.Client.Ping(ctx, nil); err != nil {
		errs = append(errs, fmt.Errorf("mongo ping failed: %w", err))
	}

	if c.S3 == nil || c.S3.Client == nil {
		errs = append(errs, errors.New("s3 not initialized"))
	} else if ok, err := c.S3.Client.BucketExists(ctx, c.S3.Bucket); err != nil {
		errs = append(errs, fmt.Errorf("s3 bucket check failed: %w", err))
	} else if !ok {
		errs = append(errs, fmt.Errorf("s3 bucket %q not found", c.S3.Bucket))
	}

	if len(errs) == 0 {
		return nil
	}

	return errors.Join(errs...)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
