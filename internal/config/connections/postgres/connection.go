package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ConnectionInfo struct {
	Host     string
	Port     string
	User     string
	Password string
	DB       string
	SSLMode  string
}

type Postgres struct {
	Pool *pgxpool.Pool
}

func NewConnection(ctx context.Context, info ConnectionInfo) (*Postgres, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		info.Host, info.Port, info.User, info.Password, info.DB, info.SSLMode,
	)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, err
	}

	return &Postgres{Pool: pool}, nil
}

func (p *Postgres) Close() {
	if p.Pool != nil {
		p.Pool.Close()
	}
}
