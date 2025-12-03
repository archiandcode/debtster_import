package database

import (
	"context"
	"debtster_import/internal/config/connections/postgres"
)

type UserRepo struct {
	pg    *postgres.Postgres
	table string
	cache map[string]*int64
}

func NewUserRepo(pg *postgres.Postgres) *UserRepo {
	return &UserRepo{
		pg:    pg,
		table: "users",
		cache: make(map[string]*int64),
	}
}

func (r *UserRepo) GetTableName() string {
	return r.table
}

func (r *UserRepo) GetUserBigint(ctx context.Context, username string) (*int64, error) {
	if v, ok := r.cache[username]; ok {
		return v, nil
	}

	var id int64
	err := r.pg.Pool.QueryRow(
		ctx,
		`SELECT id FROM `+r.table+` WHERE username = $1 LIMIT 1`,
		username,
	).Scan(&id)
	if err != nil {
		r.cache[username] = nil
		return nil, err
	}

	r.cache[username] = &id
	return &id, nil
}
