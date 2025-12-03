package database

import (
	"context"
	"debtster_import/internal/config/connections/postgres"
)

type DebtStatusesRepo struct {
	pg    *postgres.Postgres
	table string
	cache map[string]*int64
}

func NewDebtStatusesRepo(pg *postgres.Postgres) *DebtStatusesRepo {
	return &DebtStatusesRepo{
		pg:    pg,
		table: "debt_statuses",
	}
}

func (r *DebtStatusesRepo) GetStatusBigint(ctx context.Context, shortname string) (*int64, error) {
	if v, ok := r.cache[shortname]; ok {
		return v, nil
	}

	var id int64
	err := r.pg.Pool.QueryRow(
		ctx,
		`SELECT id FROM `+r.table+` WHERE shortname  	= $1 LIMIT 1`,
		shortname,
	).Scan(&id)
	if err != nil {
		r.cache[shortname] = nil
		return nil, err
	}

	r.cache[shortname] = &id
	return &id, nil
}
