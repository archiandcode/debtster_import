package database

import (
	"context"
	"time"

	"debtster_import/internal/config/connections/postgres"

	"github.com/jackc/pgx/v5"
)

type WorkplaceRepo struct {
	db    *postgres.Postgres
	table string
}

func NewWorkplaceRepo(db *postgres.Postgres, table string) *WorkplaceRepo {
	return &WorkplaceRepo{
		db:    db,
		table: table,
	}
}

type WorkplaceRow struct {
	ID       string
	Name     string
	Position string
	UIN      string
	Address  string
	Phone    string
	DebtorID string
}

func (r *WorkplaceRepo) Insert(ctx context.Context, rows []WorkplaceRow) error {
	if len(rows) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, w := range rows {
		batch.Queue(`
			INSERT INTO `+r.table+` (
				id, name, position, uin, address, phone, debtor_id, created_at
			) VALUES (
				$1::uuid, $2, $3, $4, $5, $6, $7::uuid, NOW()
			)
			ON CONFLICT (debtor_id, name) DO NOTHING
		`,
			w.ID, w.Name, w.Position, w.UIN, w.Address, w.Phone, w.DebtorID,
		)
	}

	br := r.db.Pool.SendBatch(ctx, batch)
	defer br.Close()

	for range rows {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}

	return nil
}

func (r *WorkplaceRepo) Exists(ctx context.Context, debtorID, name string) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT 1 FROM ` + r.table + `
			WHERE debtor_id = $1 AND name = $2
		)
	`

	var exists bool
	err := r.db.Pool.QueryRow(ctx, query, debtorID, name).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func TimePtr(t time.Time) *time.Time { return &t }
