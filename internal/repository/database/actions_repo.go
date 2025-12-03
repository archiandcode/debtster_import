package database

import (
	"context"
	"debtster_import/internal/config/connections/postgres"
	"debtster_import/internal/models"

	"github.com/jackc/pgx/v5"
)

type ActionRepo struct {
	pg    *postgres.Postgres
	table string
}

func NewActionRepo(pg *postgres.Postgres) *ActionRepo {
	return &ActionRepo{
		pg:    pg,
		table: "actions",
	}
}

func (r *ActionRepo) InsertActions(ctx context.Context, rows []models.Action) error {
	if len(rows) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	queued := 0

	for _, a := range rows {
		if a.DebtID == nil {
			continue
		}

		batch.Queue(`
			INSERT INTO `+r.table+` (
				id, debt_id, user_id, debt_status_id, type, comment, created_at
			) VALUES (
				$1::uuid, $2::uuid, $3::bigint, $4::bigint, $5, $6, $7
			)
		`,
			a.ID, a.DebtID, a.UserID, a.DebtStatusID, a.Type, a.Comment, a.CreatedAt,
		)
		queued++
	}

	if queued == 0 {
		return nil
	}

	br := r.pg.Pool.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < queued; i++ {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}

	return nil
}

func (r *ActionRepo) GetTableName() string {
	return r.table
}
