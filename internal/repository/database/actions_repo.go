package database

import (
	"context"
	"debtster_import/internal/config/connections/postgres"
	"debtster_import/internal/models"

	"github.com/jackc/pgx/v5"
)

type ActionRepo struct {
	pg *postgres.Postgres
}

func NewActionRepo(pg *postgres.Postgres) *ActionRepo {
	return &ActionRepo{
		pg: pg,
	}
}

func (r *ActionRepo) SaveActions(ctx context.Context, rows []models.Action) error {
	if len(rows) == 0 {
		return nil
	}

	batch := &pgx.Batch{}

	for _, a := range rows {
		if a.DebtID == nil {
			continue
		}

		table := "actions"
		batch.Queue(`
			INSERT INTO `+table+` (
				id, debt_id, user_id, debt_status_id, type, comment, created_at
			) VALUES (
				$1::uuid, $2::uuid, $3::bigint, $4::bigint, $5, $6, $7
			)
		`,
			a.ID, a.DebtID, a.UserID, a.DebtStatusID, a.Type, a.Comment, a.CreatedAt,
		)
	}

	br := r.pg.Pool.SendBatch(ctx, batch)
	defer br.Close()

	for range rows {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}

	return nil
}
