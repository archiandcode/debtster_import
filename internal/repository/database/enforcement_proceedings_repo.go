package database

import (
	"context"

	"debtster_import/internal/config/connections/postgres"
	"debtster_import/internal/models"
)

type EnforcementProceedingsRepo struct {
	PG *postgres.Postgres
}

func NewEnforcementProceedingsRepo(pg *postgres.Postgres) *EnforcementProceedingsRepo {
	return &EnforcementProceedingsRepo{PG: pg}
}

func (r *EnforcementProceedingsRepo) Create(ctx context.Context, row models.EnforcementProceeding) error {
	const q = `
		INSERT INTO enforcement_proceedings (
		  serial_number, debt_id, amount, private_bailiff_name, private_bailiff_region,
		  start_date, status_ais_oip, created_at
		) VALUES (
		  $1, $2::uuid, $3::numeric, $4, $5,
		  $6::date, $7, NOW()
		)`

	_, err := r.PG.Pool.Exec(ctx, q,
		row.SerialNumber, row.DebtID, row.Amount, row.PrivateBailiffName, row.PrivateBailiffRegion,
		row.StartDate, row.StatusAISOIP,
	)
	return err
}
