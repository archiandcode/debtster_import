package database

import (
	"context"
	"debtster_import/internal/config/connections/postgres"
	"debtster_import/internal/models"
)

type EnforcementProceedingsRepo struct {
	pg    *postgres.Postgres
	table string
}

func NewEnforcementProceedingsRepo(pg *postgres.Postgres) *EnforcementProceedingsRepo {
	return &EnforcementProceedingsRepo{
		pg:    pg,
		table: "enforcement_proceedings",
	}
}

func (r *EnforcementProceedingsRepo) Upsert(ctx context.Context, e models.EnforcementProceeding) (*models.EnforcementProceeding, error) {
	q := `
		INSERT INTO ` + r.table + ` (
			serial_number, debt_id, amount, private_bailiff_name,
			private_bailiff_region, start_date, status_ais_oip,
			created_at, updated_at
		) VALUES (
			$1, $2::uuid, $3::numeric, $4, $5, $6::date, $7,
			COALESCE($8, NOW()), NOW()
		)
		ON CONFLICT (debt_id, serial_number) DO UPDATE SET
			amount = EXCLUDED.amount,
			private_bailiff_name = EXCLUDED.private_bailiff_name,
			private_bailiff_region = EXCLUDED.private_bailiff_region,
			start_date = EXCLUDED.start_date,
			status_ais_oip = EXCLUDED.status_ais_oip,
			updated_at = NOW()
		RETURNING
			serial_number, debt_id, amount,
			private_bailiff_name, private_bailiff_region,
			start_date, status_ais_oip,
			created_at
	`

	var out models.EnforcementProceeding
	err := r.pg.Pool.QueryRow(ctx, q,
		e.SerialNumber, e.DebtID, e.Amount, e.PrivateBailiffName,
		e.PrivateBailiffRegion, e.StartDate, e.StatusAISOIP,
		e.CreatedAt,
	).Scan(
		&out.SerialNumber, &out.DebtID, &out.Amount,
		&out.PrivateBailiffName, &out.PrivateBailiffRegion,
		&out.StartDate, &out.StatusAISOIP,
		&out.CreatedAt,
	)

	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (r *EnforcementProceedingsRepo) GetTableName() string {
	return r.table
}
