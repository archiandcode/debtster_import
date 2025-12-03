package database

import (
	"context"
	"debtster_import/internal/config/connections/postgres"
	"debtster_import/internal/models"
)

type AgreementRepo struct {
	pg    *postgres.Postgres
	table string
}

func NewAgreementRepo(pg *postgres.Postgres) *AgreementRepo {
	return &AgreementRepo{
		pg:    pg,
		table: "agreements",
	}
}

func (r *AgreementRepo) UpdateOrCreate(ctx context.Context, a models.Agreement) (*models.Agreement, error) {
	query := `
		INSERT INTO ` + r.table + ` (
			agreement_type_id, debt_id, user_id, amount_debt,
			monthly_payment_amount, scheduled_payment_day,
			start_date, end_date, created_at, updated_at
		) VALUES (
			$1, $2::uuid, $3, $4::numeric, $5::numeric,    
			$6, $7::date, $8::date, COALESCE($9, NOW()),
			NOW()
		)
		ON CONFLICT (debt_id) DO UPDATE SET
			agreement_type_id = EXCLUDED.agreement_type_id,
			user_id = EXCLUDED.user_id,
			amount_debt = EXCLUDED.amount_debt,
			monthly_payment_amount = EXCLUDED.monthly_payment_amount,
			scheduled_payment_day = EXCLUDED.scheduled_payment_day,
			start_date = EXCLUDED.start_date,
			end_date = EXCLUDED.end_date,
			updated_at = NOW()
		RETURNING
			id, agreement_type_id, debt_id, user_id,
			amount_debt, monthly_payment_amount,
			scheduled_payment_day, start_date,
			end_date, created_at, updated_at
	`

	var out models.Agreement
	err := r.pg.Pool.QueryRow(ctx, query,
		a.AgreementTypeID, a.DebtID, a.UserID, a.AmountDebt,
		a.MonthlyPaymentAmount, a.ScheduledPaymentDay,
		a.StartDate, a.EndDate, a.CreatedAt,
	).Scan(
		&out.ID, &out.AgreementTypeID, &out.DebtID,
		&out.UserID, &out.AmountDebt, &out.MonthlyPaymentAmount,
		&out.ScheduledPaymentDay, &out.StartDate, &out.EndDate,
		&out.CreatedAt, &out.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (r *AgreementRepo) GetTableName() string {
	return r.table
}
