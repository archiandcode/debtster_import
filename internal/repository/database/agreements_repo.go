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

	// ---------- 1) Пытаемся обновить ----------
	updateQuery := `
		UPDATE ` + r.table + `
		SET 
			agreement_type_id      = $2,
			user_id                = $3,
			amount_debt            = $4,
			monthly_payment_amount = $5,
			scheduled_payment_day  = $6,
			start_date             = $7,
			end_date               = $8,
			updated_at             = NOW()
		WHERE debt_id = $1::uuid
		RETURNING 
			id, agreement_type_id, debt_id, user_id, amount_debt,
			monthly_payment_amount, scheduled_payment_day,
			start_date, end_date, created_at, updated_at
	`

	var out models.Agreement
	err := r.pg.Pool.QueryRow(ctx, updateQuery,
		a.DebtID, a.AgreementTypeID, a.UserID, a.AmountDebt,
		a.MonthlyPaymentAmount, a.ScheduledPaymentDay,
		a.StartDate, a.EndDate,
	).Scan(
		&out.ID, &out.AgreementTypeID, &out.DebtID, &out.UserID,
		&out.AmountDebt, &out.MonthlyPaymentAmount,
		&out.ScheduledPaymentDay, &out.StartDate,
		&out.EndDate, &out.CreatedAt, &out.UpdatedAt,
	)

	if err == nil {
		// Успешно обновили
		return &out, nil
	}

	// ---------- 2) Если не обновилось — делаем INSERT ----------
	insertQuery := `
		INSERT INTO ` + r.table + ` (
			agreement_type_id, debt_id, user_id, amount_debt,
			monthly_payment_amount, scheduled_payment_day,
			start_date, end_date, created_at, updated_at
		) VALUES (
			$1, $2::uuid, $3, $4::numeric, $5::numeric,
			$6, $7::date, $8::date, 
			COALESCE($9, NOW()), NOW()
		)
		RETURNING
			id, agreement_type_id, debt_id, user_id,
			amount_debt, monthly_payment_amount,
			scheduled_payment_day, start_date,
			end_date, created_at, updated_at
	`

	err = r.pg.Pool.QueryRow(ctx, insertQuery,
		a.AgreementTypeID, a.DebtID, a.UserID, a.AmountDebt,
		a.MonthlyPaymentAmount, a.ScheduledPaymentDay,
		a.StartDate, a.EndDate, a.CreatedAt,
	).Scan(
		&out.ID, &out.AgreementTypeID, &out.DebtID, &out.UserID,
		&out.AmountDebt, &out.MonthlyPaymentAmount,
		&out.ScheduledPaymentDay, &out.StartDate,
		&out.EndDate, &out.CreatedAt, &out.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	return &out, nil
}

func (r *AgreementRepo) GetTableName() string {
	return r.table
}
