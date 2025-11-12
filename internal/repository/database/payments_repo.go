package database

import (
	"context"
	"debtster_import/internal/config/connections/postgres"
	"debtster_import/internal/models"
)

type PaymentRepo struct {
	pg *postgres.Postgres
}

func NewPaymentRepo(pg *postgres.Postgres) *PaymentRepo {
	return &PaymentRepo{pg: pg}
}

func (r *PaymentRepo) Create(ctx context.Context, row models.Payment) error {
	const q = `
		INSERT INTO payments (
		  id, debt_id, user_id, amount, amount_after_subtraction, amount_government_duty,
		  amount_representation_expenses, amount_notary_fees, amount_postage, confirmed,
		  payment_date, created_at, amount_accounts_receivable, amount_main_debt,
		  amount_accrual, amount_fine
		) VALUES (
		  COALESCE(NULLIF($1::text,''), gen_random_uuid())::uuid,
		  $2::uuid, $3::bigint, $4::numeric, $5::numeric, $6::numeric,
		  $7::numeric, $8::numeric, $9::numeric, $10::bool,
		  $11::date, NOW(),
		  $12::numeric, $13::numeric, $14::numeric, $15::numeric
		)
		ON CONFLICT (debt_id, user_id, amount, amount_after_subtraction, payment_date) DO NOTHING
	`
	_, err := r.pg.Pool.Exec(ctx, q,
		row.ID, row.DebtID, row.UserID,
		row.Amount, row.AmountAfterSubtraction, row.AmountGovernmentDuty,
		row.AmountRepresentationExpenses, row.AmountNotaryFees, row.AmountPostage, row.Confirmed,
		row.PaymentDate,
		row.AmountAccountsReceivable, row.AmountMainDebt, row.AmountAccrual, row.AmountFine,
	)
	return err
}
