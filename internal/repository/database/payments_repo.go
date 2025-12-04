package database

import (
	"context"

	"debtster_import/internal/config/connections/postgres"
	"debtster_import/internal/models"

	"github.com/jackc/pgx/v5"
)

type PaymentRepo struct {
	pg *postgres.Postgres
}

func NewPaymentRepo(pg *postgres.Postgres) *PaymentRepo {
	return &PaymentRepo{pg: pg}
}

const insertPaymentQuery = `
	INSERT INTO payments (
		id, debt_id, user_id,
		amount, amount_after_subtraction, amount_government_duty,
		amount_representation_expenses, amount_notary_fees, amount_postage,
		confirmed, payment_date, created_at,
		amount_accounts_receivable, amount_main_debt, amount_accrual, amount_fine
	)
	VALUES (
		$1::uuid,  $2::uuid,  $3::bigint,
		$4::numeric,  $5::numeric,  $6::numeric,
		$7::numeric,  $8::numeric,  $9::numeric,
		$10::bool,  $11::date,  NOW(),
		$12::numeric,  $13::numeric,  $14::numeric,  $15::numeric
	)
	ON CONFLICT (
		debt_id,
		user_id,
		amount,
		amount_after_subtraction,
		payment_date
	) DO NOTHING;
`

func (r *PaymentRepo) CreateBatch(ctx context.Context, rows []models.Payment) []error {
	errs := make([]error, len(rows))
	if len(rows) == 0 {
		return errs
	}

	batch := &pgx.Batch{}

	for _, row := range rows {
		batch.Queue(
			insertPaymentQuery,
			row.ID, row.DebtID, row.UserID,
			row.Amount, row.AmountAfterSubtraction, row.AmountGovernmentDuty,
			row.AmountRepresentationExpenses, row.AmountNotaryFees, row.AmountPostage,
			row.Confirmed, row.PaymentDate,
			row.AmountAccountsReceivable, row.AmountMainDebt,
			row.AmountAccrual, row.AmountFine,
		)
	}

	br := r.pg.Pool.SendBatch(ctx, batch)
	defer br.Close()

	for i := range rows {
		_, err := br.Exec()
		if err != nil {
			errs[i] = err
		}
	}

	return errs
}
