package database

import (
	"context"
	"debtster_import/internal/config/connections/postgres"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
)

type PaymentRepo struct {
	pg    *postgres.Postgres
	table string
}

func NewPaymentRepo(pg *postgres.Postgres, table string) *PaymentRepo {
	return &PaymentRepo{
		pg:    pg,
		table: table,
	}
}

type PaymentRow struct {
	ID                           string
	DebtID                       string
	UserID                       string
	Amount                       string
	AmountAfterSubtraction       string
	AmountGovernmentDuty         string
	AmountRepresentationExpenses string
	AmountNotaryFees             string
	AmountPostage                string
	AmountAccountsReceivable     string
	AmountMainDebt               string
	AmountAccrual                string
	AmountFine                   string
	PaymentDate                  *time.Time
	CreatedAt                    *time.Time
	Confirmed                    bool
}

func (r *PaymentRepo) Insert(ctx context.Context, rows []PaymentRow) (int, error) {
	if len(rows) == 0 {
		log.Printf("[PROC][payments][DONE] no valid rows")
		return 0, nil
	}

	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(
			`INSERT INTO `+r.table+` (
				id, debt_id, user_id, amount, amount_after_subtraction, amount_government_duty,
				amount_representation_expenses, amount_notary_fees, amount_postage, confirmed,
				payment_date, created_at, amount_accounts_receivable, amount_main_debt,
				amount_accrual, amount_fine
			) VALUES (
				$1::uuid, $2::uuid, $3::bigint, $4::numeric, $5::numeric, $6::numeric,
				$7::numeric, $8::numeric, $9::numeric, $10::bool,
				$11::date, $12::timestamp, $13::numeric, $14::numeric,
				$15::numeric, $16::numeric
			)`,
			row.ID, row.DebtID, row.UserID,
			row.Amount, row.AmountAfterSubtraction, row.AmountGovernmentDuty,
			row.AmountRepresentationExpenses, row.AmountNotaryFees, row.AmountPostage, row.Confirmed,
			row.PaymentDate, row.CreatedAt, row.AmountAccountsReceivable, row.AmountMainDebt,
			row.AmountAccrual, row.AmountFine,
		)
	}

	br := r.pg.Pool.SendBatch(ctx, batch)
	defer br.Close()

	inserted := 0
	for range rows {
		if _, err := br.Exec(); err == nil {
			inserted++
		}
	}
	return inserted, nil
}
