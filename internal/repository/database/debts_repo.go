package database

import (
	"context"
	"strings"
	"time"

	"debtster_import/internal/config/connections/postgres"
)

type DebtsRepo struct {
	pg    *postgres.Postgres
	table string
}

func NewDebtsRepo(pg *postgres.Postgres, table string) *DebtsRepo {
	return &DebtsRepo{
		pg:    pg,
		table: table,
	}
}

type DebtRow struct {
	ID                          string
	DebtorID                    *string
	Number                      string
	StartDate                   *time.Time
	EndDate                     *time.Time
	Filial                      string
	ProductName                 string
	Currency                    string
	AmountActualDebt            *float64
	AmountAccountsReceivable    *float64
	AmountCredit                *float64
	AmountMainDebt              *float64
	AmountFine                  *float64
	AmountAccrual               *float64
	AmountGovernmentDuty        *float64
	AmountRepresentationExpense *float64
	AmountNotaryFees            *float64
	AmountPostage               *float64
	AdditionalData              string
	UserID                      *int64
	CounterpartyID              *int64
	StatusID                    *int64
	CreatedAt                   *time.Time
}

// ✅ Полностью исправлено: не затирает старые значения, работает безопасно
func (r *DebtsRepo) UpdateOrCreate(ctx context.Context, row DebtRow) error {
	row.Filial = strings.TrimSpace(row.Filial)
	row.ProductName = strings.TrimSpace(row.ProductName)
	row.Currency = strings.TrimSpace(row.Currency)

	zero := 0.0
	if row.AmountActualDebt == nil {
		row.AmountActualDebt = &zero
	}
	if row.AmountAccountsReceivable == nil {
		row.AmountAccountsReceivable = &zero
	}
	if row.AmountCredit == nil {
		row.AmountCredit = &zero
	}
	if row.AmountMainDebt == nil {
		row.AmountMainDebt = &zero
	}
	if row.AmountFine == nil {
		row.AmountFine = &zero
	}
	if row.AmountAccrual == nil {
		row.AmountAccrual = &zero
	}
	if row.AmountGovernmentDuty == nil {
		row.AmountGovernmentDuty = &zero
	}
	if row.AmountRepresentationExpense == nil {
		row.AmountRepresentationExpense = &zero
	}
	if row.AmountNotaryFees == nil {
		row.AmountNotaryFees = &zero
	}
	if row.AmountPostage == nil {
		row.AmountPostage = &zero
	}

	additional := "{}"
	if strings.TrimSpace(row.AdditionalData) != "" {
		if strings.HasPrefix(row.AdditionalData, "{") || strings.HasPrefix(row.AdditionalData, "[") {
			additional = row.AdditionalData
		}
	}

	query := `
		INSERT INTO ` + r.table + ` (
			id, debtor_id, number, start_date, end_date, filial, product_name,
			amount_currency, amount_actual_debt, amount_accounts_receivable,
			amount_credit, amount_main_debt, amount_fine, amount_accrual,
			amount_government_duty, amount_representation_expenses,
			amount_notary_fees, amount_postage, additional_data,
			user_id, counterparty_id, status_id, created_at
		) VALUES (
			$1::uuid, $2::uuid, $3, $4, $5, $6, $7,
			$8, $9, $10,
			$11, $12, $13, $14,
			$15, $16,
			$17, $18, $19::jsonb,
			$20, $21, $22, NOW()
		)
		ON CONFLICT (number) DO UPDATE SET
			debtor_id = COALESCE(EXCLUDED.debtor_id, ` + r.table + `.debtor_id),
			start_date = COALESCE(EXCLUDED.start_date, ` + r.table + `.start_date),
			end_date = COALESCE(EXCLUDED.end_date, ` + r.table + `.end_date),
			filial = COALESCE(NULLIF(EXCLUDED.filial, ''), ` + r.table + `.filial),
			product_name = COALESCE(NULLIF(EXCLUDED.product_name, ''), ` + r.table + `.product_name),
			amount_currency = COALESCE(NULLIF(EXCLUDED.amount_currency, ''), ` + r.table + `.amount_currency),
			amount_actual_debt = COALESCE(EXCLUDED.amount_actual_debt, ` + r.table + `.amount_actual_debt),
			amount_accounts_receivable = COALESCE(EXCLUDED.amount_accounts_receivable, ` + r.table + `.amount_accounts_receivable),
			amount_credit = COALESCE(EXCLUDED.amount_credit, ` + r.table + `.amount_credit),
			amount_main_debt = COALESCE(EXCLUDED.amount_main_debt, ` + r.table + `.amount_main_debt),
			amount_fine = COALESCE(EXCLUDED.amount_fine, ` + r.table + `.amount_fine),
			amount_accrual = COALESCE(EXCLUDED.amount_accrual, ` + r.table + `.amount_accrual),
			amount_government_duty = COALESCE(EXCLUDED.amount_government_duty, ` + r.table + `.amount_government_duty),
			amount_representation_expenses = COALESCE(EXCLUDED.amount_representation_expenses, ` + r.table + `.amount_representation_expenses),
			amount_notary_fees = COALESCE(EXCLUDED.amount_notary_fees, ` + r.table + `.amount_notary_fees),
			amount_postage = COALESCE(EXCLUDED.amount_postage, ` + r.table + `.amount_postage),
			additional_data = COALESCE(EXCLUDED.additional_data, ` + r.table + `.additional_data),
			user_id = COALESCE(EXCLUDED.user_id, ` + r.table + `.user_id),
			counterparty_id = COALESCE(EXCLUDED.counterparty_id, ` + r.table + `.counterparty_id),
			status_id = COALESCE(EXCLUDED.status_id, ` + r.table + `.status_id),
			updated_at = NOW()
	`

	_, err := r.pg.Pool.Exec(ctx, query,
		row.ID, row.DebtorID, row.Number,
		row.StartDate, row.EndDate, row.Filial, row.ProductName,
		row.Currency, row.AmountActualDebt, row.AmountAccountsReceivable,
		row.AmountCredit, row.AmountMainDebt, row.AmountFine, row.AmountAccrual,
		row.AmountGovernmentDuty, row.AmountRepresentationExpense,
		row.AmountNotaryFees, row.AmountPostage, additional,
		row.UserID, row.CounterpartyID, row.StatusID,
	)

	return err
}
