package processors

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	mg "debtster_import/internal/config/connections/mongo"
	"debtster_import/internal/config/connections/postgres"
	"debtster_import/internal/ports"
	importitems "debtster_import/internal/repository/imports"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type PaymentsProcessor struct {
	PG *postgres.Postgres
	MG *mg.Mongo

	PaymentsTable string
	DebtsTable    string
	UsersTable    string
}

func (p PaymentsProcessor) Type() string { return "add_payments" }

func (p PaymentsProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
	if p.PG == nil || p.PG.Pool == nil {
		return errors.New("postgres not available")
	}
	if p.MG == nil || p.MG.Database == nil {
		return errors.New("mongo not available")
	}

	var importRecordID string
	if v := ctx.Value(ports.CtxImportRecordID); v != nil {
		if s, ok := v.(string); ok {
			importRecordID = strings.TrimSpace(s)
		}
	}

	paymentsTable := firstNonEmpty(p.PaymentsTable, "payments")
	debtsTable := firstNonEmpty(p.DebtsTable, "debts")
	usersTable := firstNonEmpty(p.UsersTable, "users")

	log.Printf("[PROC][payments][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	debtIDCache := make(map[string]*string)
	userIDCache := make(map[string]*int64)

	type row struct {
		id      string
		debtID  *string
		userID  *int64
		payload map[string]string

		amount                       string
		amountAfterSubtraction       string
		amountGovernmentDuty         string
		amountRepresentationExpenses string
		amountNotaryFees             string
		amountPostage                string
		amountAccountsReceivable     string
		amountMainDebt               string
		amountAccrual                string
		amountFine                   string
		paymentDate                  *time.Time
		createdAt                    *time.Time
		confirmed                    bool
	}

	rows := make([]row, 0, len(batch))

	for _, m := range batch {
		debtNumber := strings.TrimSpace(m["debt_number"])
		if debtNumber == "" {
			logMongoFail(ctx, p.MG, importRecordID, "payments", uuid.NewString(), m, "missing debt_number")
			continue
		}
		debtUUID, err := getDebtUUID(ctx, p.PG, debtsTable, debtNumber, debtIDCache)
		if err != nil || debtUUID == nil {
			msg := "debt not found: " + debtNumber
			if err != nil {
				msg += " (" + err.Error() + ")"
			}
			logMongoFail(ctx, p.MG, importRecordID, "payments", uuid.NewString(), m, msg)
			continue
		}

		username := strings.TrimSpace(m["username"])
		if username == "" {
			logMongoFail(ctx, p.MG, importRecordID, "payments", uuid.NewString(), m, "missing username")
			continue
		}
		userID, err := getUserBigint(ctx, p.PG, usersTable, username, userIDCache)
		if err != nil || userID == nil {
			msg := "username not found: " + username
			if err != nil {
				msg += " (" + err.Error() + ")"
			}
			logMongoFail(ctx, p.MG, importRecordID, "payments", uuid.NewString(), m, msg)
			continue
		}

		paymentDate := parseDateStrict(m["payment_date"])
		if paymentDate == nil {
			logMongoFail(ctx, p.MG, importRecordID, "payments", uuid.NewString(), m, "bad payment_date")
			continue
		}

		amount := normalizeAmount(m["amount"])
		if amount == "" || amount == "0" {
			logMongoFail(ctx, p.MG, importRecordID, "payments", uuid.NewString(), m, "missing/zero amount")
			continue
		}

		rows = append(rows, row{
			id:      uuid.NewString(),
			debtID:  debtUUID,
			userID:  userID,
			payload: m,

			amount:                       amount,
			amountAfterSubtraction:       normalizeAmount(m["amount_after_subtraction"]),
			amountGovernmentDuty:         normalizeAmount(m["amount_government_duty"]),
			amountRepresentationExpenses: normalizeAmount(m["amount_representation_expenses"]),
			amountNotaryFees:             normalizeAmount(m["amount_notary_fees"]),
			amountPostage:                normalizeAmount(m["amount_postage"]),
			amountAccountsReceivable:     normalizeAmount(m["amount_accounts_receivable"]),
			amountMainDebt:               normalizeAmount(m["amount_main_debt"]),
			amountAccrual:                normalizeAmount(m["amount_accrual"]),
			amountFine:                   normalizeAmount(m["amount_fine"]),
			paymentDate:                  paymentDate,
			createdAt:                    nowPtr(),
			confirmed:                    false,
		})
	}

	if len(rows) == 0 {
		log.Printf("[PROC][payments][DONE] no valid rows")
		return nil
	}

	batchReq := &pgx.Batch{}
	for _, r := range rows {
		batchReq.Queue(
			`INSERT INTO `+paymentsTable+` (
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
			r.id, r.debtID, r.userID,
			r.amount, r.amountAfterSubtraction, r.amountGovernmentDuty,
			r.amountRepresentationExpenses, r.amountNotaryFees, r.amountPostage, r.confirmed,
			r.paymentDate, r.createdAt, r.amountAccountsReceivable, r.amountMainDebt,
			r.amountAccrual, r.amountFine,
		)
	}

	br := p.PG.Pool.SendBatch(ctx, batchReq)
	defer br.Close()

	inserted := 0
	for _, r := range rows {
		if _, err := br.Exec(); err != nil {
			logMongo(ctx, p.MG, importRecordID, "payments", r.id, r.payload, "failed", err.Error())
			continue
		}
		inserted++
		logMongo(ctx, p.MG, importRecordID, "payments", r.id, r.payload, "done", "")
	}

	log.Printf("[PROC][payments][DONE] total=%d inserted=%d", len(rows), inserted)

	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][payments][ERR] error change status: %v", err)
	}
	return nil
}
