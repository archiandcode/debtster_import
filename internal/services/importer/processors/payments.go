package processors

import (
	"context"
	"debtster_import/internal/models"
	"debtster_import/internal/ports"
	"debtster_import/internal/repository/database"
	importitems "debtster_import/internal/repository/imports"
	"log"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

type PaymentsProcessor struct {
	*BaseProcessor
	PayRepo   *database.PaymentRepo
	DebtsRepo *database.DebtsRepo
	UserRepo  *database.UserRepo
}

func (p PaymentsProcessor) Type() string { return "add_payments" }

func (p *PaymentsProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
	if err := CheckDeps(p); err != nil {
		return err
	}

	if p.PayRepo == nil {
		p.PayRepo = database.NewPaymentRepo(p.PG)
	}
	if p.DebtsRepo == nil {
		p.DebtsRepo = database.NewDebtsRepo(p.PG)
	}
	if p.UserRepo == nil {
		p.UserRepo = database.NewUserRepo(p.PG)
	}

	var importRecordID string
	if v := ctx.Value(ports.CtxImportRecordID); v != nil {
		if s, ok := v.(string); ok {
			importRecordID = strings.TrimSpace(s)
		}
	}

	log.Printf("[PROC][payments][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	debtCache := make(map[string]*string)
	userCache := make(map[string]*int64)

	type preparedRow struct {
		id      string
		payment models.Payment
		payload map[string]string
	}

	prepared := make([]preparedRow, 0, len(batch))

	for _, m := range batch {
		id := uuid.NewString()
		v := func(key string) string { return strings.TrimSpace(m[key]) }

		// ---------------------- debt ----------------------
		debtNumber := v("debt_number")
		if debtNumber == "" {
			logMongoFail(ctx, p.MG, importRecordID, "payments", id, m, "missing debt_number")
			continue
		}

		var debtUUID *string
		if cached, ok := debtCache[debtNumber]; ok {
			debtUUID = cached
		} else {
			var err error
			debtUUID, err = p.DebtsRepo.GetIDByNumber(ctx, debtNumber)
			if err != nil || debtUUID == nil {
				msg := "debt not found: " + debtNumber
				if err != nil {
					msg += " (" + err.Error() + ")"
				}
				logMongoFail(ctx, p.MG, importRecordID, "payments", id, m, msg)
				continue
			}
			debtCache[debtNumber] = debtUUID
		}

		// ---------------------- user ----------------------
		username := v("username")
		if username == "" {
			logMongoFail(ctx, p.MG, importRecordID, "payments", id, m, "missing username")
			continue
		}

		var userID *int64
		if cached, ok := userCache[username]; ok {
			userID = cached
		} else {
			var err error
			userID, err = p.UserRepo.GetUserBigint(ctx, username)
			if err != nil || userID == nil {
				msg := "username not found: " + username
				if err != nil {
					msg += " (" + err.Error() + ")"
				}
				logMongoFail(ctx, p.MG, importRecordID, "payments", id, m, msg)
				continue
			}
			userCache[username] = userID
		}

		// ---------------------- date ----------------------
		paymentDate := parseDateStrict(v("payment_date"))
		if paymentDate == nil {
			logMongoFail(ctx, p.MG, importRecordID, "payments", id, m, "bad payment_date")
			continue
		}

		// ---------------------- amount ----------------------
		amount := normalizeAmount(v("amount"))
		if amount == "" || amount == "0" {
			logMongoFail(ctx, p.MG, importRecordID, "payments", id, m, "missing/zero amount")
			continue
		}

		// ---------------------- model ----------------------
		pay := models.Payment{
			ID:     id,
			DebtID: *debtUUID,
			UserID: strconv.FormatInt(*userID, 10),

			Amount:                       amount,
			AmountAfterSubtraction:       normalizeAmount(v("amount_after_subtraction")),
			AmountGovernmentDuty:         normalizeAmount(v("amount_government_duty")),
			AmountRepresentationExpenses: normalizeAmount(v("amount_representation_expenses")),
			AmountNotaryFees:             normalizeAmount(v("amount_notary_fees")),
			AmountPostage:                normalizeAmount(v("amount_postage")),
			AmountAccountsReceivable:     normalizeAmount(v("amount_accounts_receivable")),
			AmountMainDebt:               normalizeAmount(v("amount_main_debt")),
			AmountAccrual:                normalizeAmount(v("amount_accrual")),
			AmountFine:                   normalizeAmount(v("amount_fine")),

			PaymentDate: paymentDate,
			Confirmed:   false,
		}

		prepared = append(prepared, preparedRow{id: id, payment: pay, payload: m})
	}

	if len(prepared) == 0 {
		log.Printf("[PROC][payments][DONE] no valid rows")
		return nil
	}

	payments := make([]models.Payment, 0, len(prepared))
	for _, pr := range prepared {
		payments = append(payments, pr.payment)
	}

	errs := p.PayRepo.CreateBatch(ctx, payments)

	inserted := 0
	for i, pr := range prepared {
		if errs != nil && errs[i] != nil {
			logMongo(ctx, p.MG, importRecordID, "payments", pr.id, pr.payload, "failed", errs[i].Error())
			continue
		}
		inserted++
		logMongo(ctx, p.MG, importRecordID, "payments", pr.id, pr.payload, "done", "")
	}

	log.Printf("[PROC][payments][DONE] total=%d inserted=%d", len(prepared), inserted)

	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][payments][ERR] error change status: %v", err)
	}
	return nil
}
