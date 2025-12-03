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

	var importRecordID string
	if v := ctx.Value(ports.CtxImportRecordID); v != nil {
		if s, ok := v.(string); ok {
			importRecordID = strings.TrimSpace(s)
		}
	}

	log.Printf("[PROC][payments][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	inserted := 0

	for _, m := range batch {
		id := uuid.NewString()
		v := func(key string) string { return strings.TrimSpace(m[key]) }

		// ---- debt_number → debt_id ----
		debtNumber := v("debt_number")
		if debtNumber == "" {
			logMongoFail(ctx, p.MG, importRecordID, "payments", id, m, "missing debt_number")
			continue
		}
		debtUUID, err := p.DebtsRepo.GetIDByNumber(ctx, debtNumber)
		if err != nil || debtUUID == nil {
			msg := "debt not found: " + debtNumber
			if err != nil {
				msg += " (" + err.Error() + ")"
			}
			logMongoFail(ctx, p.MG, importRecordID, "payments", id, m, msg)
			continue
		}

		// ---- username → user_id ----
		username := v("username")
		if username == "" {
			logMongoFail(ctx, p.MG, importRecordID, "payments", id, m, "missing username")
			continue
		}
		userID, err := p.UserRepo.GetUserBigint(ctx, username)
		if err != nil || userID == nil {
			msg := "username not found: " + username
			if err != nil {
				msg += " (" + err.Error() + ")"
			}
			logMongoFail(ctx, p.MG, importRecordID, "payments", id, m, msg)
			continue
		}

		// ---- дата платежа ----
		paymentDate := parseDateStrict(v("payment_date"))
		if paymentDate == nil {
			logMongoFail(ctx, p.MG, importRecordID, "payments", id, m, "bad payment_date")
			continue
		}

		// ---- сумма (обязательна и не 0) ----
		amount := normalizeAmount(v("amount"))
		if amount == "" || amount == "0" {
			logMongoFail(ctx, p.MG, importRecordID, "payments", id, m, "missing/zero amount")
			continue
		}

		// Собираем модель
		pay := models.Payment{
			ID:     id,
			DebtID: *debtUUID,
			// т.к. в модели UserID == string
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

		if err := p.PayRepo.Create(ctx, pay); err != nil {
			logMongo(ctx, p.MG, importRecordID, "payments", id, m, "failed", err.Error())
			continue
		}

		inserted++
		logMongo(ctx, p.MG, importRecordID, "payments", id, m, "done", "")
	}

	log.Printf("[PROC][payments][DONE] total=%d inserted=%d", len(batch), inserted)

	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][payments][ERR] error change status: %v", err)
	}
	return nil
}
