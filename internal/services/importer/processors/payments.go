package processors

import (
	"context"
	"log"
	"strings"
	"time"

	"debtster_import/internal/models"
	"debtster_import/internal/ports"
	"debtster_import/internal/repository/database"
	importitems "debtster_import/internal/repository/imports"

	"github.com/google/uuid"
)

type PaymentsProcessor struct {
	*BaseProcessor
	PayRepo *database.PaymentRepo
}

func (p PaymentsProcessor) Type() string { return "add_payments" }

func (p *PaymentsProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
	if err := CheckDeps(p); err != nil {
		return err
	}

	// Ленивая инициализация репозитория
	if p.PayRepo == nil {
		p.PayRepo = database.NewPaymentRepo(p.PG)
	}

	// import_record_id из контекста
	var importRecordID string
	if v := ctx.Value(ports.CtxImportRecordID); v != nil {
		if s, ok := v.(string); ok {
			importRecordID = strings.TrimSpace(s)
		}
	}

	debtsTable := "debts"
	usersTable := "users"

	log.Printf("[PROC][payments][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	// Кэши для ускорения поиска
	debtIDCache := make(map[string]*string)
	userIDCache := make(map[string]*int64)

	inserted := 0

	for _, m := range batch {
		id := uuid.NewString()

		// Хелпер для тримминга значений
		v := func(key string) string { return strings.TrimSpace(m[key]) }

		// debt_number -> debt_id
		debtNumber := v("debt_number")
		if debtNumber == "" {
			logMongoFail(ctx, p.MG, importRecordID, "payments", id, m, "missing debt_number")
			continue
		}
		debtUUID, err := getDebtUUID(ctx, p.PG, debtsTable, debtNumber, debtIDCache)
		if err != nil || debtUUID == nil {
			msg := "debt not found: " + debtNumber
			if err != nil {
				msg += " (" + err.Error() + ")"
			}
			logMongoFail(ctx, p.MG, importRecordID, "payments", id, m, msg)
			continue
		}

		// username -> user_id
		username := v("username")
		if username == "" {
			logMongoFail(ctx, p.MG, importRecordID, "payments", id, m, "missing username")
			continue
		}
		userID, err := getUserBigint(ctx, p.PG, usersTable, username, userIDCache)
		if err != nil || userID == nil {
			msg := "username not found: " + username
			if err != nil {
				msg += " (" + err.Error() + ")"
			}
			logMongoFail(ctx, p.MG, importRecordID, "payments", id, m, msg)
			continue
		}

		// Дата платежа
		paymentDate := parseDateStrict(v("payment_date"))
		if paymentDate == nil {
			logMongoFail(ctx, p.MG, importRecordID, "payments", id, m, "bad payment_date")
			continue
		}

		// Сумма (обязательная и не нулевая)
		amount := normalizeAmount(v("amount"))
		if amount == "" || amount == "0" {
			logMongoFail(ctx, p.MG, importRecordID, "payments", id, m, "missing/zero amount")
			continue
		}

		// Собираем модель для репозитория
		pay := models.Payment{
			ID:     id,
			DebtID: *debtUUID,
			UserID: *userID,

			Amount:                       amount,
			AmountAfterSubtraction:       numPtr(normalizeAmount(v("amount_after_subtraction"))),
			AmountGovernmentDuty:         numPtr(normalizeAmount(v("amount_government_duty"))),
			AmountRepresentationExpenses: numPtr(normalizeAmount(v("amount_representation_expenses"))),
			AmountNotaryFees:             numPtr(normalizeAmount(v("amount_notary_fees"))),
			AmountPostage:                numPtr(normalizeAmount(v("amount_postage"))),
			AmountAccountsReceivable:     numPtr(normalizeAmount(v("amount_accounts_receivable"))),
			AmountMainDebt:               numPtr(normalizeAmount(v("amount_main_debt"))),
			AmountAccrual:                numPtr(normalizeAmount(v("amount_accrual"))),
			AmountFine:                   numPtr(normalizeAmount(v("amount_fine"))),

			PaymentDate: paymentDate, // created_at ставится в SQL (NOW())
			Confirmed:   false,
		}

		// Вставка через репозиторий (ON CONFLICT DO NOTHING — внутри SQL)
		if err := p.PayRepo.Create(ctx, pay); err != nil {
			logMongo(ctx, p.MG, importRecordID, "payments", id, m, "failed", err.Error())
			continue
		}

		inserted++
		logMongo(ctx, p.MG, importRecordID, "payments", id, m, "done", "")
	}

	log.Printf("[PROC][payments][DONE] total=%d inserted=%d", len(batch), inserted)

	// Завершаем импорт
	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][payments][ERR] error change status: %v", err)
	}
	return nil
}

// numPtr: пустую строку превращаем в NULL, чтобы не было ""::numeric.
func numPtr(s string) *string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	return &s
}

// Заглушка для совместимости, если где-то нужен *time.Time "сейчас".
func nowPtr() *time.Time {
	t := time.Now()
	return &t
}
