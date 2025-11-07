package processors

import (
	"context"
	"errors"
	"log"
	"strconv"
	"strings"
	"time"

	mg "debtster_import/internal/config/connections/mongo"
	"debtster_import/internal/config/connections/postgres"
	"debtster_import/internal/ports"
	importitems "debtster_import/internal/repository/imports"

	"github.com/google/uuid"
)

type UpdateDebtsProcessor struct {
	PG *postgres.Postgres
	MG *mg.Mongo

	DebtsTable string
}

func (p UpdateDebtsProcessor) Type() string { return "update_debts" }

func (p UpdateDebtsProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
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

	debtsTable := firstNonEmpty(p.DebtsTable, "debts")
	log.Printf("[PROC][update_debts][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	updated := 0

	for i, m := range batch {
		debtNumber := strings.TrimSpace(strings.ReplaceAll(m["debt_number"], " ", ""))
		if debtNumber == "" {
			logMongoFail(ctx, p.MG, importRecordID, debtsTable, "", m, "missing debt_number")
			continue
		}

		setParts := make([]string, 0)
		args := make([]any, 0)
		argIdx := 1

		appendSet := func(col string, val any) {
			setParts = append(setParts, col+"=$"+strconv.Itoa(argIdx))
			args = append(args, val)
			argIdx++
		}

		// ---------- SAFE CASTS ----------
		if v := strings.TrimSpace(m["debt_status"]); v != "" {
			appendSet("status_id", v)
		}
		if v := strings.TrimSpace(m["debt_end_date"]); v != "" {
			appendSet("end_date", parseDateStrict(v))
		}
		if v := strings.TrimSpace(m["debt_amount_actual_debt"]); v != "" {
			appendSet("amount_actual_debt", normalizeAmount(v))
		}
		if v := strings.TrimSpace(m["debt_amount_main_debt"]); v != "" {
			appendSet("amount_main_debt", normalizeAmount(v))
		}
		if v := strings.TrimSpace(m["debt_amount_fine"]); v != "" {
			appendSet("amount_fine", normalizeAmount(v))
		}
		if v := strings.TrimSpace(m["debt_amount_accrual"]); v != "" {
			appendSet("amount_accrual", normalizeAmount(v))
		}
		if v := strings.TrimSpace(m["debt_username"]); v != "" {
			if _, err := uuid.Parse(v); err == nil {
				appendSet("user_id", v)
			}
		}
		if v := strings.TrimSpace(m["debt_counterparty"]); v != "" {
			if _, err := uuid.Parse(v); err == nil {
				appendSet("counterparty_id", v)
			}
		}
		if v := strings.TrimSpace(m["debt_currency"]); v != "" {
			appendSet("currency", v)
		}

		if len(setParts) == 0 {
			logMongoFail(ctx, p.MG, importRecordID, debtsTable, "", m, "no updatable fields found")
			continue
		}

		// --- Финальный SQL ---
		query := `UPDATE ` + debtsTable + ` SET ` + strings.Join(setParts, ", ") +
			`, updated_at=$` + strconv.Itoa(argIdx) +
			` WHERE number=$` + strconv.Itoa(argIdx+1)
		args = append(args, time.Now(), debtNumber)

		ct, err := p.PG.Pool.Exec(ctx, query, args...)
		if err != nil {
			logMongo(ctx, p.MG, importRecordID, debtsTable, "", m, "failed", err.Error())
			log.Printf("[PROC][update_debts][WARN] row=%d update failed: %v", i, err)
			continue
		}

		if ct.RowsAffected() == 0 {
			// Не найден договор — логируем как ошибку
			logMongoFail(ctx, p.MG, importRecordID, debtsTable, "", m, "debt not found: "+debtNumber)
			log.Printf("[PROC][update_debts][MISS] row=%d debt_number=%s not found", i, debtNumber)
			continue
		}

		updated++
		logMongo(ctx, p.MG, importRecordID, debtsTable, "", m, "done", "")
	}

	log.Printf("[PROC][update_debts][DONE] total=%d updated=%d", len(batch), updated)

	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][update_debts][ERR] error change status: %v", err)
	}
	return nil
}
