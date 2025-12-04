package processors

import (
	"context"
	"debtster_import/internal/models"
	"log"
	"strconv"
	"strings"
	"time"

	"debtster_import/internal/ports"
	"debtster_import/internal/repository/database"
	importitems "debtster_import/internal/repository/imports"

	"github.com/google/uuid"
)

type UserPlansProcessor struct {
	*BaseProcessor
	UserPlansRepo *database.UserPlanRepo
	UserRepo      *database.UserRepo
}

func (p UserPlansProcessor) Type() string { return "import_user_plans" }

func (p *UserPlansProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
	if err := CheckDeps(p); err != nil {
		return err
	}

	var importRecordID string
	if v := ctx.Value(ports.CtxImportRecordID); v != nil {
		if s, ok := v.(string); ok {
			importRecordID = strings.TrimSpace(s)
		}
	}

	log.Printf("[PROC][user_plans][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	success, failed := 0, 0
	modelType := "user_plans" // можно заменить на importitems.PHPModelByTable при необходимости

	for i, m := range batch {

		modelID := uuid.NewString()

		// --------------------------------
		// 1. username
		// --------------------------------
		username := strings.TrimSpace(m["username"])
		if username == "" {
			failed++
			importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        modelID,
				Payload:        m,
				Errors:         "missing username",
			})
			continue
		}

		uid, err := p.UserRepo.GetUserBigint(ctx, username)
		if err != nil || uid == nil {
			failed++
			msg := "username not found: " + username
			if err != nil {
				msg += " (" + err.Error() + ")"
			}
			importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        modelID,
				Payload:        m,
				Errors:         msg,
			})
			continue
		}

		// --------------------------------
		// 2. amount
		// --------------------------------
		amount := normalizeAmount(m["user_plan_amount"])

		// --------------------------------
		// 3. quantity
		// --------------------------------
		qty := strings.TrimSpace(m["user_plan_quantity"])
		if qty == "" {
			qty = "0"
		} else {
			qty = strings.NewReplacer(" ", "", ",", "", ".", "").Replace(qty)
			if _, err := strconv.ParseInt(qty, 10, 64); err != nil {
				failed++
				importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
					ImportRecordID: importRecordID,
					ModelType:      modelType,
					ModelID:        modelID,
					Payload:        m,
					Errors:         "bad user_plan_quantity",
				})
				continue
			}
		}

		// --------------------------------
		// 4. end_date
		// --------------------------------
		var endDate *time.Time

		if d := strings.TrimSpace(m["end_date"]); d != "" {
			if parsed := parseDateStrict(d); parsed != nil {
				endDate = parsed
			} else {
				failed++
				importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
					ImportRecordID: importRecordID,
					ModelType:      modelType,
					ModelID:        modelID,
					Payload:        m,
					Errors:         "bad end_date",
				})
				continue
			}
		}

		// --------------------------------
		// 5. upsert
		// --------------------------------
		err = p.UserPlansRepo.UpdateOrCreate(ctx, models.UserPlan{
			UserID:   uid,
			Amount:   amount,
			Quantity: qty,
			EndDate:  endDate,
		})
		if err != nil {
			failed++
			log.Printf("[PROC][user_plans][ERR] row=%d username=%s err=%v", i, username, err)

			importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        modelID,
				Payload:        m,
				Errors:         err.Error(),
			})
			continue
		}

		// --------------------------------
		// 6. success log
		// --------------------------------
		success++
		importitems.LogMongo(ctx, p.MG, importitems.LogParams{
			ImportRecordID: importRecordID,
			ModelType:      modelType,
			ModelID:        modelID,
			Payload:        m,
			Status:         "done",
			Errors:         "",
		})
	}

	// --------------------------------
	// Итоговый лог
	// --------------------------------
	log.Printf("[PROC][user_plans][DONE] total=%d success=%d failed=%d", len(batch), success, failed)

	// --------------------------------
	// Обновление статуса import_record
	// --------------------------------
	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][user_plans][ERR] update import_record status: %v", err)
	}

	return nil
}
