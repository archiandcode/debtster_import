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

	usersTable := "users"
	log.Printf("[PROC][user_plans][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	userIDCache := make(map[string]*int64)
	success, failed := 0, 0

	for i, m := range batch {
		modelID := uuid.NewString()

		username := strings.TrimSpace(m["username"])
		if username == "" {
			failed++
			logMongoFail(ctx, p.MG, importRecordID, "user_plans", modelID, m, "missing username")
			continue
		}
		uid, err := getUserBigint(ctx, p.PG, usersTable, username, userIDCache)
		if err != nil || uid == nil {
			failed++
			msg := "username not found: " + username
			if err != nil {
				msg += " (" + err.Error() + ")"
			}
			logMongoFail(ctx, p.MG, importRecordID, "user_plans", modelID, m, msg)
			continue
		}

		amount := normalizeAmount(m["user_plan_amount"])

		qty := strings.TrimSpace(m["user_plan_quantity"])
		if qty == "" {
			qty = "0"
		} else {
			qty = strings.NewReplacer(" ", "", ",", "", ".", "").Replace(qty)
			if _, err := strconv.ParseInt(qty, 10, 64); err != nil {
				failed++
				logMongoFail(ctx, p.MG, importRecordID, "user_plans", modelID, m, "bad user_plan_quantity")
				continue
			}
		}

		var endDate *time.Time
		if d := strings.TrimSpace(m["end_date"]); d != "" {
			if parsed := parseDateStrict(d); parsed != nil {
				endDate = parsed
			} else {
				failed++
				logMongoFail(ctx, p.MG, importRecordID, "user_plans", modelID, m, "bad end_date")
				continue
			}
		} else {
			endDate = nil
		}

		err = p.UserPlansRepo.UpdateOrCreate(ctx, models.UserPlan{
			UserID:   uid,
			Amount:   amount,
			Quantity: qty,
			EndDate:  endDate,
		})
		if err != nil {
			failed++
			log.Printf("[PROC][user_plans][ERR] row=%d username=%s err=%v", i, username, err)
			logMongoFail(ctx, p.MG, importRecordID, "user_plans", modelID, m, err.Error())
			continue
		}

		success++
		logMongo(ctx, p.MG, importRecordID, "user_plans", modelID, m, "done", "")
	}

	log.Printf("[PROC][user_plans][DONE] total=%d success=%d failed=%d", len(batch), success, failed)
	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][user_plans][ERR] update import_record status: %v", err)
	}
	return nil
}
