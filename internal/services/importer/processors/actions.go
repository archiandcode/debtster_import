package processors

import (
	"context"
	"debtster_import/internal/models"
	"debtster_import/internal/repository/database"
	"log"
	"strings"

	"debtster_import/internal/ports"
	importitems "debtster_import/internal/repository/imports"

	"github.com/google/uuid"
)

type ActionsProcessor struct {
	*BaseProcessor
	DebtsRepo        *database.DebtsRepo
	ActionsRepo      *database.ActionRepo
	UserRepo         *database.UserRepo
	DebtStatusesRepo *database.DebtStatusesRepo
}

func (p ActionsProcessor) Type() string { return "import_actions" }

func (p *ActionsProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
	log.Printf("[PROC][actions][CHECK] Mongo client: %v, db: %v",
		p.MG != nil && p.MG.Client != nil, p.MG != nil && p.MG.Database != nil)

	if err := CheckDeps(p); err != nil {
		return err
	}

	var importRecordID string
	if v := ctx.Value(ports.CtxImportRecordID); v != nil {
		if s, ok := v.(string); ok {
			importRecordID = strings.TrimSpace(s)
		}
	}

	log.Printf("[PROC][actions][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	type meta struct {
		id       string
		data     map[string]string
		warnings []string
	}

	modelType := importitems.PHPModelByTable(p.ActionsRepo.GetTableName())

	actions := make([]models.Action, 0, len(batch))
	metas := make([]meta, 0, len(batch))

	for i, m := range batch {
		debtNumber := strings.TrimSpace(m["debt_number"])
		if debtNumber == "" {
			if _, err := importitems.InsertItem(ctx, p.MG, importitems.Item{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        uuid.NewString(),
				Payload:        mustJSON(m),
				Status:         "failed",
				Errors:         "missing debt_number",
			}); err != nil {
				log.Printf("[PROC][actions][MONGO][ERR] row=%d missing debt_number: %v", i, err)
			}
			continue
		}

		debtID, err := p.DebtsRepo.GetIDByNumber(ctx, debtNumber)
		if err != nil || debtID == nil {
			msg := "debt not found: " + debtNumber
			if err != nil {
				msg += " (" + err.Error() + ")"
			}
			if _, mErr := importitems.InsertItem(ctx, p.MG, importitems.Item{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        uuid.NewString(),
				Payload:        mustJSON(m),
				Status:         "failed",
				Errors:         msg,
			}); mErr != nil {
				log.Printf("[PROC][actions][MONGO][ERR] row=%d debt not found: %v", i, mErr)
			}
			continue
		}

		var warnings []string

		var userID *int64
		if un := strings.TrimSpace(m["username"]); un == "" {
			warnings = append(warnings, "missing username -> user_id=NULL")
		} else {
			if uid, err := p.UserRepo.GetUserBigint(ctx, un); err == nil && uid != nil {
				userID = uid
			} else {
				warnings = append(warnings, "username not found: "+un+" -> user_id=NULL")
			}
		}

		var statusID *int64
		if st := strings.TrimSpace(m["status"]); st == "" {
			warnings = append(warnings, "missing status -> debt_status_id=NULL")
		} else {
			if sid, err := p.DebtStatusesRepo.GetStatusBigint(ctx, st); err == nil && sid != nil {
				statusID = sid
			} else {
				warnings = append(warnings, "status not found: "+st+" -> debt_status_id=NULL")
			}
		}

		id := uuid.NewString()

		action := models.Action{
			ID:           id,
			DebtID:       debtID,
			UserID:       userID,
			DebtStatusID: statusID,
			Type:         nullIfEmpty(strings.TrimSpace(m["type"])),
			Comment:      nullIfEmpty(strings.TrimSpace(m["comment"])),
			CreatedAt:    parseTimeLoose(m["created_at"]),
		}

		actions = append(actions, action)

		metas = append(metas, meta{
			id:       id,
			data:     m,
			warnings: warnings,
		})
	}

	if len(actions) == 0 {
		log.Printf("[PROC][actions][DONE] no valid rows")
		return nil
	}

	if err := p.ActionsRepo.InsertActions(ctx, actions); err != nil {
		log.Printf("[PROC][actions][ERR] batch insert failed: %v", err)
		for _, m := range metas {
			if _, mErr := importitems.InsertItem(ctx, p.MG, importitems.Item{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        m.id,
				Payload:        mustJSON(m.data),
				Status:         "failed",
				Errors:         err.Error(),
			}); mErr != nil {
				log.Printf("[PROC][actions][MONGO][ERR] id=%s status=failed err=%v", m.id, mErr)
			}
		}
		log.Printf("[PROC][actions][DONE] total=%d inserted=0", len(actions))
		if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
			log.Printf("[PROC][actions][ERR] error change status: %v", err)
		}
		return nil
	}

	inserted := 0

	for _, m := range metas {
		errText := strings.Join(m.warnings, "; ")
		if res, mErr := importitems.InsertItem(ctx, p.MG, importitems.Item{
			ImportRecordID: importRecordID,
			ModelType:      modelType,
			ModelID:        m.id,
			Payload:        mustJSON(m.data),
			Status:         "done",
			Errors:         errText,
		}); mErr != nil {
			log.Printf("[PROC][actions][MONGO][ERR] id=%s status=done err=%v", m.id, mErr)
		} else {
			log.Printf("[PROC][actions][MONGO][OK] id=%s status=done inserted_id=%v", m.id, res.InsertedID)
		}
		inserted++
	}

	log.Printf("[PROC][actions][DONE] total=%d inserted=%d", len(actions), inserted)

	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][actions][ERR] error change status: %v", err)
	}

	return nil
}
