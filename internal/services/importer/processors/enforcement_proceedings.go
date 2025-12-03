package processors

import (
	"context"
	"log"
	"strings"

	"debtster_import/internal/models"
	"debtster_import/internal/ports"
	"debtster_import/internal/repository/database"
	importitems "debtster_import/internal/repository/imports"

	"github.com/google/uuid"
)

type EnforcementProceedingsProcessor struct {
	*BaseProcessor
	EnfProcRepo *database.EnforcementProceedingsRepo
	DebtsRepo   *database.DebtsRepo
}

func (p EnforcementProceedingsProcessor) Type() string { return "import_enforcement_proceedings" }

func (p *EnforcementProceedingsProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
	if err := CheckDeps(p); err != nil {
		return err
	}

	var importRecordID string
	if v := ctx.Value(ports.CtxImportRecordID); v != nil {
		if s, ok := v.(string); ok {
			importRecordID = strings.TrimSpace(s)
		}
	}

	modelType := importitems.PHPModelByTable(p.EnfProcRepo.GetTableName())
	log.Printf("[PROC][enf_proc][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	for _, m := range batch {
		modelID := uuid.NewString()

		f := func(key string) string { return strings.TrimSpace(m[key]) }
		debtNumber := strings.ReplaceAll(f("debt_number"), " ", "")
		if debtNumber == "" {
			logMongoFail(ctx, p.MG, importRecordID, modelType, modelID, m, "missing debt_number")
			continue
		}

		debtUUID, err := p.DebtsRepo.GetIDByNumber(ctx, debtNumber)
		if err != nil || debtUUID == nil {
			msg := "debt not found: " + debtNumber
			if err != nil {
				msg += " (" + err.Error() + ")"
			}
			logMongoFail(ctx, p.MG, importRecordID, modelType, modelID, m, msg)
			continue
		}

		row := models.EnforcementProceeding{
			SerialNumber:         nullIfEmpty(f("enforcement_proceeding_serial_number")),
			DebtID:               debtUUID,
			Amount:               normalizeAmount(f("enforcement_proceeding_amount")),
			PrivateBailiffName:   nullIfEmpty(f("enforcement_proceeding_private_bailiff_name")),
			PrivateBailiffRegion: nullIfEmpty(f("enforcement_proceeding_private_bailiff_region")),
			StartDate:            parseDateStrict(f("enforcement_proceeding_start_date")),
			StatusAISOIP:         nullIfEmpty(f("enforcement_proceeding_status_ais_oip")),
			CreatedAt:            nowPtr(),
		}

		if _, err := p.EnfProcRepo.Upsert(ctx, row); err != nil {
			logMongoFail(ctx, p.MG, importRecordID, modelType, modelID, m, err.Error())
			continue
		}

		logMongo(ctx, p.MG, importRecordID, modelType, modelID, m, "done", "")
	}

	log.Printf("[PROC][enf_proc][DONE] total=%d", len(batch))

	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][enf_proc][ERR] error change status: %v", err)
	}
	return nil
}
