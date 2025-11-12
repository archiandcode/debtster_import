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
}

func (p EnforcementProceedingsProcessor) Type() string { return "import_enforcement_proceedings" }

func (p *EnforcementProceedingsProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
	if err := CheckDeps(p); err != nil {
		return err
	}

	if p.EnfProcRepo == nil {
		p.EnfProcRepo = database.NewEnforcementProceedingsRepo(p.PG)
	}

	var importRecordID string
	if v := ctx.Value(ports.CtxImportRecordID); v != nil {
		if s, ok := v.(string); ok {
			importRecordID = strings.TrimSpace(s)
		}
	}

	debtsTable := "debts"
	log.Printf("[PROC][enf_proc][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	debtIDCache := make(map[string]*string)

	for i, m := range batch {
		modelID := uuid.NewString()

		v := func(key string) string { return strings.TrimSpace(m[key]) }

		debtNumber := strings.ReplaceAll(v("debt_number"), " ", "")
		if debtNumber == "" {
			logMongoFail(ctx, p.MG, importRecordID, "enforcement_proceedings", modelID, m, "missing debt_number")
			continue
		}

		debtUUID, err := getDebtUUID(ctx, p.PG, debtsTable, debtNumber, debtIDCache)
		if err != nil || debtUUID == nil {
			msg := "debt not found: " + debtNumber
			if err != nil {
				msg += " (" + err.Error() + ")"
			}
			logMongoFail(ctx, p.MG, importRecordID, "enforcement_proceedings", modelID, m, msg)
			continue
		}

		row := models.EnforcementProceeding{
			SerialNumber:         nullIfEmpty(v("enforcement_proceeding_serial_number")),
			DebtID:               debtUUID,
			Amount:               normalizeAmount(v("enforcement_proceeding_amount")), // "" -> 0
			PrivateBailiffName:   nullIfEmpty(v("enforcement_proceeding_private_bailiff_name")),
			PrivateBailiffRegion: nullIfEmpty(v("enforcement_proceeding_private_bailiff_region")),
			StartDate:            parseDateStrict(v("enforcement_proceeding_start_date")),
			StatusAISOIP:         nullIfEmpty(v("enforcement_proceeding_status_ais_oip")),
		}

		if err := p.EnfProcRepo.Create(ctx, row); err != nil {
			log.Printf("[PROC][enf_proc][WARN] row=%d insert failed: %v", i, err)
			logMongoFail(ctx, p.MG, importRecordID, "enforcement_proceedings", modelID, m, err.Error())
			continue
		}

		logMongo(ctx, p.MG, importRecordID, "enforcement_proceedings", modelID, m, "done", "")
	}

	log.Printf("[PROC][enf_proc][DONE] total=%d", len(batch))

	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][enf_proc][ERR] error change status: %v", err)
	}
	return nil
}
