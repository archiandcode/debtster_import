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

	// ------------------------------------------------------------------
	// Получаем import_record_id
	// ------------------------------------------------------------------
	var importRecordID string
	if v := ctx.Value(ports.CtxImportRecordID); v != nil {
		if s, ok := v.(string); ok {
			importRecordID = strings.TrimSpace(s)
		}
	}

	modelType := importitems.PHPModelByTable(p.EnfProcRepo.GetTableName())

	log.Printf("[PROC][enf_proc][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	// ------------------------------------------------------------------
	// Начинаем обработку
	// ------------------------------------------------------------------
	for _, m := range batch {
		modelID := uuid.NewString()
		f := func(key string) string { return strings.TrimSpace(m[key]) }

		debtNumber := strings.ReplaceAll(f("debt_number"), " ", "")
		if debtNumber == "" {
			importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        modelID,
				Payload:        m,
				Errors:         "missing debt_number",
			})
			continue
		}

		// ------------------------------------------------------------------
		// Ищем debt
		// ------------------------------------------------------------------
		debtUUID, err := p.DebtsRepo.GetIDByNumber(ctx, debtNumber)
		if err != nil || debtUUID == nil {
			msg := "debt not found: " + debtNumber
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

		// ------------------------------------------------------------------
		// Формируем модель
		// ------------------------------------------------------------------
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

		// ------------------------------------------------------------------
		// Upsert
		// ------------------------------------------------------------------
		if _, err := p.EnfProcRepo.Upsert(ctx, row); err != nil {
			importitems.LogMongoFail(ctx, p.MG, importitems.LogParams{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        modelID,
				Payload:        m,
				Errors:         err.Error(),
			})
			continue
		}

		// ------------------------------------------------------------------
		// Успешная запись
		// ------------------------------------------------------------------
		importitems.LogMongo(ctx, p.MG, importitems.LogParams{
			ImportRecordID: importRecordID,
			ModelType:      modelType,
			ModelID:        modelID,
			Payload:        m,
			Status:         "done",
			Errors:         "",
		})
	}

	log.Printf("[PROC][enf_proc][DONE] total=%d", len(batch))

	// ------------------------------------------------------------------
	// Обновляем статус import_record
	// ------------------------------------------------------------------
	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][enf_proc][ERR] error change status: %v", err)
	}

	return nil
}
