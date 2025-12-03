package processors

import (
	"context"
	"debtster_import/internal/models"
	"debtster_import/internal/ports"
	"errors"
	"log"
	"strings"

	"debtster_import/internal/repository/database"
	importitems "debtster_import/internal/repository/imports"

	"github.com/google/uuid"
)

type AgreementsProcessor struct {
	*BaseProcessor
	AgreementsRepo *database.AgreementRepo
	DebtsRepo      *database.DebtsRepo
	UserRepo       *database.UserRepo
}

func (p AgreementsProcessor) Type() string { return "import_agreements" }

func (p *AgreementsProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
	if err := CheckDeps(p); err != nil {
		return err
	}

	var importRecordID string
	if v := ctx.Value(ports.CtxImportRecordID); v != nil {
		if s, ok := v.(string); ok {
			importRecordID = strings.TrimSpace(s)
		}
	}

	log.Printf("[PROC][agreements][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	typesTable := "agreement_types"
	typeIDCache := make(map[string]*int64)

	modelType := importitems.PHPModelByTable(p.AgreementsRepo.GetTableName())

	success, failed := 0, 0

	for i, m := range batch {
		modelID := uuid.NewString()

		debtNumber := strings.TrimSpace(m["debt_number"])
		if debtNumber == "" {
			failed++
			_, _ = importitems.InsertItem(ctx, p.MG, importitems.Item{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        modelID,
				Payload:        mustJSON(m),
				Status:         "failed",
				Errors:         "missing debt_number",
			})
			continue
		}

		debtUUID, err := p.DebtsRepo.GetIDByNumber(ctx, debtNumber)
		if err != nil || debtUUID == nil {
			failed++
			msg := "debt not found: " + debtNumber
			if err != nil {
				msg += " (" + err.Error() + ")"
			}
			_, _ = importitems.InsertItem(ctx, p.MG, importitems.Item{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        modelID,
				Payload:        mustJSON(m),
				Status:         "failed",
				Errors:         msg,
			})
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

		var agreementTypeID *int64
		if tname := strings.TrimSpace(m["agreement_type"]); tname != "" {
			if tid, err := p.getOrCreateAgreementType(ctx, typesTable, tname, typeIDCache); err == nil && tid != nil {
				agreementTypeID = tid
			} else if err != nil {
				warnings = append(warnings, "agreement_type upsert failed: "+err.Error())
			}
		} else {
			warnings = append(warnings, "missing agreement_type -> agreement_type_id=NULL")
		}

		amountDebt := normalizeAmount(m["agreement_amount_debt"])
		monthly := normalizeAmount(m["agreement_monthly_payment_amount"])
		schedDay := nullIfEmpty(strings.TrimSpace(m["agreement_scheduled_payment_day"]))
		start := parseDateStrict(m["agreement_start_date"])
		end := parseDateStrict(m["agreement_end_date"])

		_, err = p.AgreementsRepo.UpdateOrCreate(ctx, models.Agreement{
			AgreementTypeID:      agreementTypeID,
			DebtID:               debtUUID,
			UserID:               userID,
			AmountDebt:           amountDebt,
			MonthlyPaymentAmount: monthly,
			ScheduledPaymentDay:  schedDay,
			StartDate:            start,
			EndDate:              end,
			CreatedAt:            nowPtr(),
		})
		if err != nil {
			failed++
			log.Printf("[PROC][agreements][ERR] row=%d debt=%s err=%v", i, debtNumber, err)
			_, _ = importitems.InsertItem(ctx, p.MG, importitems.Item{
				ImportRecordID: importRecordID,
				ModelType:      modelType,
				ModelID:        modelID,
				Payload:        mustJSON(m),
				Status:         "failed",
				Errors:         err.Error(),
			})
			continue
		}

		success++
		_, _ = importitems.InsertItem(ctx, p.MG, importitems.Item{
			ImportRecordID: importRecordID,
			ModelType:      modelType,
			ModelID:        modelID,
			Payload:        mustJSON(m),
			Status:         "done",
			Errors:         strings.Join(warnings, "; "),
		})
	}

	log.Printf("[PROC][agreements][DONE] total=%d success=%d failed=%d", len(batch), success, failed)
	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][agreements][ERR] update import_record status: %v", err)
	}
	return nil
}

func (p AgreementsProcessor) getOrCreateAgreementType(ctx context.Context, table, name string, cache map[string]*int64) (*int64, error) {
	key := strings.ToLower(strings.TrimSpace(name))
	if v, ok := cache[key]; ok {
		return v, nil
	}
	var id int64
	if err := p.PG.Pool.QueryRow(ctx, "SELECT id FROM "+table+" WHERE LOWER(name)=LOWER($1) LIMIT 1", name).Scan(&id); err == nil {
		cache[key] = &id
		return &id, nil
	}
	if err := p.PG.Pool.QueryRow(ctx, "INSERT INTO "+table+" (name,created_at) VALUES ($1,NOW()) RETURNING id", name).Scan(&id); err == nil {
		cache[key] = &id
		return &id, nil
	}
	if err := p.PG.Pool.QueryRow(ctx, "SELECT id FROM "+table+" WHERE LOWER(name)=LOWER($1) LIMIT 1", name).Scan(&id); err == nil {
		cache[key] = &id
		return &id, nil
	}
	return nil, errors.New("agreement_type not found/created: " + name)
}
