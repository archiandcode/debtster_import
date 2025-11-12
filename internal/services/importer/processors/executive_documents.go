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

type ExecutiveDocumentsProcessor struct {
	*BaseProcessor
	ExecDocsRepo *database.ExecutiveDocumentsRepo
}

func (p ExecutiveDocumentsProcessor) Type() string { return "import_executive_documents" }

func (p *ExecutiveDocumentsProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
	if err := CheckDeps(p); err != nil {
		return err
	}

	if p.ExecDocsRepo == nil {
		p.ExecDocsRepo = database.NewExecutiveDocumentsRepo(p.PG)
	}

	var importRecordID string
	if v := ctx.Value(ports.CtxImportRecordID); v != nil {
		if s, ok := v.(string); ok {
			importRecordID = strings.TrimSpace(s)
		}
	}

	debtsTable := "debts"
	log.Printf("[PROC][exec_docs][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	debtIDCache := make(map[string]*string)

	for i, m := range batch {
		modelID := uuid.NewString()
		var warnings []string

		v := func(key string) string { return strings.TrimSpace(m[key]) }

		var debtUUID *string
		if dn := strings.ReplaceAll(v("debt_number"), " ", ""); dn != "" {
			if du, err := getDebtUUID(ctx, p.PG, debtsTable, dn, debtIDCache); err == nil {
				debtUUID = du
				if du == nil {
					warnings = append(warnings, "debt not found: "+dn+" -> debt_id=NULL")
				}
			} else {
				warnings = append(warnings, "debt lookup error: "+err.Error()+" -> debt_id=NULL")
			}
		} else {
			warnings = append(warnings, "missing debt_number -> debt_id=NULL")
		}

		docType := v("executive_document_type")
		if docType == "" {
			logMongoFail(ctx, p.MG, importRecordID, "executive_documents", modelID, m, "missing executive_document_type")
			continue
		}

		if v("document_has_estate") != "" {
			warnings = append(warnings, "document_has_estate provided but column absent -> ignored")
		}

		doc := models.ExecutiveDocument{
			ID:                      modelID,
			DocType:                 docType,
			SerialNumber:            nullIfEmpty(v("executive_document_serial_number")),
			DebtID:                  debtUUID,
			Amount:                  normalizeAmount(v("executive_document_amount")),
			StartDate:               parseDateStrict(v("executive_document_start_date")),
			StatusCourt:             nullIfEmpty(v("executive_document_status_court")),
			IssuingAuthority:        nullIfEmpty(v("executive_document_issuing_authority")),
			IssuePlace:              nullIfEmpty(v("executive_document_issue_place")),
			IssueDate:               parseDateStrict(v("executive_document_issue_date")),
			CreditorReplacement:     nullIfEmpty(v("executive_document_creditor_replacement")),
			IsCanceled:              boolLoose(v("executive_document_is_canceled")),
			CancellationNumber:      nullIfEmpty(v("executive_document_cancellation_number")),
			CancellationDateVarchar: nullIfEmpty(v("executive_document_cancellation_date")),
			LawyerReceivedAt:        parseDateStrict(v("executive_document_lawyer_received_at")),
			PrivateBailiffRecvAt:    parseDateStrict(v("executive_document_private_bailiff_received_at")),
			DVPTransferredAt:        parseDateStrict(v("executive_document_dvp_transferred_at")),
		}

		if err := p.ExecDocsRepo.Create(ctx, doc); err != nil {
			log.Printf("[PROC][exec_docs][WARN] row=%d insert failed: %v", i, err)
			logMongoFail(ctx, p.MG, importRecordID, "executive_documents", modelID, m, err.Error())
			continue
		}

		logMongo(ctx, p.MG, importRecordID, "executive_documents", modelID, m, "done", strings.Join(warnings, "; "))
	}

	log.Printf("[PROC][exec_docs][DONE] total=%d", len(batch))

	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][exec_docs][ERR] error change status: %v", err)
	}
	return nil
}

func boolLoose(s string) bool {
	v := strings.TrimSpace(strings.ToLower(s))
	if v == "" {
		return false
	}
	switch v {
	case "1", "true", "t", "yes", "y", "да", "д", "on":
		return true
	case "0", "false", "f", "no", "n", "нет", "off":
		return false
	default:
		return true
	}
}
