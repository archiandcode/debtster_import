package processors

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	mg "debtster_import/internal/config/connections/mongo"
	"debtster_import/internal/config/connections/postgres"
	"debtster_import/internal/ports"
	importitems "debtster_import/internal/repository/imports"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type ExecutiveDocumentsProcessor struct {
	PG *postgres.Postgres
	MG *mg.Mongo

	ExecutiveDocumentsTable string
	DebtsTable              string
}

func (p ExecutiveDocumentsProcessor) Type() string { return "import_executive_documents" }

func (p ExecutiveDocumentsProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
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

	docTable := firstNonEmpty(p.ExecutiveDocumentsTable, "executive_documents")
	debtsTable := firstNonEmpty(p.DebtsTable, "debts")

	log.Printf("[PROC][exec_docs][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	debtIDCache := make(map[string]*string)

	type row struct {
		id      string
		payload map[string]string

		docType string
		amount  string

		serialNumber          *string
		debtID                *string
		startDate             *time.Time
		statusCourt           *string
		issuingAuthority      *string
		issuePlace            *string
		issueDate             *time.Time
		creditorReplacement   *string
		isCanceled            bool
		cancellationNumber    *string
		cancellationDateVchar *string
		lawyerReceivedAt      *time.Time
		privateBailiffRecvAt  *time.Time
		dvpTransferredAt      *time.Time
		createdAt             *time.Time

		warnings []string
	}

	rows := make([]row, 0, len(batch))

	for i, m := range batch {
		var warnings []string

		var debtUUID *string
		if dn := strings.TrimSpace(strings.ReplaceAll(m["debt_number"], " ", "")); dn != "" {
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

		docType := strings.TrimSpace(m["executive_document_type"])
		if docType == "" {
			logMongoFail(ctx, p.MG, importRecordID, "executive_documents", uuid.NewString(), m, "missing executive_document_type")
			continue
		}

		serial := nullIfEmpty(m["executive_document_serial_number"])
		amount := normalizeAmount(m["executive_document_amount"])

		issueDate := parseDateStrict(m["executive_document_issue_date"])
		issuingAuthority := nullIfEmpty(m["executive_document_issuing_authority"])
		issuePlace := nullIfEmpty(m["executive_document_issue_place"])

		isCanceled := boolLoose(m["executive_document_is_canceled"])
		cancellationNumber := nullIfEmpty(m["executive_document_cancellation_number"])
		cancellationDateVchar := nullIfEmpty(m["executive_document_cancellation_date"])

		dvpTransferredAt := parseDateStrict(m["executive_document_dvp_transferred_at"])
		creditorReplacement := nullIfEmpty(m["executive_document_creditor_replacement"])
		startDate := parseDateStrict(m["executive_document_start_date"])
		statusCourt := nullIfEmpty(m["executive_document_status_court"])
		lawyerReceivedAt := parseDateStrict(m["executive_document_lawyer_received_at"])
		privateBailiffRecvAt := parseDateStrict(m["executive_document_private_bailiff_received_at"])

		if strings.TrimSpace(m["document_has_estate"]) != "" {
			warnings = append(warnings, "document_has_estate provided but column absent -> ignored")
		}

		rows = append(rows, row{
			id:                    uuid.NewString(),
			payload:               m,
			docType:               docType,
			amount:                amount,
			serialNumber:          serial,
			debtID:                debtUUID,
			startDate:             startDate,
			statusCourt:           statusCourt,
			issuingAuthority:      issuingAuthority,
			issuePlace:            issuePlace,
			issueDate:             issueDate,
			creditorReplacement:   creditorReplacement,
			isCanceled:            isCanceled,
			cancellationNumber:    cancellationNumber,
			cancellationDateVchar: cancellationDateVchar,
			lawyerReceivedAt:      lawyerReceivedAt,
			privateBailiffRecvAt:  privateBailiffRecvAt,
			dvpTransferredAt:      dvpTransferredAt,
			createdAt:             nowPtr(),
			warnings:              warnings,
		})

		_ = i
	}

	if len(rows) == 0 {
		log.Printf("[PROC][exec_docs][DONE] no valid rows")
		return nil
	}

	batchReq := &pgx.Batch{}
	for _, r := range rows {
		batchReq.Queue(
			`INSERT INTO `+docTable+` (
				id, type, serial_number, debt_id, amount,
				start_date, status_court, issuing_authority, issue_place, issue_date,
				creditor_replacement, is_canceled, cancellation_number, cancellation_date,
				lawyer_received_at, private_bailiff_received_at, dvp_transferred_at,
				created_at
			) VALUES (
				$1::uuid, $2, $3, $4::uuid, $5::numeric,
				$6::date, $7, $8, $9, $10::date,
				$11, $12::bool, $13, $14,
				$15::date, $16::date, $17::date,
				$18::timestamp
			)`,
			r.id, r.docType, r.serialNumber, r.debtID, r.amount,
			r.startDate, r.statusCourt, r.issuingAuthority, r.issuePlace, r.issueDate,
			r.creditorReplacement, r.isCanceled, r.cancellationNumber, r.cancellationDateVchar,
			r.lawyerReceivedAt, r.privateBailiffRecvAt, r.dvpTransferredAt,
			r.createdAt,
		)
	}

	br := p.PG.Pool.SendBatch(ctx, batchReq)
	defer br.Close()

	inserted := 0
	for i, r := range rows {
		if _, err := br.Exec(); err != nil {
			logMongo(ctx, p.MG, importRecordID, "executive_documents", r.id, r.payload, "failed", err.Error())
			log.Printf("[PROC][exec_docs][WARN] row=%d insert failed: %v", i, err)
			continue
		}
		inserted++
		errText := strings.Join(r.warnings, "; ")
		logMongo(ctx, p.MG, importRecordID, "executive_documents", r.id, r.payload, "done", errText)
	}

	log.Printf("[PROC][exec_docs][DONE] total=%d inserted=%d", len(rows), inserted)

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
