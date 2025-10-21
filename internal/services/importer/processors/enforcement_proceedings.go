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

type EnforcementProceedingsProcessor struct {
	PG *postgres.Postgres
	MG *mg.Mongo

	EnforcementProceedingsTable string
	DebtsTable                  string
}

func (p EnforcementProceedingsProcessor) Type() string { return "import_enforcement_proceedings" }

func (p EnforcementProceedingsProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
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

	epTable := firstNonEmpty(p.EnforcementProceedingsTable, "enforcement_proceedings")
	debtsTable := firstNonEmpty(p.DebtsTable, "debts")

	log.Printf("[PROC][enf_proc][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	debtIDCache := make(map[string]*string)

	type row struct {
		payload map[string]string

		debtID               *string
		serialNumber         *string
		amount               string
		privateBailiffName   *string
		privateBailiffRegion *string
		startDate            *time.Time
		statusAISOIP         *string
		createdAt            *time.Time

		warnings []string
	}
	rows := make([]row, 0, len(batch))

	for i, m := range batch {
		debtNumber := strings.TrimSpace(strings.ReplaceAll(m["debt_number"], " ", ""))
		if debtNumber == "" {
			logMongoFail(ctx, p.MG, importRecordID, "enforcement_proceedings", uuid.NewString(), m, "missing debt_number")
			continue
		}

		debtUUID, err := getDebtUUID(ctx, p.PG, debtsTable, debtNumber, debtIDCache)
		if err != nil || debtUUID == nil {
			msg := "debt not found: " + debtNumber
			if err != nil {
				msg += " (" + err.Error() + ")"
			}
			logMongoFail(ctx, p.MG, importRecordID, "enforcement_proceedings", uuid.NewString(), m, msg)
			continue
		}

		serial := nullIfEmpty(m["enforcement_proceeding_serial_number"])
		pbName := nullIfEmpty(m["enforcement_proceeding_private_bailiff_name"])
		pbRegion := nullIfEmpty(m["enforcement_proceeding_private_bailiff_region"])
		statusAISOIP := nullIfEmpty(m["enforcement_proceeding_status_ais_oip"])
		startDate := parseDateStrict(m["enforcement_proceeding_start_date"])
		amount := normalizeAmount(m["enforcement_proceeding_amount"]) // "" -> "0"

		r := row{
			payload:              m,
			debtID:               debtUUID,
			serialNumber:         serial,
			privateBailiffName:   pbName,
			privateBailiffRegion: pbRegion,
			statusAISOIP:         statusAISOIP,
			startDate:            startDate,
			amount:               amount,
			createdAt:            nowPtr(),
			warnings:             nil,
		}

		_ = i
		rows = append(rows, r)
	}

	if len(rows) == 0 {
		log.Printf("[PROC][enf_proc][DONE] no valid rows")
		return nil
	}

	batchReq := &pgx.Batch{}
	for _, r := range rows {
		batchReq.Queue(
			`INSERT INTO `+epTable+` (
				serial_number, debt_id, amount, private_bailiff_name, private_bailiff_region,
				start_date, status_ais_oip, created_at
			) VALUES (
				$1, $2::uuid, $3::numeric, $4, $5,
				$6::date, $7, $8::timestamp
			)`,
			r.serialNumber, r.debtID, r.amount, r.privateBailiffName, r.privateBailiffRegion,
			r.startDate, r.statusAISOIP, r.createdAt,
		)
	}

	br := p.PG.Pool.SendBatch(ctx, batchReq)
	defer br.Close()

	inserted := 0
	for i, r := range rows {
		if _, err := br.Exec(); err != nil {
			logMongo(ctx, p.MG, importRecordID, "enforcement_proceedings", uuid.NewString(), r.payload, "failed", err.Error())
			log.Printf("[PROC][enf_proc][WARN] row=%d insert failed: %v", i, err)
			continue
		}
		inserted++
		errText := strings.Join(r.warnings, "; ")
		logMongo(ctx, p.MG, importRecordID, "enforcement_proceedings", uuid.NewString(), r.payload, "done", errText)
	}

	log.Printf("[PROC][enf_proc][DONE] total=%d inserted=%d", len(rows), inserted)

	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][enf_proc][ERR] error change status: %v", err)
	}
	return nil
}
