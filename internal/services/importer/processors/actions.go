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

type ActionsProcessor struct {
	PG *postgres.Postgres
	MG *mg.Mongo

	ActionsTable      string
	DebtsTable        string
	UsersTable        string
	DebtStatusesTable string
}

func (p ActionsProcessor) Type() string { return "import_actions" }

func (p ActionsProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
	log.Printf("[PROC][actions][CHECK] Mongo client: %v, db: %v",
		p.MG != nil && p.MG.Client != nil, p.MG != nil && p.MG.Database != nil)

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

	actionsTable := firstNonEmpty(p.ActionsTable, "actions")
	debtsTable := firstNonEmpty(p.DebtsTable, "debts")
	usersTable := firstNonEmpty(p.UsersTable, "users")
	statusTable := firstNonEmpty(p.DebtStatusesTable, "debt_statuses")

	log.Printf("[PROC][actions][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	debtIDCache := make(map[string]*string)
	userIDCache := make(map[string]*int64)
	statusIDCache := make(map[string]*int64)

	type row struct {
		id        string
		debtID    *string
		userID    *int64
		statusID  *int64
		typ       *string
		comment   *string
		createdAt *time.Time
		data      map[string]string
		warnings  []string
	}

	rows := make([]row, 0, len(batch))

	for i, m := range batch {
		debtNumber := strings.TrimSpace(m["debt_number"])
		if debtNumber == "" {
			if _, err := importitems.InsertItem(ctx, p.MG, importitems.Item{
				ImportRecordID: importRecordID,
				ModelType:      "actions",
				ModelID:        uuid.NewString(),
				Payload:        mustJSON(m),
				Status:         "failed",
				Errors:         "missing debt_number",
			}); err != nil {
				log.Printf("[PROC][actions][MONGO][ERR] row=%d missing debt_number: %v", i, err)
			}
			continue
		}

		debtID, err := getDebtUUID(ctx, p.PG, debtsTable, debtNumber, debtIDCache)
		if err != nil || debtID == nil {
			msg := "debt not found: " + debtNumber
			if err != nil {
				msg += " (" + err.Error() + ")"
			}
			if _, mErr := importitems.InsertItem(ctx, p.MG, importitems.Item{
				ImportRecordID: importRecordID,
				ModelType:      "actions",
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
			if uid, err := getUserBigint(ctx, p.PG, usersTable, un, userIDCache); err == nil && uid != nil {
				userID = uid
			} else {
				warnings = append(warnings, "username not found: "+un+" -> user_id=NULL")
			}
		}

		// status (shortname): допускаем NULL, но предупреждение
		var statusID *int64
		if st := strings.TrimSpace(m["status"]); st == "" {
			warnings = append(warnings, "missing status -> debt_status_id=NULL")
		} else {
			if sid, err := getStatusBigint(ctx, p.PG, statusTable, st, statusIDCache); err == nil && sid != nil {
				statusID = sid
			} else {
				warnings = append(warnings, "status not found: "+st+" -> debt_status_id=NULL")
			}
		}

		r := row{
			id:        uuid.NewString(),
			debtID:    debtID,
			userID:    userID,
			statusID:  statusID,
			typ:       nullIfEmpty(strings.TrimSpace(m["type"])),
			comment:   nullIfEmpty(strings.TrimSpace(m["comment"])),
			createdAt: parseTimeLoose(m["created_at"]),
			data:      m,
			warnings:  warnings,
		}
		rows = append(rows, r)
	}

	if len(rows) == 0 {
		log.Printf("[PROC][actions][DONE] no valid rows")
		return nil
	}

	batchReq := &pgx.Batch{}
	for _, r := range rows {
		batchReq.Queue(
			`INSERT INTO `+actionsTable+` (id, debt_id, user_id, debt_status_id, type, comment, created_at)
			 VALUES ($1::uuid, $2::uuid, $3::bigint, $4::bigint, $5, $6, $7)`,
			r.id, r.debtID, r.userID, r.statusID, r.typ, r.comment, r.createdAt,
		)
	}

	br := p.PG.Pool.SendBatch(ctx, batchReq)
	defer br.Close()

	inserted := 0
	for i, r := range rows {
		if _, err := br.Exec(); err != nil {
			log.Printf("[PROC][actions][WARN] row=%d insert failed: %v", i, err)
			if res, mErr := importitems.InsertItem(ctx, p.MG, importitems.Item{
				ImportRecordID: importRecordID,
				ModelType:      "actions",
				ModelID:        r.id,
				Payload:        mustJSON(r.data),
				Status:         "failed",
				Errors:         err.Error(),
			}); mErr != nil {
				log.Printf("[PROC][actions][MONGO][ERR] row=%d id=%s status=failed err=%v", i, r.id, mErr)
			} else {
				log.Printf("[PROC][actions][MONGO][OK] row=%d id=%s status=failed inserted_id=%v", i, r.id, res.InsertedID)
			}
			continue
		}

		inserted++

		errText := strings.Join(r.warnings, "; ")
		if res, mErr := importitems.InsertItem(ctx, p.MG, importitems.Item{
			ImportRecordID: importRecordID,
			ModelType:      "actions",
			ModelID:        r.id,
			Payload:        mustJSON(r.data),
			Status:         "done",
			Errors:         errText,
		}); mErr != nil {
			log.Printf("[PROC][actions][MONGO][ERR] row=%d id=%s status=done err=%v", i, r.id, mErr)
		} else {
			log.Printf("[PROC][actions][MONGO][OK] row=%d id=%s status=done inserted_id=%v", i, r.id, res.InsertedID)
		}
	}

	log.Printf("[PROC][actions][DONE] total=%d inserted=%d", len(rows), inserted)

	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][actions][ERR] error change status: %v", err)
	}
	return nil
}
