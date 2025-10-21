package processors

import (
	"context"
	"errors"
	"log"
	"strconv"
	"strings"
	"time"

	mg "debtster_import/internal/config/connections/mongo"
	"debtster_import/internal/config/connections/postgres"
	"debtster_import/internal/ports"
	importitems "debtster_import/internal/repository/imports"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type UserPlansProcessor struct {
	PG *postgres.Postgres
	MG *mg.Mongo

	UserPlansTable string
	UsersTable     string
}

func (p UserPlansProcessor) Type() string { return "import_user_plans" }

func (p UserPlansProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
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

	userPlansTable := firstNonEmpty(p.UserPlansTable, "user_plans")
	usersTable := firstNonEmpty(p.UsersTable, "users")

	log.Printf("[PROC][user_plans][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	userIDCache := make(map[string]*int64)

	type row struct {
		id       string
		payload  map[string]string
		userID   *int64
		amount   string
		quantity string
		endDate  *time.Time
		created  *time.Time
	}
	rows := make([]row, 0, len(batch))

	for i, m := range batch {
		username := strings.TrimSpace(m["username"])
		if username == "" {
			logMongoFail(ctx, p.MG, importRecordID, "user_plans", uuid.NewString(), m, "missing username")
			continue
		}

		uid, err := getUserBigint(ctx, p.PG, usersTable, username, userIDCache)
		if err != nil || uid == nil {
			msg := "username not found: " + username
			if err != nil {
				msg += " (" + err.Error() + ")"
			}
			logMongoFail(ctx, p.MG, importRecordID, "user_plans", uuid.NewString(), m, msg)
			continue
		}

		amount := normalizeAmount(m["user_plan_amount"])

		qty := strings.TrimSpace(m["user_plan_quantity"])
		if qty == "" {
			qty = "0"
		} else {
			qty = strings.NewReplacer(" ", "", ",", "", ".", "").Replace(qty)
			if _, err := strconv.ParseInt(qty, 10, 64); err != nil {
				logMongoFail(ctx, p.MG, importRecordID, "user_plans", uuid.NewString(), m, "bad user_plan_quantity")
				continue
			}
		}
		var endDate *time.Time
		if d := strings.TrimSpace(m["end_date"]); d != "" {
			if parsed := parseDateStrict(d); parsed != nil {
				eom := toEndOfMonth(*parsed)
				endDate = &eom
			} else {
				logMongoFail(ctx, p.MG, importRecordID, "user_plans", uuid.NewString(), m, "bad end_date")
				continue
			}
		} else {
			now := time.Now()
			eom := toEndOfMonth(now)
			endDate = &eom
		}

		rows = append(rows, row{
			id:       uuid.NewString(),
			payload:  m,
			userID:   uid,
			amount:   amount,
			quantity: qty,
			endDate:  endDate,
			created:  nowPtr(),
		})

		_ = i
	}

	if len(rows) == 0 {
		log.Printf("[PROC][user_plans][DONE] no valid rows")
		return nil
	}

	batchReq := &pgx.Batch{}
	for _, r := range rows {
		batchReq.Queue(
			`INSERT INTO `+userPlansTable+` (
				user_id, amount, quantity, end_date, created_at
			) VALUES (
				$1::bigint, $2::numeric, $3::bigint, $4::date, $5::timestamp
			)`,
			r.userID, r.amount, r.quantity, r.endDate, r.created,
		)
	}

	br := p.PG.Pool.SendBatch(ctx, batchReq)
	defer br.Close()

	inserted := 0
	for i, r := range rows {
		if _, err := br.Exec(); err != nil {
			logMongo(ctx, p.MG, importRecordID, "user_plans", r.id, r.payload, "failed", err.Error())
			log.Printf("[PROC][user_plans][WARN] row=%d insert failed: %v", i, err)
			continue
		}
		inserted++
		logMongo(ctx, p.MG, importRecordID, "user_plans", r.id, r.payload, "done", "")
	}

	log.Printf("[PROC][user_plans][DONE] total=%d inserted=%d", len(rows), inserted)

	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][user_plans][ERR] error change status: %v", err)
	}
	return nil
}

func toEndOfMonth(t time.Time) time.Time {
	firstNext := time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
	return firstNext.Add(-24 * time.Hour)
}
