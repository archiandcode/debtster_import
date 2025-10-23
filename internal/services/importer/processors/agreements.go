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

type AgreementsProcessor struct {
	PG *postgres.Postgres
	MG *mg.Mongo

	AgreementsTable     string
	AgreementTypesTable string
	DebtsTable          string
	UsersTable          string
}

func (p AgreementsProcessor) Type() string { return "import_agreements" }

func (p AgreementsProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
	log.Printf("[PROC][agreements][CHECK] Mongo client: %v, db: %v",
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

	agreementsTable := firstNonEmpty(p.AgreementsTable, "agreements")
	typesTable := firstNonEmpty(p.AgreementTypesTable, "agreement_types")
	debtsTable := firstNonEmpty(p.DebtsTable, "debts")
	usersTable := firstNonEmpty(p.UsersTable, "users")

	log.Printf("[PROC][agreements][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	debtIDCache := make(map[string]*string)
	userIDCache := make(map[string]*int64)
	typeIDCache := make(map[string]*int64)

	type row struct {
		id              string
		payload         map[string]string
		debtID          *string
		userID          *int64
		agreementTypeID *int64
		amountDebt      string
		monthlyAmount   string
		schedDay        *string
		startDate       *time.Time
		endDate         *time.Time
		createdAt       *time.Time
		warnings        []string
	}
	rows := make([]row, 0, len(batch))

	for i, m := range batch {
		debtNumber := strings.TrimSpace(m["debt_number"])
		if debtNumber == "" {
			if _, err := importitems.InsertItem(ctx, p.MG, importitems.Item{
				ImportRecordID: importRecordID,
				ModelType:      "agreements",
				ModelID:        uuid.NewString(),
				Payload:        mustJSON(m),
				Status:         "failed",
				Errors:         "missing debt_number",
			}); err != nil {
				log.Printf("[PROC][agreements][MONGO][ERR] row=%d missing debt_number: %v", i, err)
			}
			continue
		}

		debtUUID, err := getDebtUUID(ctx, p.PG, debtsTable, debtNumber, debtIDCache)
		if err != nil || debtUUID == nil {
			msg := "debt not found: " + debtNumber
			if err != nil {
				msg += " (" + err.Error() + ")"
			}
			if _, mErr := importitems.InsertItem(ctx, p.MG, importitems.Item{
				ImportRecordID: importRecordID,
				ModelType:      "agreements",
				ModelID:        uuid.NewString(),
				Payload:        mustJSON(m),
				Status:         "failed",
				Errors:         msg,
			}); mErr != nil {
				log.Printf("[PROC][agreements][MONGO][ERR] row=%d debt not found: %v", i, mErr)
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

		var agreementTypeID *int64
		if tname := strings.TrimSpace(m["agreement_type"]); tname != "" {
			if tid, err := p.getOrCreateAgreementType(ctx, typesTable, tname, typeIDCache); err == nil && tid != nil {
				agreementTypeID = tid
			} else {
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

		rows = append(rows, row{
			id:              uuid.NewString(),
			payload:         m,
			debtID:          debtUUID,
			userID:          userID,
			agreementTypeID: agreementTypeID,
			amountDebt:      amountDebt,
			monthlyAmount:   monthly,
			schedDay:        schedDay,
			startDate:       start,
			endDate:         end,
			createdAt:       nowPtr(),
			warnings:        warnings,
		})

		_ = i
	}

	if len(rows) == 0 {
		log.Printf("[PROC][agreements][DONE] no valid rows")
		return nil
	}

	batchReq := &pgx.Batch{}
	for _, r := range rows {
		batchReq.Queue(
			`INSERT INTO `+agreementsTable+` (
				agreement_type_id, debt_id, user_id, amount_debt, monthly_payment_amount,
				scheduled_payment_day, start_date, end_date, created_at
			) VALUES (
				$1::bigint, $2::uuid, $3::bigint, $4::numeric, $5::numeric,
				$6, $7::date, $8::date, $9::timestamp
			)`,
			r.agreementTypeID, r.debtID, r.userID, r.amountDebt, r.monthlyAmount,
			r.schedDay, r.startDate, r.endDate, r.createdAt,
		)
	}

	br := p.PG.Pool.SendBatch(ctx, batchReq)
	defer br.Close()

	inserted := 0
	for i, r := range rows {
		if _, err := br.Exec(); err != nil {
			log.Printf("[PROC][agreements][WARN] row=%d insert failed: %v", i, err)
			if res, mErr := importitems.InsertItem(ctx, p.MG, importitems.Item{
				ImportRecordID: importRecordID,
				ModelType:      "agreements",
				ModelID:        r.id,
				Payload:        mustJSON(r.payload),
				Status:         "failed",
				Errors:         err.Error(),
			}); mErr != nil {
				log.Printf("[PROC][agreements][MONGO][ERR] row=%d id=%s status=failed err=%v", i, r.id, mErr)
			} else {
				log.Printf("[PROC][agreements][MONGO][OK] row=%d id=%s status=failed inserted_id=%v", i, r.id, res.InsertedID)
			}
			continue
		}

		inserted++

		errText := strings.Join(r.warnings, "; ")
		if res, mErr := importitems.InsertItem(ctx, p.MG, importitems.Item{
			ImportRecordID: importRecordID,
			ModelType:      "agreements",
			ModelID:        r.id,
			Payload:        mustJSON(r.payload),
			Status:         "done",
			Errors:         errText,
		}); mErr != nil {
			log.Printf("[PROC][agreements][MONGO][ERR] row=%d id=%s status=done err=%v", i, r.id, mErr)
		} else {
			log.Printf("[PROC][agreements][MONGO][OK] row=%d id=%s status=done inserted_id=%v", i, r.id, res.InsertedID)
		}
	}

	log.Printf("[PROC][agreements][DONE] total=%d inserted=%d", len(rows), inserted)

	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][agreements][ERR] error change status: %v", err)
	}
	return nil
}

func (p AgreementsProcessor) getOrCreateAgreementType(ctx context.Context, table, name string, cache map[string]*int64) (*int64, error) {
	key := strings.ToLower(strings.TrimSpace(name))
	if v, ok := cache[key]; ok {
		return v, nil
	}

	var id int64
	err := p.PG.Pool.QueryRow(ctx, `SELECT id FROM `+table+` WHERE LOWER(name)=LOWER($1) LIMIT 1`, name).Scan(&id)
	if err == nil {
		cache[key] = &id
		return &id, nil
	}

	if err = p.PG.Pool.QueryRow(ctx, `INSERT INTO `+table+` (name, created_at) VALUES ($1, NOW()) RETURNING id`, name).Scan(&id); err == nil {
		cache[key] = &id
		return &id, nil
	}

	if err2 := p.PG.Pool.QueryRow(ctx, `SELECT id FROM `+table+` WHERE LOWER(name)=LOWER($1) LIMIT 1`, name).Scan(&id); err2 == nil {
		cache[key] = &id
		return &id, nil
	}

	return nil, err
}
