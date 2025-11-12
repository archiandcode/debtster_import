package processors

import (
	"context"
	"log"
	"regexp"
	"strings"
	"time"

	"debtster_import/internal/ports"
	importitems "debtster_import/internal/repository/imports"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
)

type UsersProcessor struct {
	*BaseProcessor

	UsersTable string
}

func (p UsersProcessor) Type() string { return "import_users" }

func (p UsersProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
	if err := CheckDeps(p); err != nil {
		return err
	}

	var importRecordID string
	if v := ctx.Value(ports.CtxImportRecordID); v != nil {
		if s, ok := v.(string); ok {
			importRecordID = strings.TrimSpace(s)
		}
	}

	usersTable := "users"
	log.Printf("[PROC][users][START] rows=%d import_record_id=%s", len(batch), importRecordID)

	type row struct {
		payload map[string]string

		firstName  string
		lastName   *string
		middleName *string
		username   string
		email      *string
		phone      *string
		password   string
		createdAt  *time.Time

		role       *string
		department *string

		warnings []string
	}
	rows := make([]row, 0, len(batch))

	for _, m := range batch {
		warnings := make([]string, 0, 2)

		firstName := strings.TrimSpace(m["first_name"])
		username := strings.TrimSpace(m["username"])
		password := strings.TrimSpace(m["password"])

		if firstName == "" {
			logMongoFail(ctx, p.MG, importRecordID, "users", uuid.NewString(), m, "missing first_name")
			continue
		}
		if username == "" {
			logMongoFail(ctx, p.MG, importRecordID, "users", uuid.NewString(), m, "missing username")
			continue
		}
		if password == "" {
			logMongoFail(ctx, p.MG, importRecordID, "users", uuid.NewString(), m, "missing password")
			continue
		}

		lastName := nullIfEmpty(m["last_name"])
		middleName := nullIfEmpty(m["middle_name"])

		var email *string
		if v := strings.TrimSpace(m["email"]); v != "" {
			ev := strings.ToLower(v)
			email = &ev
		}

		phone := nullIfEmpty(m["phone"])

		role := nullIfEmpty(m["role"])
		dept := nullIfEmpty(m["department"])
		if role != nil && *role != "" {
			warnings = append(warnings, "role provided but not applied (no role-binding logic)")
		}
		if dept != nil && *dept != "" {
			warnings = append(warnings, "department provided but not applied (no department-binding logic)")
		}

		pwHashed, hashed := ensureBcrypt(password)
		if !hashed {
			warnings = append(warnings, "password was plaintext -> bcrypt applied")
		}

		rows = append(rows, row{
			payload:    m,
			firstName:  firstName,
			lastName:   lastName,
			middleName: middleName,
			username:   username,
			email:      email,
			phone:      phone,
			password:   pwHashed,
			createdAt:  nowPtr(),
			role:       role,
			department: dept,
			warnings:   warnings,
		})
	}

	if len(rows) == 0 {
		log.Printf("[PROC][users][DONE] no valid rows")
		return nil
	}

	batchReq := &pgx.Batch{}
	for _, r := range rows {
		batchReq.Queue(
			`INSERT INTO `+usersTable+` (
				first_name, last_name, middle_name, username, email, phone, password, created_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8::timestamp
			)`,
			r.firstName, r.lastName, r.middleName, r.username, r.email, r.phone, r.password, r.createdAt,
		)
	}

	br := p.PG.Pool.SendBatch(ctx, batchReq)
	defer br.Close()

	inserted := 0
	for i, r := range rows {
		if _, err := br.Exec(); err != nil {
			logMongo(ctx, p.MG, importRecordID, "users", uuid.NewString(), r.payload, "failed", err.Error())
			log.Printf("[PROC][users][WARN] row=%d insert failed: %v", i, err)
			continue
		}
		inserted++
		errText := strings.Join(r.warnings, "; ")
		logMongo(ctx, p.MG, importRecordID, "users", uuid.NewString(), r.payload, "done", errText)
	}

	log.Printf("[PROC][users][DONE] total=%d inserted=%d", len(rows), inserted)
	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][users][ERR] error change status: %v", err)
	}
	return nil
}

var bcryptPrefix = regexp.MustCompile(`^\$(2a|2b|2y)\$`)

func ensureBcrypt(pw string) (string, bool) {
	if bcryptPrefix.MatchString(pw) && len(pw) >= 60 {
		return pw, true
	}
	h, err := bcrypt.GenerateFromPassword([]byte(pw), 12)
	if err != nil {
		return pw, false
	}
	return string(h), false
}
