package database

import (
	"context"
	"debtster_import/internal/config/connections/postgres"
	"regexp"
	"strings"
	"time"
)

type PhoneRepo struct {
	pg    *postgres.Postgres
	table string
}

func NewPhoneRepo(pg *postgres.Postgres, table string) *PhoneRepo {
	return &PhoneRepo{
		pg:    pg,
		table: table,
	}
}

type PhoneRow struct {
	ID          string
	SubjectType string
	SubjectID   string
	PhonesRaw   string
	TypeID      *int
	CreatedAt   *time.Time
	UpdatedAt   *time.Time
}

func (r *PhoneRepo) SavePhones(ctx context.Context, row PhoneRow) error {
	row.PhonesRaw = strings.TrimSpace(row.PhonesRaw)
	row.SubjectType = strings.TrimSpace(row.SubjectType)

	if row.PhonesRaw == "" || row.SubjectType == "" || row.SubjectID == "" {
		return nil
	}

	splitter := regexp.MustCompile(`[\/|,]+`)
	phones := splitter.Split(row.PhonesRaw, -1)

	for _, p := range phones {
		phone := sanitizePhone(p)
		if phone == "" {
			continue
		}

		query := `
			INSERT INTO ` + r.table + ` (
				id, subject_type, subject_id, phone, type_id, created_at, updated_at
			) VALUES (
				gen_random_uuid(), $1, $2, $3, $4,  NOW(), NOW()
			)
		`

		_, err := r.pg.Pool.Exec(ctx, query,
			row.SubjectType, row.SubjectID, phone, row.TypeID,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func sanitizePhone(input string) string {
	onlyDigits := regexp.MustCompile(`\D+`)
	return onlyDigits.ReplaceAllString(strings.TrimSpace(input), "")
}
