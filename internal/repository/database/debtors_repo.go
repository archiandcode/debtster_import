package database

import (
	"context"
	"debtster_import/internal/config/connections/postgres"
	"strings"
	"time"
)

type DebtorRepo struct {
	pg    *postgres.Postgres
	table string
}

func NewDebtorRepo(pg *postgres.Postgres, table string) *DebtorRepo {
	return &DebtorRepo{
		pg:    pg,
		table: table,
	}
}

type DebtorRow struct {
	ID                          string
	IIN                         string
	LastName                    string
	FirstName                   string
	MiddleName                  string
	FullName                    string
	Status                      string
	IDCardNumber                string
	IDCardAuthoritiesInGranting string
	IDCardStartDate             *time.Time
	IDCardEndDate               *time.Time
	BirthDay                    *time.Time
	Birthplace                  string
	Nationality                 string
	CreatedAt                   *time.Time
}

func (r *DebtorRepo) UpdateOrCreate(ctx context.Context, d DebtorRow) (*DebtorRow, bool, error) {
	if strings.TrimSpace(d.IIN) == "" {
		return nil, false, nil
	}

	if strings.TrimSpace(d.FullName) != "" &&
		strings.TrimSpace(d.LastName) == "" &&
		strings.TrimSpace(d.FirstName) == "" &&
		strings.TrimSpace(d.MiddleName) == "" {
		d.LastName, d.FirstName, d.MiddleName = parseFullName(d.FullName)
	}

	query := `
		INSERT INTO ` + r.table + ` (
			iin, last_name, first_name, middle_name, status,
			id_card_number, id_card_authorities_in_granting,
			id_card_start_date, id_card_end_date, birth_day,
			birthplace, nationality, created_at, updated_at, id
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10,
			$11, $12, NOW(), NOW(), gen_random_uuid()
		)
		ON CONFLICT (iin) DO UPDATE SET
			last_name = COALESCE(NULLIF(EXCLUDED.last_name, ''), ` + r.table + `.last_name),
			first_name = COALESCE(NULLIF(EXCLUDED.first_name, ''), ` + r.table + `.first_name),
			middle_name = COALESCE(NULLIF(EXCLUDED.middle_name, ''), ` + r.table + `.middle_name),
			status = COALESCE(NULLIF(EXCLUDED.status, ''), ` + r.table + `.status),
			id_card_number = COALESCE(NULLIF(EXCLUDED.id_card_number, ''), ` + r.table + `.id_card_number),
			id_card_authorities_in_granting = COALESCE(NULLIF(EXCLUDED.id_card_authorities_in_granting, ''), ` + r.table + `.id_card_authorities_in_granting),
			id_card_start_date = COALESCE(EXCLUDED.id_card_start_date, ` + r.table + `.id_card_start_date),
			id_card_end_date = COALESCE(EXCLUDED.id_card_end_date, ` + r.table + `.id_card_end_date),
			birth_day = COALESCE(EXCLUDED.birth_day, ` + r.table + `.birth_day),
			birthplace = COALESCE(NULLIF(EXCLUDED.birthplace, ''), ` + r.table + `.birthplace),
			nationality = COALESCE(NULLIF(EXCLUDED.nationality, ''), ` + r.table + `.nationality),
			updated_at = NOW()
		RETURNING 
			id, iin, last_name, first_name, middle_name, status,
			id_card_number, id_card_authorities_in_granting,
			id_card_start_date, id_card_end_date, birth_day,
			birthplace, nationality, created_at
	`

	row := r.pg.Pool.QueryRow(ctx, query,
		d.IIN, d.LastName, d.FirstName, d.MiddleName, d.Status,
		d.IDCardNumber, d.IDCardAuthoritiesInGranting,
		d.IDCardStartDate, d.IDCardEndDate, d.BirthDay,
		d.Birthplace, d.Nationality,
	)

	var debtor DebtorRow
	err := row.Scan(
		&debtor.ID, &debtor.IIN, &debtor.LastName, &debtor.FirstName, &debtor.MiddleName, &debtor.Status,
		&debtor.IDCardNumber, &debtor.IDCardAuthoritiesInGranting,
		&debtor.IDCardStartDate, &debtor.IDCardEndDate, &debtor.BirthDay,
		&debtor.Birthplace, &debtor.Nationality, &debtor.CreatedAt,
	)
	if err != nil {
		return nil, false, err
	}

	return &debtor, true, nil
}

func parseFullName(fullname string) (last, first, middle string) {
	fullname = strings.TrimSpace(fullname)
	fullname = strings.Join(strings.Fields(fullname), " ")

	parts := strings.Split(fullname, " ")
	if len(parts) >= 1 {
		last = parts[0]
	}
	if len(parts) >= 2 {
		first = parts[1]
	}
	if len(parts) >= 3 {
		middle = parts[2]
	}
	return
}
