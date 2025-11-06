package database

import (
	"context"
	"debtster_import/internal/config/connections/postgres"
	"fmt"
	"strings"
)

type ContactPersonPhonesRepo struct {
	pg            *postgres.Postgres
	phonesTable   string
	contactsTable string
}

func NewContactPersonPhonesRepo(pg *postgres.Postgres, phonesTable, contactsTable string) *ContactPersonPhonesRepo {
	return &ContactPersonPhonesRepo{
		pg:            pg,
		phonesTable:   phonesTable,
		contactsTable: contactsTable,
	}
}

type ContactPersonPhoneRow struct {
	DebtorID string
	Value    string
}

func (r *ContactPersonPhonesRepo) SaveContactPersonPhones(ctx context.Context, row ContactPersonPhoneRow) error {
	if strings.TrimSpace(row.Value) == "" || row.DebtorID == "" {
		return nil
	}

	entries := strings.Split(row.Value, "|")

	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		parts := strings.Split(entry, ",")
		if len(parts) == 0 {
			continue
		}

		phone := sanitizePhone(parts[0])
		if phone == "" {
			continue
		}

		fullname := ""
		if len(parts) >= 2 {
			fullname = strings.TrimSpace(parts[1])
		}

		typeID := 1
		if len(parts) >= 3 {
			if t := strings.TrimSpace(parts[2]); t != "" {
				fmt.Sscanf(t, "%d", &typeID)
			}
		}

		contactPersonID, err := r.upsertContactPerson(ctx, row.DebtorID, fullname, typeID)
		if err != nil {
			return fmt.Errorf("upsert contact person failed: %w", err)
		}

		err = r.insertPhone(ctx,
			`App\Infrastructure\Persistence\Models\ContactPerson`,
			contactPersonID,
			phone,
			typeID,
		)
		if err != nil {
			return fmt.Errorf("insert phone failed: %w", err)
		}
	}

	return nil
}

func (r *ContactPersonPhonesRepo) upsertContactPerson(ctx context.Context, debtorID, fullName string, typeID int) (string, error) {
	var id string

	err := r.pg.Pool.QueryRow(ctx, `
		SELECT id FROM `+r.contactsTable+`
		WHERE debtor_id = $1 AND full_name = $2
	`, debtorID, fullName).Scan(&id)

	if err == nil {
		_, err = r.pg.Pool.Exec(ctx, `
			UPDATE `+r.contactsTable+`
			SET type_id = $1, updated_at = NOW()
			WHERE id = $2
		`, typeID, id)
		return id, err
	}

	err = r.pg.Pool.QueryRow(ctx, `
		INSERT INTO `+r.contactsTable+` (id, debtor_id, full_name, type_id, created_at, updated_at)
		VALUES (gen_random_uuid(), $1, $2, $3, NOW(), NOW())
		RETURNING id
	`, debtorID, fullName, typeID).Scan(&id)

	return id, err
}

func (r *ContactPersonPhonesRepo) insertPhone(ctx context.Context, subjectType, subjectID, phone string, typeID int) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (id, subject_type, subject_id, phone, type_id, created_at, updated_at)
		VALUES (gen_random_uuid(), $1, $2, $3, $4, NOW(), NOW())
	`, r.phonesTable)

	_, err := r.pg.Pool.Exec(ctx, query, subjectType, subjectID, phone, typeID)
	return err
}
