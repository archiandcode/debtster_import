package database

import (
	"context"
	"debtster_import/internal/config/connections/postgres"
	"debtster_import/internal/models"
	"fmt"
	"strings"
)

type AddressesRepo struct {
	pg *postgres.Postgres
}

func NewAddressesRepo(pg *postgres.Postgres) *AddressesRepo {
	return &AddressesRepo{
		pg: pg,
	}
}

func (r *AddressesRepo) SaveAddress(ctx context.Context, row models.Address) error {
	if strings.TrimSpace(row.IIN) == "" {
		return fmt.Errorf("iin is required")
	}
	if strings.TrimSpace(row.Address) == "" {
		return nil
	}
	if strings.TrimSpace(row.DebtorID) == "" {
		return fmt.Errorf("debtor_id is required")
	}

	typeID := 0
	if row.TypeID != nil {
		typeID = *row.TypeID
	}

	table := "addresses"
	var exists bool
	checkQuery := fmt.Sprintf(
		`SELECT EXISTS(SELECT 1 FROM %s WHERE subject_id = $1 AND type_id = $2)`,
		table,
	)
	err := r.pg.Pool.QueryRow(ctx, checkQuery, row.DebtorID, typeID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check exists error: %w", err)
	}

	if exists {
		updateQuery := fmt.Sprintf(`
			UPDATE %s
			SET address = $1, updated_at = NOW()
			WHERE subject_id = $2 AND type_id = $3
		`, table)
		_, err = r.pg.Pool.Exec(ctx, updateQuery, row.Address, row.DebtorID, typeID)
		if err != nil {
			return fmt.Errorf("update address error: %w", err)
		}
	} else {
		insertQuery := fmt.Sprintf(`
			INSERT INTO %s (
				id, subject_type, subject_id, address, type_id, created_at, updated_at
			) VALUES (
				gen_random_uuid(),
				'App\Infrastructure\Persistence\Models\Debtor',
				$1, $2, $3, NOW(), NOW()
			)
		`, table)
		_, err = r.pg.Pool.Exec(ctx, insertQuery, row.DebtorID, row.Address, typeID)
		if err != nil {
			return fmt.Errorf("insert address error: %w", err)
		}
	}

	return nil
}

func (r *AddressesRepo) SaveBatchAddresses(ctx context.Context, rows []models.Address) error {
	for _, row := range rows {
		if err := r.SaveAddress(ctx, row); err != nil {
			return err
		}
	}
	return nil
}
