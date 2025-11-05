package database

import (
	"context"
	"debtster_import/internal/config/connections/postgres"
	"fmt"
	"strings"
	"time"
)

type AddressesRepo struct {
	pg    *postgres.Postgres
	table string
}

func NewAddressesRepo(pg *postgres.Postgres, table string) *AddressesRepo {
	return &AddressesRepo{
		pg:    pg,
		table: table,
	}
}

type AddressRow struct {
	ID          string     // UUID
	DebtorID    string     // id должника
	Address     string     // строка адреса
	TypeID      *int       // тип адреса (1=рег, 2=факт, 3=рабочий)
	IIN         string     // ИИН должника (для логов/связи)
	SubjectType string     // всегда 'App\\Infrastructure\\Persistence\\Models\\Debtor'
	CreatedAt   *time.Time // опционально
	UpdatedAt   *time.Time // опционально
}

// SaveAddress выполняет ручную проверку существования записи и обновляет или создаёт новую
func (r *AddressesRepo) SaveAddress(ctx context.Context, row AddressRow) error {
	if strings.TrimSpace(row.IIN) == "" {
		return fmt.Errorf("iin is required")
	}
	if strings.TrimSpace(row.Address) == "" {
		return nil // пропускаем пустые значения
	}
	if strings.TrimSpace(row.DebtorID) == "" {
		return fmt.Errorf("debtor_id is required")
	}

	typeID := 0
	if row.TypeID != nil {
		typeID = *row.TypeID
	}

	// 🔍 Проверяем, есть ли уже адрес с таким subject_id и type_id
	var exists bool
	checkQuery := fmt.Sprintf(
		`SELECT EXISTS(SELECT 1 FROM %s WHERE subject_id = $1 AND type_id = $2)`,
		r.table,
	)
	err := r.pg.Pool.QueryRow(ctx, checkQuery, row.DebtorID, typeID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check exists error: %w", err)
	}

	if exists {
		// 🟡 обновляем существующий адрес
		updateQuery := fmt.Sprintf(`
			UPDATE %s
			SET address = $1, updated_at = NOW()
			WHERE subject_id = $2 AND type_id = $3
		`, r.table)
		_, err = r.pg.Pool.Exec(ctx, updateQuery, row.Address, row.DebtorID, typeID)
		if err != nil {
			return fmt.Errorf("update address error: %w", err)
		}
	} else {
		// 🟢 создаём новый адрес
		insertQuery := fmt.Sprintf(`
			INSERT INTO %s (
				id, subject_type, subject_id, address, type_id, created_at, updated_at
			) VALUES (
				gen_random_uuid(),
				'App\\Infrastructure\\Persistence\\Models\\Debtor',
				$1, $2, $3, NOW(), NOW()
			)
		`, r.table)
		_, err = r.pg.Pool.Exec(ctx, insertQuery, row.DebtorID, row.Address, typeID)
		if err != nil {
			return fmt.Errorf("insert address error: %w", err)
		}
	}

	return nil
}

func (r *AddressesRepo) SaveBatchAddresses(ctx context.Context, rows []AddressRow) error {
	for _, row := range rows {
		if err := r.SaveAddress(ctx, row); err != nil {
			return err
		}
	}
	return nil
}
