package database

import (
	"context"

	"debtster_import/internal/config/connections/postgres"
	"debtster_import/internal/models"
)

type ExecutiveDocumentsRepo struct {
	PG *postgres.Postgres
}

func NewExecutiveDocumentsRepo(pg *postgres.Postgres) *ExecutiveDocumentsRepo {
	return &ExecutiveDocumentsRepo{PG: pg}
}

func (r *ExecutiveDocumentsRepo) Create(ctx context.Context, row models.ExecutiveDocument) error {
	const q = `
		INSERT INTO executive_documents (
		  id, type, serial_number, debt_id, amount,
		  start_date, status_court, issuing_authority, issue_place, issue_date,
		  creditor_replacement, is_canceled, cancellation_number, cancellation_date,
		  lawyer_received_at, private_bailiff_received_at, dvp_transferred_at,
		  created_at
		) VALUES (
		  $1::uuid, $2, $3, $4::uuid, $5::numeric,
		  $6::date, $7, $8, $9, $10::date,
		  $11, $12::bool, $13, $14,
		  $15::date, $16::date, $17::date,
		  NOW()
		)`
	_, err := r.PG.Pool.Exec(ctx, q,
		row.ID, row.DocType, row.SerialNumber, row.DebtID, row.Amount,
		row.StartDate, row.StatusCourt, row.IssuingAuthority, row.IssuePlace, row.IssueDate,
		row.CreditorReplacement, row.IsCanceled, row.CancellationNumber, row.CancellationDateVarchar,
		row.LawyerReceivedAt, row.PrivateBailiffRecvAt, row.DVPTransferredAt,
	)
	return err
}
