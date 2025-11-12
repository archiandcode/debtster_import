package models

import "time"

type ExecutiveDocument struct {
	ID                      string
	DocType                 string
	SerialNumber            *string
	DebtID                  *string
	Amount                  string
	StartDate               *time.Time
	StatusCourt             *string
	IssuingAuthority        *string
	IssuePlace              *string
	IssueDate               *time.Time
	CreditorReplacement     *string
	IsCanceled              bool
	CancellationNumber      *string
	CancellationDateVarchar *string
	LawyerReceivedAt        *time.Time
	PrivateBailiffRecvAt    *time.Time
	DVPTransferredAt        *time.Time
	CreatedAt               *time.Time
}
