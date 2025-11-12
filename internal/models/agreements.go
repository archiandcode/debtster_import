package models

import "time"

type Agreement struct {
	ID                   string
	AgreementTypeID      *int64
	DebtID               *string
	UserID               *int64
	AmountDebt           string
	MonthlyPaymentAmount string
	ScheduledPaymentDay  *string
	StartDate            *time.Time
	EndDate              *time.Time
	CreatedAt            *time.Time
	UpdatedAt            *time.Time
}
