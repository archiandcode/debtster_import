package models

import "time"

type Debt struct {
	ID                          string
	DebtorID                    *string
	Number                      string
	StartDate                   *time.Time
	EndDate                     *time.Time
	Filial                      string
	ProductName                 string
	Currency                    string
	AmountActualDebt            *float64
	AmountAccountsReceivable    *float64
	AmountCredit                *float64
	AmountMainDebt              *float64
	AmountFine                  *float64
	AmountAccrual               *float64
	AmountGovernmentDuty        *float64
	AmountRepresentationExpense *float64
	AmountNotaryFees            *float64
	AmountPostage               *float64
	AdditionalData              string
	UserID                      *int64
	CounterpartyID              *int64
	StatusID                    *int64
	CreatedAt                   *time.Time
}
