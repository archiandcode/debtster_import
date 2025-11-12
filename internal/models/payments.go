package models

import "time"

type Payment struct {
	ID                           string
	DebtID                       string
	UserID                       string
	Amount                       string
	AmountAfterSubtraction       string
	AmountGovernmentDuty         string
	AmountRepresentationExpenses string
	AmountNotaryFees             string
	AmountPostage                string
	AmountAccountsReceivable     string
	AmountMainDebt               string
	AmountAccrual                string
	AmountFine                   string
	PaymentDate                  *time.Time
	Confirmed                    bool
}
