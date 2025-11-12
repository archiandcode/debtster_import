package models

import "time"

type EnforcementProceeding struct {
	SerialNumber         *string
	DebtID               *string
	Amount               string
	PrivateBailiffName   *string
	PrivateBailiffRegion *string
	StartDate            *time.Time
	StatusAISOIP         *string
	CreatedAt            *time.Time
}
