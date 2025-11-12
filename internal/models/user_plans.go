package models

import "time"

type UserPlan struct {
	ID       *int64
	UserID   *int64
	Amount   string
	Quantity string
	EndDate  *time.Time
}
