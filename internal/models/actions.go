package models

import "time"

type Action struct {
	ID           string
	DebtID       *string
	UserID       *string
	DebtStatusID *int64
	Type         *string
	Comment      *string
	CreatedAt    *time.Time
}
