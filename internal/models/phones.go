package models

import "time"

type Phone struct {
	ID          string
	SubjectType string
	SubjectID   string
	PhonesRaw   string
	TypeID      *int
	CreatedAt   *time.Time
}
