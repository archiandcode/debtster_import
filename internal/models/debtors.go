package models

import "time"

type Debtor struct {
	ID                          string
	IIN                         string
	LastName                    string
	FirstName                   string
	MiddleName                  string
	FullName                    string
	Status                      string
	IDCardNumber                string
	IDCardAuthoritiesInGranting string
	IDCardStartDate             *time.Time
	IDCardEndDate               *time.Time
	BirthDay                    *time.Time
	Birthplace                  string
	Nationality                 string
	CreatedAt                   *time.Time
}
