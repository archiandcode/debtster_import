package models

type Address struct {
	ID       string
	DebtorID string
	Address  string
	TypeID   *int
	IIN      string
}
