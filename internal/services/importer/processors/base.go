package processors

import (
	"errors"

	mg "debtster_import/internal/config/connections/mongo"
	"debtster_import/internal/config/connections/postgres"
)

type BaseProcessor struct {
	PG *postgres.Postgres
	MG *mg.Mongo
}

func NewBaseProcessor(pg *postgres.Postgres, m *mg.Mongo) *BaseProcessor {
	return &BaseProcessor{PG: pg, MG: m}
}

type DepProvider interface {
	GetPG() *postgres.Postgres
	GetMG() *mg.Mongo
}

func (b *BaseProcessor) GetPG() *postgres.Postgres { return b.PG }
func (b *BaseProcessor) GetMG() *mg.Mongo          { return b.MG }

func CheckDeps[T DepProvider](p T) error {
	pg := p.GetPG()
	if pg == nil || pg.Pool == nil {
		return errors.New("postgres not available")
	}
	m := p.GetMG()
	if m == nil || m.Database == nil {
		return errors.New("mongo not available")
	}
	return nil
}
