package processors

import (
	"context"
	"debtster_import/internal/ports"
)

type NoopProcessor struct{}

func (NoopProcessor) Type() string { return "noop" }

func (NoopProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
	return nil
}

func DefaultRegistry() map[string]ports.Processor {
	return map[string]ports.Processor{
		"noop": NoopProcessor{},
	}
}
