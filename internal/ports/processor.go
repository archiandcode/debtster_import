package ports

import "context"

type ctxKey string

const CtxImportRecordID ctxKey = "import_record_id"

type Processor interface {
	Type() string
	ProcessBatch(ctx context.Context, batch []map[string]string) error
}
