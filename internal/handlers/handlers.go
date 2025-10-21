package handlers

import (
	"encoding/json"
	"log"
	"net/http"

	"debtster_import/internal/config/connections/mongo"
	"debtster_import/internal/config/connections/postgres"
	"debtster_import/internal/config/connections/s3"
	"debtster_import/internal/ports"
	"debtster_import/internal/services/importer/processors"
)

type Handlers struct {
	Postgres *postgres.Postgres
	Mongo    *mongo.Mongo
	S3       *s3.S3
	HTTP     *http.Client

	Registry map[string]ports.Processor

	Logger *log.Logger
}

func New(pg *postgres.Postgres, mg *mongo.Mongo, s3c *s3.S3) *Handlers {
	httpClient := &http.Client{}

	reg := processors.DefaultRegistry()

	reg["import_actions"] = processors.ActionsProcessor{
		PG: pg,
		MG: mg,
	}
	reg["import_agreements"] = processors.AgreementsProcessor{
		PG: pg,
		MG: mg,
	}
	reg["import_executive_documents"] = processors.ExecutiveDocumentsProcessor{
		PG: pg,
		MG: mg,
	}
	reg["import_enforcement_proceedings"] = processors.EnforcementProceedingsProcessor{
		PG: pg,
		MG: mg,
	}
	reg["add_payments"] = processors.PaymentsProcessor{
		PG: pg,
		MG: mg,
	}
	reg["import_user_plans"] = processors.UserPlansProcessor{
		PG: pg,
		MG: mg,
	}

	return &Handlers{
		Postgres: pg,
		Mongo:    mg,
		S3:       s3c,
		HTTP:     httpClient,
		Registry: reg,
		Logger:   log.Default(),
	}
}

func (h *Handlers) JSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
