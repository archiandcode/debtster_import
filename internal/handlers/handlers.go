package handlers

import (
	"debtster_import/internal/repository/database"
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

	reg := initProcessors(pg, mg)

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

func initProcessors(pg *postgres.Postgres, mg *mongo.Mongo) map[string]ports.Processor {
	reg := map[string]ports.Processor{}

	base := processors.NewBaseProcessor(pg, mg)
	debtsRepo := database.NewDebtsRepo(pg)
	usersRepo := database.NewUserRepo(pg)
	debtStatusesRepo := database.NewDebtStatusesRepo(pg)
	actionRepo := database.NewActionRepo(pg)
	payRepo := database.NewPaymentRepo(pg)
	userPlanRepo := database.NewUserPlanRepo(pg)
	execDocsRepo := database.NewExecutiveDocumentsRepo(pg)
	enfProcRepo := database.NewEnforcementProceedingsRepo(pg)
	agreementRepo := database.NewAgreementRepo(pg)

	reg["import_actions"] = &processors.ActionsProcessor{
		BaseProcessor:    base,
		DebtsRepo:        debtsRepo,
		UserRepo:         usersRepo,
		DebtStatusesRepo: debtStatusesRepo,
		ActionsRepo:      actionRepo,
	}

	reg["import_agreements"] = &processors.AgreementsProcessor{
		BaseProcessor:  base,
		AgreementsRepo: agreementRepo,
		DebtsRepo:      debtsRepo,
		UserRepo:       usersRepo,
	}
	reg["import_executive_documents"] = &processors.ExecutiveDocumentsProcessor{
		BaseProcessor: base,
		DebtsRepo:     debtsRepo,
		ExecDocsRepo:  execDocsRepo,
	}
	reg["import_enforcement_proceedings"] = &processors.EnforcementProceedingsProcessor{
		BaseProcessor: base,
		DebtsRepo:     debtsRepo,
		EnfProcRepo:   enfProcRepo,
	}
	reg["add_payments"] = &processors.PaymentsProcessor{
		BaseProcessor: base,
		DebtsRepo:     debtsRepo,
		UserRepo:      usersRepo,
		PayRepo:       payRepo,
	}
	reg["import_user_plans"] = &processors.UserPlansProcessor{
		BaseProcessor: base,
		UserPlansRepo: userPlanRepo,
		UserRepo:      usersRepo,
	}
	reg["distribution_debts"] = &processors.DistributionDebtsProcessor{
		BaseProcessor: base,
	}

	reg["update_debts"] = &processors.UpdateDebtsProcessor{
		BaseProcessor: base,
	}

	reg["import_debtors"] = &processors.DebtorsProcessor{
		BaseProcessor: base,

		DebtorsRepo:             database.NewDebtorRepo(pg),
		DebtsRepo:               debtsRepo,
		AddressesRepo:           database.NewAddressesRepo(pg),
		PhonesRepo:              database.NewPhoneRepo(pg),
		ContactPersonPhonesRepo: database.NewContactPersonPhonesRepo(pg),
	}

	return reg
}
