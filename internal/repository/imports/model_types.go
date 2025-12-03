package importitems

type ModelType string

const (
	ModelTypeDebtors        ModelType = "debtors"
	ModelTypeDebts          ModelType = "debts"
	ModelTypePhones         ModelType = "phones"
	ModelTypeAddresses      ModelType = "addresses"
	ModelTypeUsers          ModelType = "users"
	ModelTypeAgreements     ModelType = "agreements"
	ModelTypeContactPersons ModelType = "contact_persons"
	ModelTypePayments       ModelType = "payments"
	ModelTypeActions        ModelType = "actions"
	ModelTypeUserPlans      ModelType = "user_plans"
	ModelTypeEnforcements   ModelType = "enforcement_proceedings"
	ModelTypeExecDocs       ModelType = "executive_documents"
)

var PHPModelMap = map[ModelType]string{
	ModelTypeDebtors:        "App\\Infrastructure\\Persistence\\Models\\Debtor",
	ModelTypeDebts:          "App\\Infrastructure\\Persistence\\Models\\Debt",
	ModelTypePhones:         "App\\Infrastructure\\Persistence\\Models\\Phone",
	ModelTypeAddresses:      "App\\Infrastructure\\Persistence\\Models\\Address",
	ModelTypeUsers:          "App\\Infrastructure\\Persistence\\Models\\User",
	ModelTypeAgreements:     "App\\Infrastructure\\Persistence\\Models\\Agreement",
	ModelTypeContactPersons: "App\\Infrastructure\\Persistence\\Models\\ContactPerson",
	ModelTypePayments:       "App\\Infrastructure\\Persistence\\Models\\Payment",
	ModelTypeActions:        "App\\Infrastructure\\Persistence\\Models\\Action",
	ModelTypeUserPlans:      "App\\Infrastructure\\Persistence\\Models\\UserPlan",
	ModelTypeEnforcements:   "App\\Infrastructure\\Persistence\\Models\\EnforcementProceeding",
	ModelTypeExecDocs:       "App\\Infrastructure\\Persistence\\Models\\ExecutiveDocument",
}

func PHPModelByTable(table string) string {
	mt := ModelType(table)
	if php, ok := PHPModelMap[mt]; ok {
		return php
	}

	return PHPModelMap[ModelTypeDebtors]
}
