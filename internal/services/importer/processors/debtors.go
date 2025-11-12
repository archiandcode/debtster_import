package processors

import (
	"context"
	"debtster_import/internal/models"
	"debtster_import/internal/ports"
	"log"
	"strconv"
	"strings"
	"time"

	"debtster_import/internal/repository/database"
	importitems "debtster_import/internal/repository/imports"

	"github.com/google/uuid"
)

type DebtorsProcessor struct {
	*BaseProcessor

	DebtorsRepo             *database.DebtorRepo
	DebtsRepo               *database.DebtsRepo
	AddressesRepo           *database.AddressesRepo
	PhonesRepo              *database.PhoneRepo
	ContactPersonPhonesRepo *database.ContactPersonPhonesRepo
}

func (p DebtorsProcessor) Type() string { return "import_debtors" }

func (p *DebtorsProcessor) ProcessBatch(ctx context.Context, batch []map[string]string) error {
	if err := CheckDeps(p); err != nil {
		return err
	}

	log.Printf("[PROC][debtors][START] rows=%d", len(batch))

	success := 0
	failed := 0
	var importRecordID string
	if v := ctx.Value(ports.CtxImportRecordID); v != nil {
		if s, ok := v.(string); ok {
			importRecordID = strings.TrimSpace(s)
		}
	}

	for i, m := range batch {
		iin := strings.TrimSpace(m["iin"])
		if iin == "" {
			failed++
			logMongoFail(ctx, p.MG, importRecordID, p.Type(), uuid.NewString(), m, "missing iin")
			continue
		}

		row := models.Debtor{
			IIN:                         iin,
			FullName:                    strings.TrimSpace(m["full_name"]),
			Status:                      strings.TrimSpace(m["status"]),
			IDCardNumber:                strings.TrimSpace(m["id_card_number"]),
			IDCardAuthoritiesInGranting: strings.TrimSpace(m["id_card_authorities_in_granting"]),
			IDCardStartDate:             parseDate(m["id_card_start_date"]),
			IDCardEndDate:               parseDate(m["id_card_end_date"]),
			BirthDay:                    parseDate(m["birth_day"]),
			Birthplace:                  strings.TrimSpace(m["birthplace"]),
			Nationality:                 strings.TrimSpace(m["nationality"]),
			CreatedAt:                   nowPtr(),
		}

		debtor, err := p.DebtorsRepo.UpdateOrCreate(ctx, row)
		if err != nil {
			failed++
			log.Printf("[PROC][debtors][ERR] row=%d iin=%s err=%v", i, iin, err)
			logMongoFail(ctx, p.MG, importRecordID, p.Type(), uuid.NewString(), m, err.Error())
			continue
		}

		if p.DebtsRepo != nil {
			debtNumber := strings.TrimSpace(m["debt_number"])
			if debtNumber != "" {
				debtRow := models.Debt{
					ID:               uuid.NewString(),
					DebtorID:         &debtor.ID,
					Number:           debtNumber,
					StartDate:        parseDate(m["start_date"]),
					EndDate:          parseDate(m["end_date"]),
					Filial:           strings.TrimSpace(m["filial"]),
					ProductName:      strings.TrimSpace(m["product_name"]),
					Currency:         strings.TrimSpace(m["currency"]),
					AmountActualDebt: parseFloatPtr(m["amount_actual_debt"]),
					AmountCredit:     parseFloatPtr(m["amount_credit"]),
					AmountMainDebt:   parseFloatPtr(m["amount_main_debt"]),
					AmountFine:       parseFloatPtr(m["amount_fine"]),
					AdditionalData:   strings.TrimSpace(m["additional_data"]),
					CreatedAt:        nowPtr(),
				}

				if err := p.DebtsRepo.UpdateOrCreate(ctx, debtRow); err != nil {
					log.Printf("[PROC][debts][ERR] iin=%s debt_number=%s: %v", iin, debtNumber, err)
					logMongoFail(ctx, p.MG, importRecordID, "debts", uuid.NewString(), m, err.Error())
				}
			}
		}

		if p.AddressesRepo != nil {
			addresses := []struct {
				key    string
				typeID int
			}{
				{"reg_address", 1},
				{"fact_address", 2},
				{"work_address", 3},
			}
			for _, a := range addresses {
				addr := strings.TrimSpace(m[a.key])
				if addr == "" {
					continue
				}
				addrRow := models.Address{
					DebtorID: debtor.ID,
					IIN:      iin,
					Address:  addr,
					TypeID:   &a.typeID,
				}
				if err := p.AddressesRepo.SaveAddress(ctx, addrRow); err != nil {
					log.Printf("[PROC][addresses][ERR] iin=%s type_id=%d: %v", iin, a.typeID, err)
					logMongoFail(ctx, p.MG, importRecordID, "addresses", uuid.NewString(), m, err.Error())
				}
			}
		}

		if p.PhonesRepo != nil {
			phones := []struct {
				key    string
				typeID int
			}{
				{"phones", 1},
				{"work_phones", 2},
				{"home_phones", 3},
			}

			for _, ph := range phones {
				raw := strings.TrimSpace(m[ph.key])
				if raw == "" {
					continue
				}

				phoneRow := models.Phone{
					SubjectType: "App\\Infrastructure\\Persistence\\Models\\Debtor",
					SubjectID:   debtor.ID,
					PhonesRaw:   raw,
					TypeID:      &ph.typeID,
					CreatedAt:   nowPtr(),
				}

				if err := p.PhonesRepo.SavePhones(ctx, phoneRow); err != nil {
					log.Printf("[PROC][phones][ERR] iin=%s phones=%s: %v", iin, raw, err)
					logMongoFail(ctx, p.MG, importRecordID, "phones", uuid.NewString(), m, err.Error())
				}
			}
		}

		if p.ContactPersonPhonesRepo != nil {
			raw := strings.TrimSpace(m["contact_person_phones"])
			if raw != "" {
				contactRow := database.ContactPersonPhoneRow{
					DebtorID: debtor.ID,
					Value:    raw,
				}
				if err := p.ContactPersonPhonesRepo.SaveContactPersonPhones(ctx, contactRow); err != nil {
					log.Printf("[PROC][contact_phones][ERR] iin=%s value=%s: %v", iin, raw, err)
					logMongoFail(ctx, p.MG, importRecordID, "contact_person_phones", uuid.NewString(), m, err.Error())
				}
			}
		}

		status := "done"
		success++

		if _, err := importitems.InsertItem(ctx, p.MG, importitems.Item{
			ImportRecordID: importRecordID,
			ModelType:      "debtors",
			ModelID:        debtor.ID,
			Payload:        mustJSON(m),
			Status:         status,
			Errors:         "",
		}); err != nil {
			log.Printf("[PROC][mongo][ERR] row=%d iin=%s: %v", i, iin, err)
		}
	}

	log.Printf("[PROC][debtors][DONE] total=%d success=%d failed=%d", len(batch), success, failed)
	if err := importitems.UpdateImportRecordStatusDone(ctx, p.MG, importRecordID); err != nil {
		log.Printf("[PROC][debtors][ERR] update import_record status: %v", err)
	}

	return nil
}

func parseDate(s string) *time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	layouts := []string{"2006-01-02", "02.01.2006", "02/01/2006", "02-01-2006"}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return &t
		}
	}
	return nil
}

func parseFloatPtr(s string) *float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	v, err := strconv.ParseFloat(strings.ReplaceAll(s, ",", "."), 64)
	if err != nil {
		return nil
	}
	return &v
}
