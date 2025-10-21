package processors

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	mg "debtster_import/internal/config/connections/mongo"
	"debtster_import/internal/config/connections/postgres"
	importitems "debtster_import/internal/repository/imports"
)

func firstNonEmpty(s, def string) string {
	if strings.TrimSpace(s) == "" {
		return def
	}
	return s
}

func nullIfEmpty(s string) *string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	return &s
}

func mustJSON(m map[string]string) string {
	b, err := json.Marshal(m)
	if err != nil {
		log.Printf("[PROC][WARN] json marshal payload failed: %v; fallback {}", err)
		return "{}"
	}
	return string(b)
}

func normalizeAmount(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "0"
	}
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, ",", ".")
	return s
}

func parseTimeLoose(s string) *time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	layouts := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"02.01.2006 15:04:05",
		"02.01.2006",
		"2006-01-02",
	}
	for _, l := range layouts {
		if t, err := time.ParseInLocation(l, s, time.Local); err == nil {
			return &t
		}
	}
	if t, err := time.Parse(time.RFC1123Z, s); err == nil {
		return &t
	}
	return nil
}

func parseDateStrict(s string) *time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	layouts := []string{
		"2006-01-02",
		"02.01.2006",
		"2006/01/02",
		time.RFC3339,
		"2006-01-02 15:04:05",
		"02.01.2006 15:04:05",
	}
	for _, l := range layouts {
		if t, err := time.ParseInLocation(l, s, time.Local); err == nil {
			tt := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.Local)
			return &tt
		}
	}
	return nil
}

func nowPtr() *time.Time {
	t := time.Now()
	return &t
}

func logMongoFail(ctx context.Context, mgc *mg.Mongo, importRecordID, model, id string, payload map[string]string, errText string) {
	logMongo(ctx, mgc, importRecordID, model, id, payload, "failed", errText)
}

func logMongo(ctx context.Context, mgc *mg.Mongo, importRecordID, model, id string, payload map[string]string, status, errText string) {
	if mgc == nil || mgc.Database == nil {
		return
	}
	b, _ := json.Marshal(payload)
	if _, mErr := importitems.InsertItem(ctx, mgc, importitems.Item{
		ImportRecordID: importRecordID,
		ModelType:      model,
		ModelID:        id,
		Payload:        string(b),
		Status:         status,
		Errors:         errText,
	}); mErr != nil {
		log.Printf("[PROC][%s][MONGO][ERR] id=%s status=%s err=%v", model, id, status, mErr)
	}
}

func getDebtUUID(ctx context.Context, pg *postgres.Postgres, table, number string, cache map[string]*string) (*string, error) {
	if v, ok := cache[number]; ok {
		return v, nil
	}
	var id string
	err := pg.Pool.QueryRow(ctx, `SELECT id::text FROM `+table+` WHERE number = $1 LIMIT 1`, number).Scan(&id)
	if err != nil {
		cache[number] = nil
		return nil, err
	}
	cache[number] = &id
	return &id, nil
}

func getUserBigint(ctx context.Context, pg *postgres.Postgres, table, username string, cache map[string]*int64) (*int64, error) {
	if v, ok := cache[username]; ok {
		return v, nil
	}
	var id int64
	err := pg.Pool.QueryRow(ctx, `SELECT id FROM `+table+` WHERE username = $1 LIMIT 1`, username).Scan(&id)
	if err != nil {
		cache[username] = nil
		return nil, err
	}
	cache[username] = &id
	return &id, nil
}

func getStatusBigint(ctx context.Context, pg *postgres.Postgres, table, shortname string, cache map[string]*int64) (*int64, error) {
	if v, ok := cache[shortname]; ok {
		return v, nil
	}
	var id int64
	err := pg.Pool.QueryRow(ctx, `SELECT id FROM `+table+` WHERE shortname = $1 LIMIT 1`, shortname).Scan(&id)
	if err != nil {
		cache[shortname] = nil
		return nil, err
	}
	cache[shortname] = &id
	return &id, nil
}
