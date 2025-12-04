package processors

import (
	"encoding/json"
	"log"
	"strings"
	"time"
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
