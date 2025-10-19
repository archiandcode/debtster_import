package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"debtster_import/internal/adapters/opener"
	"debtster_import/internal/services/importer"
)

type importRequest struct {
	Type           string `json:"type"`
	FilePath       string `json:"file_path"`
	BatchSize      int    `json:"batch_size"`
	TimeoutMin     int    `json:"timeout_minutes,omitempty"`
	ImportRecordID string `json:"import_record_id"`
}

func (h *Handlers) Import(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.Method != http.MethodPost {
		h.JSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "use POST"})
		return
	}

	var req importRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		h.Logger.Printf("[IMPORT][REQ][ERR] bad JSON: %v", err)
		h.JSON(w, http.StatusBadRequest, map[string]string{"error": "bad JSON: " + err.Error()})
		return
	}
	if strings.TrimSpace(req.FilePath) == "" {
		h.Logger.Printf("[IMPORT][REQ][ERR] file_path is required")
		h.JSON(w, http.StatusBadRequest, map[string]string{"error": "file_path is required"})
		return
	}
	if req.BatchSize <= 0 {
		req.BatchSize = 1000
	}
	h.Logger.Printf("[IMPORT][REQ] type=%q file_path=%q batch_size=%d timeout_min=%d import_record_id=%q remote=%s",
		req.Type, req.FilePath, req.BatchSize, req.TimeoutMin, req.ImportRecordID, r.RemoteAddr)

	httpOp := opener.NewHTTPOpener(h.HTTP)
	s3Op := opener.NewS3Opener(h.S3.Client)
	compound := opener.NewCompoundOpener(httpOp, s3Op, h.S3.Bucket)

	svc := importer.NewService(compound, h.Registry, req.BatchSize)

	timeout := 15 * time.Minute
	if req.TimeoutMin > 0 {
		timeout = time.Duration(req.TimeoutMin) * time.Minute
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	res, err := svc.Import(ctx, importer.Request{
		Type:           req.Type,
		FilePath:       req.FilePath,
		BatchSize:      req.BatchSize,
		ImportRecordID: req.ImportRecordID,
	})
	if err != nil {
		h.Logger.Printf("[IMPORT][ERR] type=%q path=%q err=%v duration=%s", req.Type, req.FilePath, err, time.Since(start))
		h.JSON(w, http.StatusBadRequest, map[string]string{"error": "import failed: " + err.Error()})
		return
	}

	h.Logger.Printf("[IMPORT][OK] type=%q src=%s fmt=%s rows=%d sha256_first_chunk=%s bucket=%q key=%q size=%d duration=%s",
		req.Type, res.Source, res.Format, res.RowsProcessed, res.SHA256FirstChunk, res.Bucket, res.Key, res.SizeBytes, time.Since(start))

	h.JSON(w, http.StatusOK, map[string]any{
		"status":             "ok",
		"type":               req.Type,
		"file_path":          res.FilePath,
		"source":             res.Source,
		"format":             res.Format,
		"rows_processed":     res.RowsProcessed,
		"sha256_first_chunk": res.SHA256FirstChunk,
		"content_type":       res.ContentType,
		"bucket":             res.Bucket,
		"key":                res.Key,
		"size_bytes":         res.SizeBytes,
		"import_record_id":   req.ImportRecordID,
	})
}
