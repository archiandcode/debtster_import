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

	reqCopy := req

	go func() {
		start := time.Now()

		httpOp := opener.NewHTTPOpener(h.HTTP)
		s3Op := opener.NewS3Opener(h.S3.Client)
		compound := opener.NewCompoundOpener(httpOp, s3Op, h.S3.Bucket)

		svc := importer.NewService(compound, h.Registry, reqCopy.BatchSize)

		timeout := 15 * time.Minute
		if reqCopy.TimeoutMin > 0 {
			timeout = time.Duration(reqCopy.TimeoutMin) * time.Minute
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		res, err := svc.Import(ctx, importer.Request{
			Type:           reqCopy.Type,
			FilePath:       reqCopy.FilePath,
			BatchSize:      reqCopy.BatchSize,
			ImportRecordID: reqCopy.ImportRecordID,
		})
		if err != nil {
			h.Logger.Printf("[IMPORT][ERR][BG] type=%q path=%q err=%v took=%s",
				reqCopy.Type, reqCopy.FilePath, err, time.Since(start))
			return
		}

		h.Logger.Printf("[IMPORT][OK][BG] type=%q src=%s fmt=%s rows=%d bucket=%q key=%q size=%d took=%s",
			reqCopy.Type, res.Source, res.Format, res.RowsProcessed, res.Bucket, res.Key, res.SizeBytes, time.Since(start))
	}()

	h.JSON(w, http.StatusAccepted, map[string]any{
		"status":           "started",
		"type":             req.Type,
		"file_path":        req.FilePath,
		"batch_size":       req.BatchSize,
		"import_record_id": req.ImportRecordID,
	})
}
