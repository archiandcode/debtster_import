package handlers

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"time"

	importitems "debtster_import/internal/repository/imports"
	auth "debtster_import/internal/transport/auth"

	"github.com/minio/minio-go/v7"
)

// Upload accepts multipart/form-data with `file` and `action` fields and stores the file in S3
// and creates an import_record entry in Mongo.
func (h *Handlers) Upload(w http.ResponseWriter, r *http.Request) {
	// CORS preflight support for simple usage from frontend apps
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if r.Method != http.MethodPost {
		h.JSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "use POST"})
		return
	}

	if err := r.ParseMultipartForm(128 << 20); err != nil {
		h.Logger.Printf("[UPLOAD][ERR] parse multipart: %v", err)
		h.JSON(w, http.StatusBadRequest, map[string]any{"error": "bad multipart: " + err.Error()})
		return
	}

	action := r.FormValue("action")
	if action == "" {
		action = r.FormValue("type")
	}
	if action == "" {
		h.JSON(w, http.StatusBadRequest, map[string]any{"error": "action/type is required"})
		return
	}

	f, fh, err := r.FormFile("file")
	if err != nil {
		h.Logger.Printf("[UPLOAD][ERR] missing file: %v", err)
		h.JSON(w, http.StatusBadRequest, map[string]any{"error": "file is required"})
		return
	}
	defer f.Close()

	fname := path.Base(fh.Filename)
	key := fmt.Sprintf("imports/%d-%s", time.Now().UnixNano(), fname)

	size := fh.Size
	if size <= 0 {
		size = -1
	}

	info, err := h.S3.Client.PutObject(context.Background(), h.S3.Bucket, key, f, size, minio.PutObjectOptions{ContentType: fh.Header.Get("Content-Type")})
	if err != nil {
		h.Logger.Printf("[UPLOAD][ERR] s3 put: %v", err)
		h.JSON(w, http.StatusInternalServerError, map[string]any{"error": "failed to store file: " + err.Error()})
		return
	}

	s3path := fmt.Sprintf("s3://%s/%s", h.S3.Bucket, key)

	rec := importitems.Record{
		UserID:    nil,
		Count:     0,
		Status:    "parsed",
		Type:      action,
		Path:      &s3path,
		Bucket:    &h.S3.Bucket,
		Key:       &key,
		SizeBytes: &info.Size,
	}

	if userID, errGet := auth.GetUserID(r.Context()); errGet == nil {
		rec.UserID = &userID
	}

	ins, err := importitems.InsertImportRecord(r.Context(), h.Mongo, rec)
	if err != nil {
		h.Logger.Printf("[UPLOAD][ERR] db insert: %v", err)
		h.JSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	// add CORS header so browser clients can read the response
	w.Header().Set("Access-Control-Allow-Origin", "*")
	h.JSON(w, http.StatusCreated, map[string]any{"id": ins.InsertedID, "path": s3path})
}
