package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type healthResp struct {
	OK     bool     `json:"ok"`
	Errors []string `json:"errors,omitempty"`
}

func (h *Handlers) Health(w http.ResponseWriter, _ *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var errs []string

	if h.Postgres == nil || h.Postgres.Pool == nil {
		errs = append(errs, "postgres not initialized")
	} else if err := h.Postgres.Pool.Ping(ctx); err != nil {
		errs = append(errs, "postgres ping failed: "+err.Error())
	}

	if h.Mongo == nil || h.Mongo.Client == nil {
		errs = append(errs, "mongo not initialized")
	} else if err := h.Mongo.Client.Ping(ctx, nil); err != nil {
		errs = append(errs, "mongo ping failed: "+err.Error())
	}

	if h.S3 == nil || h.S3.Client == nil {
		errs = append(errs, "s3 not initialized")
	} else if ok, err := h.S3.Client.BucketExists(ctx, h.S3.Bucket); err != nil {
		errs = append(errs, "s3 bucket check failed: "+err.Error())
	} else if !ok {
		errs = append(errs, `s3 bucket "`+h.S3.Bucket+`" not found`)
	}

	resp := healthResp{OK: len(errs) == 0}
	if len(errs) > 0 {
		resp.Errors = errs
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
