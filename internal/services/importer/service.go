package importer

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"mime"
	"net/url"
	"path"
	"strings"
	"time"

	"debtster_import/internal/ports"

	"github.com/xuri/excelize/v2"
)

type Request struct {
	Type           string
	FilePath       string
	BatchSize      int
	ImportRecordID string
}

type Result struct {
	Source           string
	FilePath         string
	Format           string
	RowsProcessed    int
	SHA256FirstChunk string
	ContentType      string
	Bucket           string
	Key              string
	SizeBytes        int64
}

type Service struct {
	Opener     ports.FileOpener
	Processors map[string]ports.Processor
	DefaultBS  int
}

func NewService(opener ports.FileOpener, registry map[string]ports.Processor, defaultBatch int) *Service {
	if defaultBatch <= 0 {
		defaultBatch = 1000
	}
	return &Service{Opener: opener, Processors: registry, DefaultBS: defaultBatch}
}

func (s *Service) Import(ctx context.Context, req Request) (Result, error) {
	log.Printf("%v", req.ImportRecordID)
	t0 := time.Now()
	ctx = context.WithValue(ctx, ports.CtxImportRecordID, req.ImportRecordID)
	log.Printf("[IMP][START] type=%q path=%q batch_size=%d import_record_id=%q", req.Type, req.FilePath, req.BatchSize, req.ImportRecordID)

	proc, ok := s.Processors[req.Type]
	if !ok {
		log.Printf("[IMP][ERR] no processor for type=%q", req.Type)
		return Result{}, errors.New("no processor for type: " + req.Type)
	}

	rc, meta, err := s.Opener.Open(ctx, req.FilePath)
	if err != nil {
		log.Printf("[IMP][ERR] open: %v", err)
		return Result{}, err
	}
	defer rc.Close()

	format := detectFormat(req.FilePath, meta.ContentType)
	log.Printf("[IMP] source=%s content_type=%q size=%d detected_format=%s", meta.Source, meta.ContentType, meta.Size, format)

	hasher := sha256.New()
	tee := io.TeeReader(rc, hasher)

	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = s.DefaultBS
	}

	var total int
	var readErr error

	switch format {
	case "xlsx":
		log.Printf("[IMP] using XLSX first-sheet reader")
		total, readErr = s.streamXLSXFirstSheet(ctx, tee, proc, batchSize)
		if readErr != nil {
			log.Printf("[IMP][XLSX][ERR] %v — fallback to CSV", readErr)
			total, readErr = s.streamCSV(ctx, tee, proc, batchSize)
			if readErr == nil {
				format = "csv"
			}
		}
	case "csv":
		log.Printf("[IMP] using CSV reader")
		total, readErr = s.streamCSV(ctx, tee, proc, batchSize)
		if readErr != nil {
			log.Printf("[IMP][CSV][ERR] %v — fallback to XLSX", readErr)
			total, readErr = s.streamXLSXFirstSheet(ctx, tee, proc, batchSize)
			if readErr == nil {
				format = "xlsx"
			}
		}
	default:
		log.Printf("[IMP] unknown format — try XLSX then CSV")
		total, readErr = s.streamXLSXFirstSheet(ctx, tee, proc, batchSize)
		if readErr != nil {
			log.Printf("[IMP][XLSX][ERR] %v — fallback to CSV", readErr)
			total, readErr = s.streamCSV(ctx, tee, proc, batchSize)
			if readErr == nil {
				format = "csv"
			}
		} else {
			format = "xlsx"
		}
	}

	if readErr != nil {
		log.Printf("[IMP][ERR] read pipeline: %v", readErr)
		return Result{}, readErr
	}

	sum := hex.EncodeToString(hasher.Sum(nil))
	dur := time.Since(t0)
	log.Printf("[IMP][DONE] type=%q fmt=%s rows=%d sha256=%s duration=%s", req.Type, format, total, sum, dur)

	return Result{
		Source:           meta.Source,
		FilePath:         req.FilePath,
		Format:           format,
		RowsProcessed:    total,
		SHA256FirstChunk: sum,
		ContentType:      meta.ContentType,
		Bucket:           meta.Bucket,
		Key:              meta.Key,
		SizeBytes:        meta.Size,
	}, nil
}

func (s *Service) streamCSV(ctx context.Context, r io.Reader, proc ports.Processor, batchSize int) (int, error) {
	start := time.Now()
	reader := csv.NewReader(bufio.NewReader(r))
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return 0, err
	}
	log.Printf("[IMP][CSV] header=%v", header)

	hmap := make([]string, len(header))
	copy(hmap, header)

	batch := make([]map[string]string, 0, batchSize)
	total, batches := 0, 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			if len(batch) > 0 {
				log.Printf("[IMP][CSV] send final batch size=%d", len(batch))
				if e := proc.ProcessBatch(ctx, batch); e != nil {
					return total, e
				}
				total += len(batch)
				batches++
			}
			break
		}
		if err != nil {
			log.Printf("[IMP][CSV][WARN] read row err: %v", err)
			continue
		}
		row := toMap(hmap, record)
		batch = append(batch, row)

		if len(batch) >= batchSize {
			log.Printf("[IMP][CSV] send batch #%d size=%d total_so_far=%d", batches+1, len(batch), total)
			if e := proc.ProcessBatch(ctx, batch); e != nil {
				return total, e
			}
			total += len(batch)
			batches++
			batch = batch[:0]
		}
	}
	log.Printf("[IMP][CSV][DONE] total_rows=%d batches=%d duration=%s", total, batches, time.Since(start))
	return total, nil
}

func (s *Service) streamXLSXFirstSheet(ctx context.Context, r io.Reader, proc ports.Processor, batchSize int) (int, error) {
	start := time.Now()
	f, err := excelize.OpenReader(r)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		return 0, errors.New("xlsx has no sheets")
	}
	sheet := sheets[0]
	log.Printf("[IMP][XLSX] first_sheet=%q", sheet)

	rows, err := f.Rows(sheet)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	if !rows.Next() {
		if rows.Error() != nil {
			return 0, rows.Error()
		}
		return 0, nil
	}
	header, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	log.Printf("[IMP][XLSX] header=%v", header)

	hmap := make([]string, len(header))
	copy(hmap, header)

	batch := make([]map[string]string, 0, batchSize)
	total, batches := 0, 0

	for rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			log.Printf("[IMP][XLSX][WARN] read row err: %v", err)
			continue
		}
		row := toMap(hmap, cols)
		batch = append(batch, row)

		if len(batch) >= batchSize {
			log.Printf("[IMP][XLSX] send batch #%d size=%d total_so_far=%d", batches+1, len(batch), total)
			if e := proc.ProcessBatch(ctx, batch); e != nil {
				return total, e
			}
			total += len(batch)
			batches++
			batch = batch[:0]
		}
	}
	if err := rows.Error(); err != nil {
		return total, err
	}
	if len(batch) > 0 {
		log.Printf("[IMP][XLSX] send final batch size=%d", len(batch))
		if e := proc.ProcessBatch(ctx, batch); e != nil {
			return total, e
		}
		total += len(batch)
		batches++
	}
	log.Printf("[IMP][XLSX][DONE] total_rows=%d batches=%d duration=%s", total, batches, time.Since(start))
	return total, nil
}

// ---------- helpers ----------

func toMap(header []string, row []string) map[string]string {
	m := make(map[string]string, len(header))
	for i, key := range header {
		val := ""
		if i < len(row) {
			val = row[i]
		}
		m[strings.TrimSpace(key)] = strings.TrimSpace(val)
	}
	return m
}

func detectFormat(filePath, contentType string) string {
	p := filePath
	if u, err := url.Parse(filePath); err == nil && u != nil && u.Path != "" {
		p = u.Path
	}
	ext := strings.ToLower(strings.TrimPrefix(path.Ext(p), "."))
	switch ext {
	case "xlsx":
		return "xlsx"
	case "csv":
		return "csv"
	}
	med, _, _ := mime.ParseMediaType(contentType)
	switch med {
	case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
		return "xlsx"
	case "text/csv", "application/csv", "text/plain":
		return "csv"
	}
	return ""
}
