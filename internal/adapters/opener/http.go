package opener

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"

	"debtster_import/internal/ports"
)

type HTTPOpener struct{ Client *http.Client }

func NewHTTPOpener(cli *http.Client) *HTTPOpener {
	if cli == nil {
		cli = &http.Client{}
	}
	return &HTTPOpener{Client: cli}
}

func (h *HTTPOpener) Open(ctx context.Context, url string) (io.ReadCloser, ports.Meta, error) {
	log.Printf("[OPENER][HTTP][START] url=%q", url)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		log.Printf("[OPENER][HTTP][ERR] build request: %v", err)
		return nil, ports.Meta{}, err
	}
	resp, err := h.Client.Do(req)
	if err != nil {
		log.Printf("[OPENER][HTTP][ERR] do request: %v", err)
		return nil, ports.Meta{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		ct := resp.Header.Get("Content-Type")
		cl := resp.ContentLength
		log.Printf("[OPENER][HTTP][ERR] status=%d content_type=%q content_length=%d", resp.StatusCode, ct, cl)
		defer resp.Body.Close()
		return nil, ports.Meta{}, fmt.Errorf("http status %d", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	size := resp.ContentLength
	log.Printf("[OPENER][HTTP][OK] content_type=%q size=%d", ct, size)
	if size < 0 {
		size = -1
	}
	return resp.Body, ports.Meta{
		Source:      "https",
		ContentType: ct,
		Size:        size,
	}, nil
}
