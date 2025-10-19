package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"debtster_import/internal/handlers"
)

type Server struct {
	httpServer *http.Server
}

func NewServer(port string, h *handlers.Handlers) *Server {
	mux := http.NewServeMux()

	if h != nil {
		mux.HandleFunc("/health", h.Health)
		mux.HandleFunc("/import", h.Import)
	}

	return &Server{
		httpServer: &http.Server{
			Addr:         fmt.Sprintf(":%s", port),
			Handler:      mux,
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
	}
}

func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.httpServer.Shutdown(shCtx)
	case err := <-errCh:
		return err
	}
}
