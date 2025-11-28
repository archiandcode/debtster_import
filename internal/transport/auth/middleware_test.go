package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"debtster_import/internal/repository"
)

type fakeRepo struct {
	token *repository.PersonalAccessToken
	err   error
}

func (f *fakeRepo) FindTokenByPlainToken(ctx context.Context, plainToken string) (*repository.PersonalAccessToken, error) {
	return f.token, f.err
}

func TestSanctumMiddleware_setsUserID(t *testing.T) {
	// fake token with user id
	token := &repository.PersonalAccessToken{ID: 1, UserID: 123}
	fr := &fakeRepo{token: token}

	got := ""
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uid, err := GetUserID(r.Context())
		if err != nil {
			t.Fatalf("expected user id present, got err: %v", err)
		}
		got = uid
		w.WriteHeader(http.StatusOK)
	})

	mw := SanctumMiddleware(fr)
	srv := mw(handler)

	req := httptest.NewRequest("POST", "/upload", nil)
	req.Header.Set("Authorization", "Bearer mytoken")
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}
	if got == "" {
		t.Fatalf("expected user id to be set in context")
	}
}

func TestSanctumMiddleware_blockWhenMissing(t *testing.T) {
	fr := &fakeRepo{token: nil}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("should not reach handler with missing token")
	})
	mw := SanctumMiddleware(fr)
	srv := mw(handler)

	req := httptest.NewRequest("POST", "/upload", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", rr.Code)
	}
}

func TestSanctumMiddleware_allowsOptions(t *testing.T) {
	fr := &fakeRepo{token: nil}
	reached := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusNoContent)
	})
	mw := SanctumMiddleware(fr)
	srv := mw(handler)

	req := httptest.NewRequest("OPTIONS", "/upload", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204 No Content, got %d", rr.Code)
	}
	if !reached {
		t.Fatalf("expected handler to be reached on OPTIONS")
	}
}
