package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"debtster_import/internal/repository"
)

type ctxKey string

const UserIDKey ctxKey = "userID"

type TokenRepo interface {
	FindTokenByPlainToken(ctx context.Context, plainToken string) (*repository.PersonalAccessToken, error)
}

func SanctumMiddleware(tokenRepo TokenRepo) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// allow OPTIONS (CORS preflight) to pass through
			if r.Method == http.MethodOptions {
				next.ServeHTTP(w, r)
				return
			}

			// Try Authorization header first
			authHeader := r.Header.Get("Authorization")
			var pat *repository.PersonalAccessToken
			if authHeader != "" && strings.HasPrefix(authHeader, "Bearer ") {
				plainToken := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
				if plainToken != "" {
					p, err := tokenRepo.FindTokenByPlainToken(r.Context(), plainToken)
					if err == nil {
						pat = p
					} else {
						fmt.Printf("[AUTH] token lookup (header) error: %v\n", err)
					}
				}
			}

			// If not found in header, try token query parameter
			if pat == nil {
				token := r.URL.Query().Get("token")
				if token != "" {
					p, err := tokenRepo.FindTokenByPlainToken(r.Context(), token)
					if err == nil {
						pat = p
					} else {
						fmt.Printf("[AUTH] token lookup (query) error: %v\n", err)
					}
				}
			}

			if pat == nil {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			if pat.ExpiresAt != nil && pat.ExpiresAt.Before(time.Now()) {
				http.Error(w, "Token expired", http.StatusUnauthorized)
				return
			}

			// Store user id in context as string (for consistency with import records)
			uid := fmt.Sprintf("%d", pat.UserID)
			ctx := context.WithValue(r.Context(), UserIDKey, uid)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func GetUserID(ctx context.Context) (string, error) {
	v, ok := ctx.Value(UserIDKey).(string)
	if !ok || v == "" {
		return "", errors.New("userID not found in context")
	}
	return v, nil
}
