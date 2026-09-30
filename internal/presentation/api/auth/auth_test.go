package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/matryer/is"
)

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

// THINGS-002: errors wrapped with %w must preserve the chain, so
// errors.Is/As keep working across the presentation layer.
func TestNewAuthenticatorReadErrorWrapsCause(t *testing.T) {
	is := is.New(t)

	_, err := NewAuthenticator(context.Background(), errReader{})
	is.True(err != nil)
	is.Equal(err.Error(), "unable to read authz policies: read failed")
	is.True(errors.Unwrap(err) != nil)
	is.Equal(errors.Unwrap(err).Error(), "read failed")
}

func TestAccessObjectDefaultAndExplicitLegacy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy string
		opts   []Option
		want   int
	}{
		{"access-default", `package example.authz
allow := {"access": {"read-only": ["things.read"], "writer": ["things.create"]}}`, nil, http.StatusNoContent},
		{"legacy-requires-flag", `package example.authz
allow := {"tenants": ["writer"]}`, nil, http.StatusInternalServerError},
		{"explicit-legacy", `package example.authz
allow := {"tenants": ["writer"]}`, []Option{WithAccessObjectAuthorization(false)}, http.StatusNoContent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, err := NewAuthenticator(t.Context(), strings.NewReader(tc.policy), tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			h := a.RequireAccess("things.create")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got := GetTenantsWithAllowedScopes(r.Context(), "things.create")
				if len(got) != 1 || got[0] != "writer" {
					t.Errorf("scope filtering changed: %v", got)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			r := httptest.NewRequest(http.MethodPost, "/api/v0/things", nil)
			r.Header.Set("Authorization", "Bearer test")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Errorf("status %d, want %d", w.Code, tc.want)
			}
		})
	}
}
