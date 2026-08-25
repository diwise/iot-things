package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	app "github.com/diwise/iot-things/internal/application"
	"github.com/diwise/iot-things/internal/application/things"
	"github.com/diwise/iot-things/internal/presentation/api/auth"
)

type fakeThingsApp struct {
	queryThingsFunc func(context.Context, app.ThingQuery) (app.QueryResult, error)
	queryValuesFunc func(context.Context, app.ValueQuery) (app.QueryResult, error)
	addFunc         func(context.Context, []byte) error
	seedFunc        func(context.Context, io.Reader) error
}

func (f fakeThingsApp) HandleMeasurements(ctx context.Context, measurements []things.Measurement) {}
func (f fakeThingsApp) Add(ctx context.Context, b []byte) error {
	if f.addFunc != nil {
		return f.addFunc(ctx, b)
	}
	return nil
}
func (f fakeThingsApp) Delete(ctx context.Context, thingID string, tenants []string) error {
	return nil
}
func (f fakeThingsApp) Merge(ctx context.Context, thingID string, b []byte, tenants []string) error {
	return nil
}
func (f fakeThingsApp) Query(ctx context.Context, query app.ThingQuery) (app.QueryResult, error) {
	if f.queryThingsFunc != nil {
		return f.queryThingsFunc(ctx, query)
	}
	return app.QueryResult{}, nil
}
func (f fakeThingsApp) Update(ctx context.Context, b []byte, tenants []string) error { return nil }
func (f fakeThingsApp) AddValue(ctx context.Context, t things.Thing, m things.Value) error {
	return nil
}
func (f fakeThingsApp) Values(ctx context.Context, query app.ValueQuery) (app.QueryResult, error) {
	if f.queryValuesFunc != nil {
		return f.queryValuesFunc(ctx, query)
	}
	return app.QueryResult{}, nil
}
func (f fakeThingsApp) Tags(ctx context.Context, tenants []string) ([]string, error) {
	return nil, nil
}
func (f fakeThingsApp) Types(ctx context.Context, tenants []string) ([]things.ThingType, error) {
	return nil, nil
}
func (f fakeThingsApp) LoadConfig(ctx context.Context, r io.Reader) error { return nil }
func (f fakeThingsApp) Seed(ctx context.Context, r io.Reader) error {
	if f.seedFunc != nil {
		return f.seedFunc(ctx, r)
	}
	return nil
}

func requestWithAccess(req *http.Request, scopes ...auth.Scope) *http.Request {
	tenantScopes := map[auth.Scope]struct{}{}
	for _, scope := range scopes {
		tenantScopes[scope] = struct{}{}
	}

	ctx := auth.WithAccess(req.Context(), map[string]map[auth.Scope]struct{}{
		"default": tenantScopes,
	})
	return req.WithContext(ctx)
}

func TestAddHandlerRejectsCreateForUnauthorizedTenant(t *testing.T) {
	called := false

	h := addHandler(slog.Default(), fakeThingsApp{
		addFunc: func(ctx context.Context, b []byte) error {
			called = true
			return nil
		},
	})

	thing := things.NewWasteContainer("thing-1", things.DefaultLocation, "other")
	req := httptest.NewRequest(http.MethodPost, "/things", strings.NewReader(string(thing.Byte())))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, requestWithAccess(req, CreateThings))

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d", rr.Code)
	}
	if called {
		t.Fatal("expected application Add not to be called")
	}
}

func TestAddHandlerAllowsCreateForAuthorizedTenant(t *testing.T) {
	called := false

	h := addHandler(slog.Default(), fakeThingsApp{
		addFunc: func(ctx context.Context, b []byte) error {
			called = true
			return nil
		},
	})

	thing := things.NewWasteContainer("thing-1", things.DefaultLocation, "default")
	req := httptest.NewRequest(http.MethodPost, "/things", strings.NewReader(string(thing.Byte())))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, requestWithAccess(req, CreateThings))

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d", rr.Code)
	}
	if !called {
		t.Fatal("expected application Add to be called")
	}
}

func TestValidateSeedTenantsRejectsUnauthorizedTenant(t *testing.T) {
	csv := strings.NewReader("id,type,subType,name,decsription,location,tenant,tags,refDevices,args\nthing-1,Container,WasteContainer,name,desc,\"0,0\",other,,,\n")

	err := validateSeedTenants(csv, []string{"default"})
	if err == nil {
		t.Fatal("expected unauthorized tenant error")
	}
}

func TestValidateSeedTenantsAllowsAuthorizedTenant(t *testing.T) {
	csv := strings.NewReader("id,type,subType,name,decsription,location,tenant,tags,refDevices,args\nthing-1,Container,WasteContainer,name,desc,\"0,0\",default,,,\n")

	err := validateSeedTenants(csv, []string{"default"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestQueryHandlerCSVExportReturns500WithoutPartialCSVOnError(t *testing.T) {
	thing := things.NewWasteContainer("thing-1", things.DefaultLocation, "default")

	h := queryHandler(slog.Default(), fakeThingsApp{
		queryThingsFunc: func(ctx context.Context, query app.ThingQuery) (app.QueryResult, error) {
			return app.QueryResult{
				Count: 2,
				Data: [][]byte{
					thing.Byte(),
					[]byte("not-json"),
				},
			}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/things", nil)
	req.Header.Set("Accept", "text/csv")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, requestWithAccess(req, ReadThings))

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", rr.Code)
	}

	if strings.Contains(rr.Body.String(), "id;type;subType") {
		t.Fatalf("expected no partial CSV body on export error, got %q", rr.Body.String())
	}

	if !strings.Contains(rr.Body.String(), "unexpected end of JSON input") && !strings.Contains(rr.Body.String(), "invalid") {
		t.Fatalf("expected export error in response body, got %q", rr.Body.String())
	}
}

func TestQueryHandlerCSVExportReturnsCSVContentType(t *testing.T) {
	thing := things.NewWasteContainer("thing-1", things.DefaultLocation, "default")
	thing.AddTag("tag1")

	h := queryHandler(slog.Default(), fakeThingsApp{
		queryThingsFunc: func(ctx context.Context, query app.ThingQuery) (app.QueryResult, error) {
			return app.QueryResult{
				Count: 1,
				Data:  [][]byte{thing.Byte()},
			}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/things", nil)
	req.Header.Set("Accept", "text/csv")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, requestWithAccess(req, ReadThings))

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	if got := rr.Header().Get("Content-Type"); got != "text/csv" {
		t.Fatalf("expected text/csv content type, got %q", got)
	}

	if !strings.Contains(rr.Body.String(), "id;type;subType") {
		t.Fatalf("expected CSV header in response body, got %q", rr.Body.String())
	}
}

func TestQueryHandlerRejectsInvalidThingQuery(t *testing.T) {
	called := false

	h := queryHandler(slog.Default(), fakeThingsApp{
		queryThingsFunc: func(ctx context.Context, query app.ThingQuery) (app.QueryResult, error) {
			called = true
			return app.QueryResult{}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/things?limit=0", nil)
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, requestWithAccess(req, ReadThings))

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rr.Code)
	}
	if called {
		t.Fatal("expected application query not to be called for invalid request")
	}
}

func TestGetValuesHandlerRejectsConflictingValueQueryModes(t *testing.T) {
	called := false

	h := getValuesHandler(slog.Default(), fakeThingsApp{
		queryValuesFunc: func(ctx context.Context, query app.ValueQuery) (app.QueryResult, error) {
			called = true
			return app.QueryResult{}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/things/values?thingid=thing-1&latest=true&distinct=v", nil)
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, requestWithAccess(req, ReadThings))

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rr.Code)
	}
	if called {
		t.Fatal("expected application query not to be called for invalid request")
	}
}

func TestGetValuesHandlerCSVExportReturns500WithoutPartialCSVOnError(t *testing.T) {
	h := getValuesHandler(slog.Default(), fakeThingsApp{
		queryValuesFunc: func(ctx context.Context, query app.ValueQuery) (app.QueryResult, error) {
			return app.QueryResult{
				Count: 2,
				Data: [][]byte{
					[]byte(`{"id":"value-1","timestamp":"2024-01-01T00:00:00Z"}`),
					[]byte("not-json"),
				},
			}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/things/values", nil)
	req.Header.Set("Accept", "text/csv")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, requestWithAccess(req, ReadThings))

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "time;id;urn") {
		t.Fatalf("expected no partial CSV body on export error, got %q", rr.Body.String())
	}
}

func TestGetValuesHandlerCSVExportReturnsCSVContentType(t *testing.T) {
	h := getValuesHandler(slog.Default(), fakeThingsApp{
		queryValuesFunc: func(ctx context.Context, query app.ValueQuery) (app.QueryResult, error) {
			return app.QueryResult{
				Count: 1,
				Data:  [][]byte{[]byte(`{"timestamp":"2024-01-01T00:00:00Z","id":"value-1","urn":"u","v":1.2,"unit":"m","ref":"r"}`)},
			}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/things/values", nil)
	req.Header.Set("Accept", "text/csv")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, requestWithAccess(req, ReadThings))

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	if got := rr.Header().Get("Content-Type"); got != "text/csv" {
		t.Fatalf("expected text/csv content type, got %q", got)
	}
	if !strings.Contains(rr.Body.String(), "time;id;urn") {
		t.Fatalf("expected CSV header in response body, got %q", rr.Body.String())
	}
}

var _ app.ThingsApp = fakeThingsApp{}
