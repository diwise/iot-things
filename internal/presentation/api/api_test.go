package api

import (
	"context"
	"encoding/json"
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

func marshalThing(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

type fakeThingsApp struct {
	queryThingsFunc func(context.Context, app.ThingQuery) (app.QueryResult, error)
	queryValuesFunc func(context.Context, app.ValueQuery) (app.QueryResult, error)
	addFunc         func(context.Context, []byte) error
	seedFunc        func(context.Context, io.Reader) error
	deleteFunc      func(context.Context, string, []string) error
	updateFunc      func(context.Context, []byte, []string) error
	mergeFunc       func(context.Context, string, []byte, []string) error
	tagsFunc        func(context.Context, []string) ([]string, error)
	typesFunc       func(context.Context, []string) ([]things.ThingType, error)
}

func (f fakeThingsApp) HandleMeasurements(ctx context.Context, tenant string, messageID string, measurements []things.Measurement) error {
	return nil
}

func (f fakeThingsApp) Add(ctx context.Context, b []byte) error {
	if f.addFunc != nil {
		return f.addFunc(ctx, b)
	}
	return nil
}
func (f fakeThingsApp) Delete(ctx context.Context, thingID string, tenants []string) error {
	if f.deleteFunc != nil {
		return f.deleteFunc(ctx, thingID, tenants)
	}
	return nil
}
func (f fakeThingsApp) Merge(ctx context.Context, thingID string, b []byte, tenants []string) error {
	if f.mergeFunc != nil {
		return f.mergeFunc(ctx, thingID, b, tenants)
	}
	return nil
}
func (f fakeThingsApp) Query(ctx context.Context, query app.ThingQuery) (app.QueryResult, error) {
	if f.queryThingsFunc != nil {
		return f.queryThingsFunc(ctx, query)
	}
	return app.QueryResult{}, nil
}
func (f fakeThingsApp) Update(ctx context.Context, b []byte, tenants []string) error {
	if f.updateFunc != nil {
		return f.updateFunc(ctx, b, tenants)
	}
	return nil
}
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
	if f.tagsFunc != nil {
		return f.tagsFunc(ctx, tenants)
	}
	return nil, nil
}
func (f fakeThingsApp) Types(ctx context.Context, tenants []string) ([]things.ThingType, error) {
	if f.typesFunc != nil {
		return f.typesFunc(ctx, tenants)
	}
	return nil, nil
}
func (f fakeThingsApp) LoadConfig(ctx context.Context, r io.Reader) error { return nil }
func (f fakeThingsApp) MigrateBindings(ctx context.Context) (int, error)  { return 0, nil }
func (f fakeThingsApp) HasUnmigratedThings(ctx context.Context) (bool, error) {
	return false, nil
}
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
	req := httptest.NewRequest(http.MethodPost, "/things", strings.NewReader(string(marshalThing(thing))))
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
	req := httptest.NewRequest(http.MethodPost, "/things", strings.NewReader(string(marshalThing(thing))))
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
					marshalThing(thing),
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
				Data:  [][]byte{marshalThing(thing)},
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

func TestGetByIDHandlerReturnsThing(t *testing.T) {
	thing := things.NewWasteContainer("thing-1", things.DefaultLocation, "default")

	h := getByIDHandler(slog.Default(), fakeThingsApp{
		queryThingsFunc: func(ctx context.Context, query app.ThingQuery) (app.QueryResult, error) {
			return app.QueryResult{Count: 1, Data: [][]byte{marshalThing(thing)}}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/things/thing-1", nil)
	req.SetPathValue("id", "thing-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, requestWithAccess(req, ReadThings))

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "thing-1") {
		t.Fatalf("expected thing id in response body, got %q", rr.Body.String())
	}
}

func TestGetByIDHandlerNotFound(t *testing.T) {
	h := getByIDHandler(slog.Default(), fakeThingsApp{
		queryThingsFunc: func(ctx context.Context, query app.ThingQuery) (app.QueryResult, error) {
			return app.QueryResult{Count: 0}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/things/missing", nil)
	req.SetPathValue("id", "missing")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, requestWithAccess(req, ReadThings))

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rr.Code)
	}
}

func TestDeleteHandlerRequiresDeleteScope(t *testing.T) {
	called := false

	h := deleteHandler(slog.Default(), fakeThingsApp{
		deleteFunc: func(ctx context.Context, id string, tenants []string) error {
			called = true
			return nil
		},
	})

	req := httptest.NewRequest(http.MethodDelete, "/things/thing-1", nil)
	req.SetPathValue("id", "thing-1")
	rr := httptest.NewRecorder()

	// Endast läs-scope: får inte radera.
	h.ServeHTTP(rr, requestWithAccess(req, ReadThings))

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected status 401 without delete scope, got %d", rr.Code)
	}
	if called {
		t.Fatal("expected application Delete not to be called without delete scope")
	}

	req = httptest.NewRequest(http.MethodDelete, "/things/thing-1", nil)
	req.SetPathValue("id", "thing-1")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, requestWithAccess(req, DeleteThings))

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200 with delete scope, got %d", rr.Code)
	}
	if !called {
		t.Fatal("expected application Delete to be called with delete scope")
	}
}

func TestGetTagsHandlerReturnsTags(t *testing.T) {
	h := getTagsHandler(slog.Default(), fakeThingsApp{
		tagsFunc: func(ctx context.Context, tenants []string) ([]string, error) {
			return []string{"tag-a", "tag-b"}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/things/tags", nil)
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, requestWithAccess(req, ReadThings))

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "tag-a") {
		t.Fatalf("expected tag in response body, got %q", rr.Body.String())
	}
}

func TestGetTypesHandlerReturnsTypes(t *testing.T) {
	h := getTypesHandler(slog.Default(), fakeThingsApp{
		typesFunc: func(ctx context.Context, tenants []string) ([]things.ThingType, error) {
			return []things.ThingType{{Type: "Room", Name: "Room"}}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/things/types", nil)
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, requestWithAccess(req, ReadThings))

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "Room") {
		t.Fatalf("expected type in response body, got %q", rr.Body.String())
	}
}

var _ app.ThingsApp = fakeThingsApp{}
