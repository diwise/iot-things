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
)

type fakeThingsApp struct {
	queryThingsFunc func(context.Context, app.ThingQuery) (app.QueryResult, error)
	queryValuesFunc func(context.Context, app.ValueQuery) (app.QueryResult, error)
}

func (f fakeThingsApp) HandleMeasurements(ctx context.Context, measurements []things.Measurement) {}
func (f fakeThingsApp) Add(ctx context.Context, b []byte) error                                   { return nil }
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
func (f fakeThingsApp) Seed(ctx context.Context, r io.Reader) error       { return nil }

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

	h.ServeHTTP(rr, req)

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

	h.ServeHTTP(rr, req)

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

	h.ServeHTTP(rr, req)

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

	h.ServeHTTP(rr, req)

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

	h.ServeHTTP(rr, req)

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

	h.ServeHTTP(rr, req)

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
