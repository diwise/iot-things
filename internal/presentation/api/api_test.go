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
	queryThingsFunc func(context.Context, map[string][]string) (app.QueryResult, error)
}

func (f fakeThingsApp) HandleMeasurements(ctx context.Context, measurements []things.Measurement) {}
func (f fakeThingsApp) AddThing(ctx context.Context, b []byte) error                              { return nil }
func (f fakeThingsApp) DeleteThing(ctx context.Context, thingID string, tenants []string) error {
	return nil
}
func (f fakeThingsApp) MergeThing(ctx context.Context, thingID string, b []byte, tenants []string) error {
	return nil
}
func (f fakeThingsApp) QueryThings(ctx context.Context, params map[string][]string) (app.QueryResult, error) {
	if f.queryThingsFunc != nil {
		return f.queryThingsFunc(ctx, params)
	}
	return app.QueryResult{}, nil
}
func (f fakeThingsApp) UpdateThing(ctx context.Context, b []byte, tenants []string) error { return nil }
func (f fakeThingsApp) AddValue(ctx context.Context, t things.Thing, m things.Value) error {
	return nil
}
func (f fakeThingsApp) QueryValues(ctx context.Context, params map[string][]string) (app.QueryResult, error) {
	return app.QueryResult{}, nil
}
func (f fakeThingsApp) GetTags(ctx context.Context, tenants []string) ([]string, error) {
	return nil, nil
}
func (f fakeThingsApp) GetTypes(ctx context.Context, tenants []string) ([]things.ThingType, error) {
	return nil, nil
}
func (f fakeThingsApp) LoadConfig(ctx context.Context, r io.Reader) error { return nil }
func (f fakeThingsApp) Seed(ctx context.Context, r io.Reader) error       { return nil }

func TestQueryHandlerCSVExportReturns500WithoutPartialCSVOnError(t *testing.T) {
	thing := things.NewWasteContainer("thing-1", things.DefaultLocation, "default")

	h := queryHandler(slog.Default(), fakeThingsApp{
		queryThingsFunc: func(ctx context.Context, params map[string][]string) (app.QueryResult, error) {
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
		queryThingsFunc: func(ctx context.Context, params map[string][]string) (app.QueryResult, error) {
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

var _ app.ThingsApp = fakeThingsApp{}
