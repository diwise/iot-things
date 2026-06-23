package api

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"

	"github.com/diwise/iot-things/assets/docs"
	app "github.com/diwise/iot-things/internal/application"
	"github.com/diwise/iot-things/internal/application/things"
	"github.com/diwise/iot-things/internal/presentation/api/auth"

	"github.com/diwise/service-chassis/pkg/infrastructure/net/http/router"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"

	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("iot-things/api/things")

var errUnauthorizedTenant = errors.New("unauthorized tenant")

const (
	CreateThings auth.Scope = "things.create"
	ReadThings   auth.Scope = "things.read"
	UpdateThings auth.Scope = "things.update"
	DeleteThings auth.Scope = "things.delete"
)

type registerOptions struct {
	authOptions []auth.Option
}

type RegisterOption func(*registerOptions)

func WithAccessObjectAuthorization(enabled bool) RegisterOption {
	return func(o *registerOptions) {
		o.authOptions = append(o.authOptions, auth.WithAccessObjectAuthorization(enabled))
	}
}

func RegisterHandlers(ctx context.Context, mux *http.ServeMux, app app.ThingsApp, policies io.Reader, opts ...RegisterOption) error {
	const apiPrefix string = "/api/v0"

	log := logging.GetFromContext(ctx)

	docs.RegisterHandlers(ctx, mux)

	docs.RegisterHandlers(ctx, mux)

	registerOpts := registerOptions{}
	for _, apply := range opts {
		apply(&registerOpts)
	}

	authz, err := auth.NewAuthenticator(ctx, policies, registerOpts.authOptions...)

	if err != nil {
		return fmt.Errorf("failed to create api authenticator: %w", err)
	}

	r := router.New(mux, router.WithPrefix(apiPrefix))

	r.Route("things", func(r router.ServeMux) {
		r.Group(func(r router.ServeMux) {
			r.Use(authz.RequireAccess(ReadThings))
			r.Get("", queryHandler(log, app))
			r.Get("tags", getTagsHandler(log, app))
			r.Get("types", getTypesHandler(log, app))
			r.Get("values", getValuesHandler(log, app))
			r.Get("{id}", getByIDHandler(log, app))
		})

		r.Group(func(r router.ServeMux) {
			r.Use(authz.RequireAccess(CreateThings))
			r.Post("", addHandler(log, app))
		})

		r.Group(func(r router.ServeMux) {
			r.Use(authz.RequireAccess(UpdateThings))
			r.Put("{id}", updateHandler(log, app))
			r.Patch("{id}", patchHandler(log, app))
		})

		r.Group(func(r router.ServeMux) {
			r.Use(authz.RequireAccess(DeleteThings))
			r.Delete("{id}", deleteHandler(log, app))
		})
	})

	return nil
}

func queryHandler(log *slog.Logger, a app.ThingsApp) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		ctx, span := tracer.Start(r.Context(), "query-things")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		w.Header().Set("Content-Type", "application/vnd.api+json")

		tenants := auth.GetTenantsWithAllowedScopes(r.Context(), ReadThings)
		if len(tenants) == 0 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		query, err := parseThingQuery(r.URL.Query(), tenants)
		if err != nil {
			logger.Error("invalid thing query", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}

		result, err := a.Query(ctx, query)
		if err != nil {
			logger.Error("could not query things", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		if result.Count == 0 {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("[]"))
			return
		}

		if r.Header.Get("Accept") == "text/csv" {
			csv, err := presentThingQueryCSV(result)
			if err != nil {
				logger.Error("could not export query response as CSV", "err", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}

			w.Header().Set("Content-Type", "text/csv")
			w.WriteHeader(http.StatusOK)
			w.Write(csv)

			return
		}

		b, err := presentThingQueryJSON(r, result)
		if err != nil {
			logger.Error("could not render query response", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		w.Header().Set("Content-Type", "application/vnd.api+json")
		w.WriteHeader(http.StatusOK)
		w.Write(b)
	}
}

func getByIDHandler(log *slog.Logger, a app.ThingsApp) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		ctx, span := tracer.Start(r.Context(), "get-thing-byID")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		w.Header().Set("Content-Type", "application/vnd.api+json")

		thingId := r.PathValue("id")
		if thingId == "" {
			logger.Error("no id parameter found in request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		tenants := auth.GetTenantsWithAllowedScopes(r.Context(), ReadThings)
		if len(tenants) == 0 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		result, err := a.Query(ctx, app.ThingQuery{
			ID:      &thingId,
			Tenants: tenants,
			Page: app.Pagination{
				Limit:  100,
				Offset: 0,
			},
		})
		if err != nil {
			logger.Debug("failed to query things", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		if result.Count == 0 || result.Count > 1 {
			logger.Debug("thing not found", "id", thingId)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		valueQuery, err := parseValueQuery(r.URL.Query())
		if err != nil {
			logger.Error("invalid value query", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		valueQuery.ThingID = &thingId
		valueQuery.Tenants = tenants

		values, err := a.Values(ctx, valueQuery)
		if err != nil {
			logger.Debug("failed to query values", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		response, err := presentThingDetailJSON(r, result.Data[0], values)
		if err != nil {
			logger.Debug("failed to render thing detail", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(response)
	}
}

func addHandler(log *slog.Logger, a app.ThingsApp) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer r.Body.Close()

		w.Header().Set("Content-Type", "application/vnd.api+json")

		if isMultipartFormData(r) {
			ctx, span := tracer.Start(r.Context(), "seed")
			defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
			_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

			tenants := createTenantsFromRequest(r)
			if len(tenants) == 0 {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			file, _, err := r.FormFile("fileupload")
			if err != nil {
				logger.Error("unable to get file from fileupload", "err", err.Error())
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			defer file.Close()

			payload, err := io.ReadAll(file)
			if err != nil {
				logger.Error("could not read uploaded seed file", "err", err.Error())
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return
			}

			err = validateSeedTenants(bytes.NewReader(payload), tenants)
			if err != nil {
				if errors.Is(err, errUnauthorizedTenant) {
					logger.Error("seed contains unauthorized tenant", "err", err.Error())
					w.WriteHeader(http.StatusForbidden)
				} else {
					logger.Error("invalid seed file", "err", err.Error())
					w.WriteHeader(http.StatusBadRequest)
				}
				w.Write([]byte(err.Error()))
				return
			}

			err = a.Seed(ctx, bytes.NewReader(payload))
			if err != nil {
				logger.Error("could not seed", "err", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}

			w.WriteHeader(http.StatusCreated)
			return
		}

		ctx, span := tracer.Start(r.Context(), "create-thing")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		b, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Error("could not read body", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}

		tenants := createTenantsFromRequest(r)
		if len(tenants) == 0 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		t, err := things.ConvToThing(b)
		if err != nil {
			logger.Error("could not convert to thing", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}

		if !slices.Contains(tenants, t.Tenant()) {
			logger.Error("you are not allowed to create this thing")
			w.WriteHeader(http.StatusForbidden)
			return
		}

		err = a.Add(ctx, b)
		if err != nil && errors.Is(err, app.ErrAlreadyExists) {
			w.WriteHeader(http.StatusConflict)
			return
		}
		if err != nil {
			logger.Error("could not create thing", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}

func createTenantsFromRequest(r *http.Request) []string {
	return auth.GetTenantsWithAllowedScopes(r.Context(), CreateThings)
}

func validateSeedTenants(r io.Reader, tenants []string) error {
	reader := csv.NewReader(r)

	row := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to read csv row %d: %w", row+1, err)
		}

		row++
		if row == 1 {
			continue
		}

		if len(record) != 10 {
			return fmt.Errorf("invalid csv row %d: expected 10 columns, got %d", row, len(record))
		}

		if tenant := record[6]; !slices.Contains(tenants, tenant) {
			return fmt.Errorf("%w: tenant %q on row %d is not allowed", errUnauthorizedTenant, tenant, row)
		}
	}
}

func updateHandler(log *slog.Logger, a app.ThingsApp) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer r.Body.Close()

		ctx, span := tracer.Start(r.Context(), "update-thing")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		w.Header().Set("Content-Type", "application/vnd.api+json")

		b, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Error("could not read body", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		t, err := things.ConvToThing(b)
		if err != nil {
			logger.Error("could not convert to thing", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}

		thingID := r.PathValue("id")

		if thingID != t.ID() {
			logger.Error("id in path does not match id in body", "pathID", thingID, "bodyID", t.ID())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		tenants := auth.GetTenantsWithAllowedScopes(r.Context(), UpdateThings)
		if len(tenants) == 0 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if !slices.Contains(tenants, t.Tenant()) {
			logger.Error("you are not allowed to update this thing")
			w.WriteHeader(http.StatusForbidden)
			return
		}

		err = a.Update(ctx, b, tenants)
		if err != nil {
			logger.Error("could not update thing", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}

func patchHandler(log *slog.Logger, a app.ThingsApp) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer r.Body.Close()

		ctx, span := tracer.Start(r.Context(), "patch-thing")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		w.Header().Set("Content-Type", "application/vnd.api+json")

		thingId := r.PathValue("id")
		if thingId == "" {
			logger.Error("no id parameter found in request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		b, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Error("could not read body", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		tenants := auth.GetTenantsWithAllowedScopes(r.Context(), UpdateThings)
		if len(tenants) == 0 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		err = a.Merge(ctx, thingId, b, tenants)
		if err != nil {
			logger.Error("could not patch thing", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}

func deleteHandler(log *slog.Logger, a app.ThingsApp) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer r.Body.Close()

		ctx, span := tracer.Start(r.Context(), "delete-thing")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		w.Header().Set("Content-Type", "application/vnd.api+json")

		thingId := r.PathValue("id")
		if thingId == "" {
			logger.Error("no id parameter found in request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		tenants := auth.GetTenantsWithAllowedScopes(r.Context(), DeleteThings)
		if len(tenants) == 0 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		err = a.Delete(ctx, thingId, tenants)
		if err != nil {
			logger.Error("could not delete thing", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}

func getTagsHandler(log *slog.Logger, a app.ThingsApp) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		ctx, span := tracer.Start(r.Context(), "get-tags")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		w.Header().Set("Content-Type", "application/vnd.api+json")

		tenants := auth.GetTenantsWithAllowedScopes(r.Context(), ReadThings)
		if len(tenants) == 0 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		tags, err := a.Tags(ctx, tenants)
		if err != nil {
			logger.Error("could not get tags", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		response := NewApiResponse(r, tags, uint64(len(tags)), uint64(len(tags)), 0, uint64(len(tags)))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(response.Byte())
	}
}

func getTypesHandler(log *slog.Logger, a app.ThingsApp) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		ctx, span := tracer.Start(r.Context(), "get-types")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		tenants := auth.GetTenantsWithAllowedScopes(r.Context(), ReadThings)
		if len(tenants) == 0 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		types, err := a.Types(ctx, tenants)
		if err != nil {
			logger.Error("could not get tags", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		response := NewApiResponse(r, types, uint64(len(types)), uint64(len(types)), 0, uint64(len(types)))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(response.Byte())
	}
}

func getValuesHandler(log *slog.Logger, a app.ThingsApp) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		ctx, span := tracer.Start(r.Context(), "query-things-values")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		w.Header().Set("Content-Type", "application/vnd.api+json")

		tenants := auth.GetTenantsWithAllowedScopes(r.Context(), ReadThings)
		if len(tenants) == 0 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		query, err := parseValueQuery(r.URL.Query())
		if err != nil {
			logger.Error("invalid value query", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		query.Tenants = tenants

		result, err := a.Values(ctx, query)
		if err != nil {
			logger.Error("could not query for values", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		if r.Header.Get("Accept") == "text/csv" {
			csv, err := presentValueQueryCSV(result)
			if err != nil {
				logger.Error("could not export values as CSV", "err", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}

			w.Header().Set("Content-Type", "text/csv")
			w.WriteHeader(http.StatusOK)
			w.Write(csv)

			return
		}

		if result.Count == 0 {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("[]"))
			return
		}

		b, err := presentValueQueryJSON(r, result)
		if err != nil {
			logger.Error("could not render query response", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		w.Header().Set("Content-Type", "application/vnd.api+json")
		w.WriteHeader(http.StatusOK)
		w.Write(b)
	}
}

func isMultipartFormData(r *http.Request) bool {
	contentType := r.Header.Get("Content-Type")
	return strings.Contains(contentType, "multipart/form-data")
}

func mapToOutModel(m map[string]any) {
	if refDevices, ok := m["refDevices"]; ok {
		if ref, ok := refDevices.([]any); ok {
			for _, device := range ref {
				x := device.(map[string]any)
				delete(x, "measurements")
			}
			m["refDevices"] = ref
		}
	}

	// remove internal fields (i.e. fields starting with "_")
	for k := range m {
		if strings.HasPrefix(k, "_") {
			delete(m, k)
		}
	}
}
