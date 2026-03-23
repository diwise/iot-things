package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	app "github.com/diwise/iot-things/internal/application"
	"github.com/diwise/iot-things/internal/presentation/api/auth"
	"github.com/go-chi/chi/v5"

	"github.com/diwise/service-chassis/pkg/infrastructure/net/http/router"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"

	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("iot-things/api/things")

func RegisterHandlers(ctx context.Context, mux *http.ServeMux, app app.ThingsApp, policies io.Reader) error {
	const apiPrefix string = "/api/v0"

	log := logging.GetFromContext(ctx)

	authenticator, err := auth.NewAuthenticator(ctx, policies)
	if err != nil {
		return fmt.Errorf("failed to create api authenticator: %w", err)
	}

	r := router.New(mux, router.WithPrefix(apiPrefix))
	r.Use(authenticator)

	r.Get("/things", queryHandler(log, app))
	r.Get("/things/{id}", getByIDHandler(log, app))
	r.Post("/things", addHandler(log, app))
	r.Put("/things/{id}", updateHandler(log, app))
	r.Patch("/things/{id}", patchHandler(log, app))
	r.Delete("/things/{id}", deleteHandler(log, app))
	r.Get("/things/tags", getTagsHandler(log, app))
	r.Get("/things/types", getTypesHandler(log, app))
	r.Get("/things/values", getValuesHandler(log, app))

	return nil
}

func queryHandler(log *slog.Logger, a app.ThingsApp) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		ctx, span := tracer.Start(r.Context(), "query-things")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		w.Header().Set("Content-Type", "application/vnd.api+json")

		query, err := parseThingQuery(r.URL.Query(), auth.GetAllowedTenantsFromContext(ctx))
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

		thingId := chi.URLParam(r, "id")
		if thingId == "" {
			logger.Error("no id parameter found in request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		result, err := a.Query(ctx, app.ThingQuery{
			ID:      &thingId,
			Tenants: auth.GetAllowedTenantsFromContext(ctx),
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

			file, _, err := r.FormFile("fileupload")
			if err != nil {
				logger.Error("unable to get file from fileupload", "err", err.Error())
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			defer file.Close()

			err = a.Seed(ctx, file)
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

		tenants := auth.GetAllowedTenantsFromContext(ctx)

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

		thingId := chi.URLParam(r, "id")
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

		tenants := auth.GetAllowedTenantsFromContext(ctx)

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

		thingId := chi.URLParam(r, "id")
		if thingId == "" {
			logger.Error("no id parameter found in request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		tenants := auth.GetAllowedTenantsFromContext(ctx)

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

		tenants := auth.GetAllowedTenantsFromContext(ctx)

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

		tenants := auth.GetAllowedTenantsFromContext(ctx)

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

		query, err := parseValueQuery(r.URL.Query())
		if err != nil {
			logger.Error("invalid value query", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}

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
