package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	app "github.com/diwise/iot-things/internal/app/iot-things"
	"github.com/diwise/iot-things/internal/app/iot-things/things"
	"github.com/diwise/iot-things/internal/pkg/auth"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("iot-things/api/things")

func Register(ctx context.Context, app app.ThingsApp, policies io.Reader) (*chi.Mux, error) {
	log := logging.GetFromContext(ctx)

	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))

	authenticator, err := auth.NewAuthenticator(ctx, log, policies)
	if err != nil {
		return nil, fmt.Errorf("failed to create api authenticator: %w", err)
	}

	r.Route("/api/v0", func(r chi.Router) {
		r.Group(func(r chi.Router) {
			r.Use(authenticator)

			r.Route("/things", func(r chi.Router) {
				r.Get("/", queryHandler(log, app))
				r.Get("/{id}", getByIDHandler(log, app))
				r.Post("/", addHandler(log, app))
				r.Put("/{id}", updateHandler(log, app))
				r.Patch("/{id}", patchHandler(log, app))
				r.Delete("/{id}", deleteHandler(log, app))
				r.Get("/tags", getTagsHandler(log, app))
				r.Get("/types", getTypesHandler(log, app))
				r.Get("/values", getValuesHandler(log, app))
			})
		})
	})

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return r, nil
}

func queryHandler(log *slog.Logger, a app.ThingsApp) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		ctx, span := tracer.Start(r.Context(), "query-things")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		w.Header().Set("Content-Type", "application/vnd.api+json")

		params := r.URL.Query()
		params["tenant"] = auth.GetAllowedTenantsFromContext(ctx)

		result, err := a.QueryThings(ctx, params)
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
			err := exportQueryResultAsCSV(result, w)
			if err != nil {
				logger.Error("could not export query response as CSV", "err", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}

			w.Header().Set("Content-Type", "text/csv")
			w.WriteHeader(http.StatusOK)

			return
		}

		data := make([]map[string]any, 0, len(result.Data))
		for _, b := range result.Data {
			m := make(map[string]any)
			err = json.Unmarshal(b, &m)
			if err != nil {
				logger.Error("could not unmarshal thing", "err", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}
			mapToOutModel(m)
			data = append(data, m)
		}

		response := NewApiResponse(r, data, uint64(result.Count), uint64(result.TotalCount), uint64(result.Offset), uint64(result.Limit))

		b, err := json.Marshal(response)
		if err != nil {
			logger.Error("could not marshal query response", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		w.Header().Set("Content-Type", "application/vnd.api+json")
		w.WriteHeader(http.StatusOK)
		w.Write(b)
	}
}

func exportQueryResultAsCSV(result app.QueryResult, w io.Writer) error {
	if result.Count == 0 {
		return nil
	}

	for i, b := range result.Data {
		t, err := things.ConvToThing(b)
		if err != nil {
			return err
		}

		m := make(map[string]any)
		err = json.Unmarshal(b, &m)
		if err != nil {
			return err
		}

		if i == 0 {
			header := strings.Join([]string{"id", "type", "subType", "name", "decsription", "location", "tenant", "tags", "refDevices", "args"}, ";")
			_, err := w.Write([]byte(fmt.Sprintln(header)))
			if err != nil {
				return err
			}
		}

		asString := func(v any) string {
			if v == nil {
				return ""
			}
			return fmt.Sprintf("%v", v)
		}
		asTags := func(v any) string {
			if v == nil {
				return ""
			}
			values := v.([]any)
			tags := make([]string, len(values))
			for i, tag := range values {
				tags[i] = fmt.Sprintf("%v", tag)
			}

			return strings.Join(tags, ",")
		}
		asRefDevices := func(v any) string {
			if v == nil {
				return ""
			}
			devices := v.([]any)
			refDevices := make([]string, len(devices))
			for i, device := range devices {
				d := device.(map[string]any)
				refDevices[i] = fmt.Sprintf("%v", d["deviceID"])
			}
			return strings.Join(refDevices, ",")
		}
		asArgs := func(m map[string]any) string {
			args := []string{}

			for k, v := range m {
				if slices.Contains([]string{"maxd", "maxl", "meanl", "offset", "angle"}, k) {
					args = append(args, fmt.Sprintf("'%s':%f", k, v.(float64)))
				}
				if slices.Contains([]string{"alternativeName"}, k) {
					s := v.(string)
					if s != "" {
						args = append(args, fmt.Sprintf("'%s':'%s'", k, s))
					}
				}
			}

			if len(args) > 0 {
				j := "{" + strings.Join(args, ",") + "}"
				return j
			}

			return ""
		}

		lat, lon := t.LatLon()
		values := []string{
			t.ID(),
			t.Type(),
			asString(m["subType"]),
			asString(m["name"]),
			asString(m["description"]),
			fmt.Sprintf("%f,%f", lat, lon),
			t.Tenant(),
			asTags(m["tags"]),
			asRefDevices(m["refDevices"]),
			asArgs(m),
		}

		row := strings.Join(values, ";")

		_, err = w.Write([]byte(fmt.Sprintln(row)))
		if err != nil {
			return err
		}
	}

	return nil
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

		params := map[string][]string{"id": {thingId}}
		params["tenant"] = auth.GetAllowedTenantsFromContext(ctx)

		result, err := a.QueryThings(ctx, params)
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

		q := r.URL.Query()
		q.Set("thingid", thingId)
		values, err := a.QueryValues(ctx, q)
		if err != nil {
			logger.Debug("failed to query values", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		thing := make(map[string]any)
		err = json.Unmarshal(result.Data[0], &thing)
		if err != nil {
			logger.Debug("failed to marshal thing", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		thing["values"] = transformValues(r, values.Data)

		mapToOutModel(thing)

		response := NewApiResponse(r, thing, uint64(values.Count), uint64(values.TotalCount), uint64(values.Offset), uint64(values.Limit))

		w.WriteHeader(http.StatusOK)
		w.Write(response.Byte())
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

		err = a.AddThing(ctx, b)
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

		err = a.UpdateThing(ctx, b, tenants)
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

		err = a.MergeThing(ctx, thingId, b, tenants)
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

		err = a.DeleteThing(ctx, thingId, tenants)
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

		tags, err := a.GetTags(ctx, tenants)
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

		types, err := a.GetTypes(ctx, tenants)
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

		params := r.URL.Query()
		params["tenant"] = auth.GetAllowedTenantsFromContext(ctx)

		result, err := a.QueryValues(ctx, params)
		if err != nil {
			logger.Error("could not query for values", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		if r.Header.Get("Accept") == "text/csv" {
			err := exportValuesAsCSV(result, w)
			if err != nil {
				logger.Error("could not export values as CSV", "err", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}

			w.Header().Set("Content-Type", "text/csv")
			w.WriteHeader(http.StatusOK)

			return
		}

		if result.Count == 0 {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("[]"))
			return
		}

		data := transformValues(r, result.Data)

		response := NewApiResponse(r, data, uint64(result.Count), uint64(result.TotalCount), uint64(result.Offset), uint64(result.Limit))

		b, err := json.Marshal(response)
		if err != nil {
			logger.Error("could not marshal query response", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		w.Header().Set("Content-Type", "application/vnd.api+json")
		w.WriteHeader(http.StatusOK)
		w.Write(b)
	}
}

func exportValuesAsCSV(result app.QueryResult, w io.Writer) error {
	header := strings.Join([]string{"time", "id", "urn", "v", "vb", "vs", "unit", "ref"}, ";")

	if result.Count == 0 {
		w.Write([]byte(header))
		return nil
	}

	for i, b := range result.Data {
		m := make(map[string]any)
		err := json.Unmarshal(b, &m)
		if err != nil {
			return err
		}

		if i == 0 {
			_, err := w.Write([]byte(fmt.Sprintln(header)))
			if err != nil {
				return err
			}
		}

		str := func(v any) string {
			if v == nil {
				return ""
			}
			return fmt.Sprintf("%v", v)
		}

		values := []string{
			str(m["timestamp"]),
			str(m["id"]),
			str(m["urn"]),
			str(m["v"]),
			str(m["vb"]),
			str(m["vs"]),
			str(m["unit"]),
			str(m["ref"]),
		}

		row := strings.Join(values, ";")

		_, err = w.Write([]byte(fmt.Sprintln(row)))
		if err != nil {
			return err
		}
	}

	return nil
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

func transformValues(r *http.Request, values [][]byte) any {
	group := r.URL.Query().Get("options")

	if !slices.Contains([]string{"", "groupByID", "groupByRef"}, group) {
		group = ""
	}

	flatValues := make([]json.RawMessage, 0, len(values))
	groupedValues := map[string][]json.RawMessage{}

	for _, v := range values {
		switch group {
		case "":
			flatValues = append(flatValues, json.RawMessage(v))
		case "groupByID":
			valueID := struct {
				ID string `json:"id"`
			}{}
			err := json.Unmarshal(v, &valueID)
			if err != nil {
				continue
			}

			if _, ok := groupedValues[valueID.ID]; !ok {
				groupedValues[valueID.ID] = []json.RawMessage{}
			}

			groupedValues[valueID.ID] = append(groupedValues[valueID.ID], json.RawMessage(v))
		case "groupByRef":
			valueID := struct {
				Ref string `json:"ref"`
			}{}
			err := json.Unmarshal(v, &valueID)
			if err != nil {
				continue
			}

			if _, ok := groupedValues[valueID.Ref]; !ok {
				groupedValues[valueID.Ref] = []json.RawMessage{}
			}

			groupedValues[valueID.Ref] = append(groupedValues[valueID.Ref], json.RawMessage(v))
		}
	}

	if group == "" {
		return flatValues
	}

	return groupedValues
}
