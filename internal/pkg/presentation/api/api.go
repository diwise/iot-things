package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/diwise/iot-things/internal/pkg/application"
	"github.com/diwise/iot-things/internal/pkg/presentation/auth"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("iot-things/api/things")

func Register(ctx context.Context, app application.App, policies io.Reader) (*chi.Mux, error) {
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
				r.Get("/", queryThingsHandler(log, app))
				r.Post("/", createThingHandler(log, app))
				r.Get("/{id}", retrieveThingHandler(log, app))
				r.Put("/{id}", updateThingHandler(log, app))
				r.Get("/{id}/related", retrieveRelatedThingsHandler(log, app))
				r.Post("/{id}/related", addRelatedThingHandler(log, app))

				r.Post("/seed", seedHandler(log, app))
			})
		})
	})

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return r, nil
}

func retrieveRelatedThingsHandler(log *slog.Logger, app application.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		ctx, span := tracer.Start(r.Context(), "retrieve-relative-things")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		thingId := chi.URLParam(r, "id")
		if thingId == "" {
			logger.Error("could not read body", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		b, err := app.RetrieveRelatedThings(ctx, thingId)
		if err != nil {
			logger.Error("could not query things", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(b)

	}
}

func addRelatedThingHandler(log *slog.Logger, app application.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer r.Body.Close()

		ctx, span := tracer.Start(r.Context(), "add-related-thing")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		thingId := chi.URLParam(r, "id")
		if thingId == "" {
			logger.Error("could not read body", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		b, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Error("could not read body", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if valid, err := app.IsValidThing(b); !valid {
			logger.Error("invalid thing", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err = app.AddRelatedThing(ctx, thingId, b)
		if err != nil {
			logger.Error("could not add related thing", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}

func queryThingsHandler(log *slog.Logger, app application.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		ctx, span := tracer.Start(r.Context(), "query-all-things")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		b, err := app.QueryThings(ctx, r.URL.Query())
		if err != nil {
			logger.Error("could not query things", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		contentType := r.Header.Get("Accept")

		if !(contentType == "application/json" || contentType == "application/geo+json") {
			contentType = "application/json"
		}

		if len(b) == 0 {
			w.Header().Set("Content-Type", contentType)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("[]"))
			return
		}

		if contentType == "application/geo+json" {
			things := []struct {
				Id       string `json:"id"`
				Type     string `json:"type"`
				Location struct {
					Latitude  float64 `json:"latitude"`
					Longitude float64 `json:"longitude"`
				} `json:"location"`
			}{}

			err = json.Unmarshal(b, &things)
			if err != nil {
				logger.Error("could not query things", "err", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			fc := FeatureCollection{
				Type: "FeatureCollection",
			}
			for _, e := range things {
				fc.Features = append(fc.Features, Feature{
					ID:   e.Id,
					Type: "Feature",
					Geometry: Geometry{
						Type:        "Point",
						Coordinates: []float64{e.Location.Longitude, e.Location.Latitude},
					},
					Properties: map[string]any{
						"type": e.Type,
					},
				})
			}

			b, err = json.Marshal(fc)
			if err != nil {
				logger.Error("could not marshal things", "err", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}

		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(http.StatusOK)
		w.Write(b)
	}
}

func retrieveThingHandler(log *slog.Logger, app application.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		ctx, span := tracer.Start(r.Context(), "retrieve-thing")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		thingId := chi.URLParam(r, "id")
		if thingId == "" {
			logger.Error("could not read body", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		thing, err := app.RetrieveThing(ctx, thingId)
		if err != nil {
			logger.Info("could not find thjing", "err", err.Error())
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(thing)
	}
}

func createThingHandler(log *slog.Logger, app application.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer r.Body.Close()

		ctx, span := tracer.Start(r.Context(), "create-thing")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		b, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Error("could not read body", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if valid, err := app.IsValidThing(b); !valid {
			logger.Error("invalid thing", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err = app.CreateThing(ctx, b)
		if err != nil && errors.Is(err, application.ErrAlreadyExists) {
			logger.Info("thing already exists")
			w.WriteHeader(http.StatusConflict)
			return
		}
		if err != nil {
			logger.Error("could not create thing", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}

func updateThingHandler(log *slog.Logger, app application.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer r.Body.Close()

		ctx, span := tracer.Start(r.Context(), "update-thing")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		b, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Error("could not read body", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if valid, err := app.IsValidThing(b); !valid {
			logger.Error("invalid thing", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err = app.UpdateThing(ctx, b)
		if err != nil {
			logger.Error("could not update thing", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}

func seedHandler(log *slog.Logger, app application.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		ctx, span := tracer.Start(r.Context(), "seed")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		if !isMultipartFormData(r) {
			logger.Error("could not read body", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		file, _, err := r.FormFile("fileupload")
		if err != nil {
			logger.Error("unable to read file", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		defer file.Close()

		b, err := io.ReadAll(file)
		if err != nil {
			logger.Error("unable to read file upload", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = app.Seed(ctx, b)
		if err != nil {
			logger.Error("could not seed", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}

func isMultipartFormData(r *http.Request) bool {
	contentType := r.Header.Get("Content-Type")
	return strings.Contains(contentType, "multipart/form-data")
}

type FeatureCollection struct {
	Type     string    `json:"type"`
	Features []Feature `json:"features"`
}
type Feature struct {
	ID         string   `json:"id"`
	Type       string   `json:"type"`
	Geometry   Geometry `json:"geometry"`
	Properties map[string]any
}
type Geometry struct {
	Type        string    `json:"type"`
	Coordinates []float64 `json:"coordinates"`
}
