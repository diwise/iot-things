package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/diwise/iot-entities/internal/pkg/application"
	"github.com/diwise/iot-entities/internal/pkg/presentation/auth"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("iot-entities/api/entities")

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

			r.Route("/entities", func(r chi.Router) {
				r.Get("/", queryEntitiesHandler(log, app))
				r.Get("/{id}", retrieveEntityHandler(log, app))
				r.Post("/", createEntityHandler(log, app))
				r.Get("/{id}/related", retrieveRelatedEntitiesHandler(log, app))
				r.Post("/{id}/related", addRelatedEntityHandler(log, app))

				r.Post("/seed", seedHandler(log, app))
			})
		})
	})

	return r, nil
}

func retrieveRelatedEntitiesHandler(log *slog.Logger, app application.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		ctx, span := tracer.Start(r.Context(), "retrieve-relative-entities")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		entityId := chi.URLParam(r, "id")
		if entityId == "" {
			logger.Error("could not read body", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		b, err := app.RetrieveRelatedEntities(ctx, entityId)
		if err != nil {
			logger.Error("could not query entities", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(b)

	}
}

func addRelatedEntityHandler(log *slog.Logger, app application.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer r.Body.Close()

		ctx, span := tracer.Start(r.Context(), "add-related-entity")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		entityId := chi.URLParam(r, "id")
		if entityId == "" {
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

		if valid, err := app.IsValidEntity(b); !valid {
			logger.Error("invalid entity", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err = app.AddRelatedEntity(ctx, entityId, b)
		if err != nil {
			logger.Error("could not add related entity", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}

func queryEntitiesHandler(log *slog.Logger, app application.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		ctx, span := tracer.Start(r.Context(), "query-all-entities")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		b, err := app.QueryEntities(ctx, r.URL.Query())
		if err != nil {
			logger.Error("could not query entities", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		contentType := r.Header.Get("Accept")
		if contentType == "" {
			contentType = "application/json"
		}

		if len(b) == 0 {
			w.Header().Set("Content-Type", contentType)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("[]"))
			return
		}

		if contentType == "application/geo+json" {
			entities := []struct {
				Id       string `json:"id"`
				Type     string `json:"type"`
				Location struct {
					Latitude  float64 `json:"latitude"`
					Longitude float64 `json:"longitude"`
				} `json:"location"`
			}{}

			err = json.Unmarshal(b, &entities)
			if err != nil {
				logger.Error("could not query entities", "err", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			fc := FeatureCollection{
				Type: "FeatureCollection",
			}
			for _, e := range entities {
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
				logger.Error("could not marshal entities", "err", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}

		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(http.StatusOK)
		w.Write(b)
	}
}

func retrieveEntityHandler(log *slog.Logger, app application.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		ctx, span := tracer.Start(r.Context(), "retrieve-entity")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		entityId := chi.URLParam(r, "id")
		if entityId == "" {
			logger.Error("could not read body", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		entity, err := app.RetrieveEntity(ctx, entityId)
		if err != nil {
			logger.Info("could not find entity", "err", err.Error())
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(entity)
	}
}

func createEntityHandler(log *slog.Logger, app application.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer r.Body.Close()

		ctx, span := tracer.Start(r.Context(), "create-entity")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		b, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Error("could not read body", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if valid, err := app.IsValidEntity(b); !valid {
			logger.Error("invalid entity", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err = app.CreateEntity(ctx, b)
		if err != nil {
			logger.Error("could not create entity", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
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
