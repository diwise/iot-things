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
				r.Patch("/{id}", patchThingHandler(log, app))
				r.Post("/{id}", addRelatedThingHandler(log, app))
			})
		})
	})

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return r, nil
}

func queryThingsHandler(log *slog.Logger, app application.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		ctx, span := tracer.Start(r.Context(), "query-things")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		result, err := app.QueryThings(ctx, r.URL.Query())
		if err != nil {
			logger.Error("could not query things", "err", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		accept := r.Header.Get("Accept")
		contentType := "application/vnd.api+json"

		if accept == "application/geo+json" || accept == "application/json" {
			contentType = accept
		}

		if result.Count == 0 {
			w.Header().Set("Content-Type", contentType)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("[]"))
			return
		}

		b := result.Things

		if contentType == "application/geo+json" {
			things := []struct {
				Id       string `json:"id"`
				Type     string `json:"type"`
				Location struct {
					Latitude  float64 `json:"latitude"`
					Longitude float64 `json:"longitude"`
				} `json:"location"`
			}{}

			err = json.Unmarshal(result.Things, &things)
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

			links := L(result, r)
			if links != nil {
				w.Header().Add("Link", fmt.Sprintf(`<%s>; rel="self"; type="application/geo+json"`, links.Self))
				if links.Next != nil {
					w.Header().Add("Link", fmt.Sprintf(`<%s>; rel="next"; type="application/geo+json"`, *links.Next))
				}
				if links.Prev != nil {
					w.Header().Add("Link", fmt.Sprintf(`<%s>; rel="prev"; type="application/geo+json"`, *links.Prev))
				}
			}
		}

		if contentType == "application/vnd.api+json" {
			response := JsonApiResponse{
				Data:  result.Things,
				Links: L(result, r),
			}

			b, err = json.Marshal(response)
			if err != nil {
				logger.Error("could not marshal query response", "err", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}

		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(http.StatusOK)
		w.Write(b)
	}
}

func L(result application.QueryResult, r *http.Request) *Links {
	var prev *string
	var next *string

	q := r.URL.Query()
	delete(q, "page[number]")
	delete(q, "page[size]")
	delete(q, "offset")
	delete(q, "limit")

	url := fmt.Sprintf("%s?%s", r.URL.Path, q.Encode())

	if result.Size != nil && result.Number != nil {
		lastPage := result.TotalCount / int64(*result.Size)
		if *result.Number+1 <= int(lastPage) {
			n := fmt.Sprintf("%s&page[number]=%d&page[size]=%d", url, *result.Number+1, *result.Size)
			next = &n
		}

		if *result.Number-1 >= 1 {
			n := fmt.Sprintf("%s&page[number]=%d&page[size]=%d", url, *result.Number-1, *result.Size)
			prev = &n
		}

		return &Links{
			Self:  fmt.Sprintf("%s&page[number]=%d&page[size]=%d", url, *result.Number, *result.Size),
			First: fmt.Sprintf("%s&page[number]=%d&page[size]=%d", url, 1, *result.Size),
			Last:  fmt.Sprintf("%s&page[number]=%d&page[size]=%d", url, lastPage, *result.Size),
			Prev:  prev,
			Next:  next,
		}
	}

	if result.Offset-result.Limit >= 0 {
		n := fmt.Sprintf("%s&offset=%d&limit=%d", url, result.Offset-result.Limit, result.Limit)
		prev = &n
	}

	if !(int64(result.Offset)+int64(result.Limit) > result.TotalCount) {
		n := fmt.Sprintf("%s&offset=%d&limit=%d", url, result.Offset+result.Limit, result.Limit)
		next = &n
	}

	var last int64 = 1

	for {
		if last*int64(result.Limit) >= result.TotalCount {
			break
		}
		last++
	}

	return &Links{
		Self:  fmt.Sprintf("%s&offset=%d&limit=%d", url, result.Offset, result.Limit),
		First: fmt.Sprintf("%s&offset=%d&limit=%d", url, 0, result.Limit),
		Last:  fmt.Sprintf("%s&offset=%d&limit=%d", url, (last-1)*int64(result.Limit), result.Limit),
		Prev:  prev,
		Next:  next,
	}
}

func createThingHandler(log *slog.Logger, app application.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer r.Body.Close()

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

			err = app.Seed(ctx, file)
			if err != nil {
				logger.Error("could not seed", "err", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
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

func retrieveThingHandler(log *slog.Logger, app application.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		ctx, span := tracer.Start(r.Context(), "retrieve-thing")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		thingId := chi.URLParam(r, "id")
		if thingId == "" {
			logger.Error("no id parameter found in request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		accept := r.Header.Get("Accept")
		contentType := "application/vnd.api+json"

		if accept == "application/json" {
			contentType = accept
		}

		b, err := app.RetrieveThing(ctx, thingId)
		if err != nil {
			logger.Info("could not find thing", "err", err.Error())
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if contentType == "application/vnd.api+json" {
			response := JsonApiResponse{
				Data: b,
			}

			r, err := app.RetrieveRelatedThings(ctx, thingId)
			if err != nil {
				logger.Error("could not fetch related things", "err", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			related := []Resource{}
			err = json.Unmarshal(r, &related)
			if err != nil {
				logger.Error("could not marshal query response", "err", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if len(related) > 0 {
				response.Included = related
			}

			b, err = json.Marshal(response)
			if err != nil {
				logger.Error("could not marshal query response", "err", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}

		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(http.StatusOK)
		w.Write(b)
	}
}

func patchThingHandler(log *slog.Logger, app application.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer r.Body.Close()

		ctx, span := tracer.Start(r.Context(), "patch-thing")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

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
			return
		}

		err = app.PatchThing(ctx, thingId, b)
		if err != nil {
			logger.Error("could not patch thing", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
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

func addRelatedThingHandler(log *slog.Logger, app application.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer r.Body.Close()

		ctx, span := tracer.Start(r.Context(), "add-related-thing")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		thingId := chi.URLParam(r, "id")
		if thingId == "" {
			logger.Error("no id parameter found in request")
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

func isMultipartFormData(r *http.Request) bool {
	contentType := r.Header.Get("Content-Type")
	return strings.Contains(contentType, "multipart/form-data")
}
