package application

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"

	"github.com/diwise/iot-things/internal/pkg/presentation/auth"
	"github.com/diwise/iot-things/internal/pkg/storage"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

var ErrAlreadyExists error = fmt.Errorf("Thing already exists")

type App struct {
	w ThingWriter
	r ThingReader
}

//go:generate moq -rm -out reader_mock.go . ThingReader
type ThingReader interface {
	QueryThings(ctx context.Context, conditions ...storage.ConditionFunc) (storage.QueryResult, error)
	RetrieveThing(ctx context.Context, thingId string) ([]byte, string, error)
	RetrieveRelatedThings(ctx context.Context, thingId string) ([]byte, error)
}

//go:generate moq -rm -out writer_mock.go . ThingWriter
type ThingWriter interface {
	CreateThing(ctx context.Context, v []byte) error
	UpdateThing(ctx context.Context, v []byte) error
	AddRelatedThing(ctx context.Context, thingId string, v []byte) error
}

func New(r ThingReader, w ThingWriter) App {
	return App{
		r: r,
		w: w,
	}
}

func (a App) AddRelatedThing(ctx context.Context, thingId string, data []byte) error {
	return a.w.AddRelatedThing(ctx, thingId, data)
}

func (a App) RetrieveRelatedThings(ctx context.Context, thingId string) ([]byte, error) {
	return a.r.RetrieveRelatedThings(ctx, thingId)
}

func (a App) QueryThings(ctx context.Context, conditions map[string][]string) (QueryResult, error) {
	var err error
	var number, size *int

	q := make([]storage.ConditionFunc, 0)

	if v, ok := conditions["thing_id"]; ok {
		q = append(q, storage.WithThingID(v[0]))
	}

	if v, ok := conditions["type"]; ok {
		q = append(q, storage.WithThingType(v[0]))
	}

	if v, ok := conditions["page[size]"]; ok {
		s, err := strconv.Atoi(v[0])
		if err != nil || s < 1 {
			return QueryResult{}, fmt.Errorf("invalid size parameter")
		}
		q = append(q, storage.WithLimit(s))
		size = &s
	}

	if v, ok := conditions["page[number]"]; ok {
		n, err := strconv.Atoi(v[0])
		if err != nil || n < 1 {
			return QueryResult{}, fmt.Errorf("invalid number parameter")
		}
		if size == nil {
			s := 10
			size = &s
			q = append(q, storage.WithLimit(s))
		}
		q = append(q, storage.WithOffset((n-1)**size))
		number = &n
	}

	if v, ok := conditions["offset"]; ok {
		offset, err := strconv.Atoi(v[0])
		if err != nil || offset < 0 {
			return QueryResult{}, fmt.Errorf("invalid offset parameter")
		}
		q = append(q, storage.WithOffset(offset))
	}

	if v, ok := conditions["limit"]; ok {
		limit, err := strconv.Atoi(v[0])
		if err != nil || limit < 1 {
			return QueryResult{}, fmt.Errorf("invalid limit parameter")
		}
		q = append(q, storage.WithLimit(limit))
	}

	r, err := a.r.QueryThings(ctx, q...)

	return QueryResult{
		Things:     r.Things,
		Count:      r.Count,
		Limit:      r.Limit,
		Offset:     r.Offset,
		Number:     number,
		Size:       size,
		TotalCount: r.TotalCount,
	}, err
}

func (a App) RetrieveThing(ctx context.Context, thingId string) ([]byte, error) {
	b, _, err := a.r.RetrieveThing(ctx, thingId)
	return b, err
}

func (a App) CreateThing(ctx context.Context, data []byte) error {
	err := a.w.CreateThing(ctx, data)
	if errors.Is(err, storage.ErrAlreadyExists) {
		return ErrAlreadyExists
	}
	return err
}

func (a App) IsValidThing(data []byte) (bool, error) {
	_, _, err := unmarshalThing(data)
	return err == nil, err
}

func (a App) CreateOrUpdateThing(ctx context.Context, data []byte) error {
	id, _, err := unmarshalThing(data)
	if err != nil {
		return err
	}

	_, _, err = a.r.RetrieveThing(ctx, id)
	if err != nil {
		if !errors.Is(err, storage.ErrNotExist) {
			return err
		}

		err := a.w.CreateThing(ctx, data)
		if err != nil {
			if errors.Is(err, storage.ErrAlreadyExists) {
				return ErrAlreadyExists
			}
			return err
		}
	}

	return a.w.UpdateThing(ctx, data)
}

func (a App) UpdateThing(ctx context.Context, data []byte) error {
	id, _, err := unmarshalThing(data)
	if err != nil {
		return err
	}

	_, _, err = a.r.RetrieveThing(ctx, id)
	if err != nil {
		return err
	}

	return a.w.UpdateThing(ctx, data)
}

func (a App) PatchThing(ctx context.Context, thingId string, patch []byte) error {
	var err error

	p := make(map[string]any)
	err = json.Unmarshal(patch, &p)
	if err != nil {
		return fmt.Errorf("could not unmarshal patch, %w", err)
	}

	thing, _, err := a.r.RetrieveThing(ctx, thingId)
	if err != nil {
		return fmt.Errorf("could not find thing to patch, %w", err)
	}

	current := make(map[string]any)
	err = json.Unmarshal(thing, &current)
	if err != nil {
		return fmt.Errorf("could not unmarshal current thing to map, %w", err)
	}

	for k, v := range p {
		current[k] = v
	}

	patchedThing, err := json.Marshal(current)
	if err != nil {
		return fmt.Errorf("could not marshal patched thing, %w", err)
	}

	return a.w.UpdateThing(ctx, patchedThing)
}

func (a App) Seed(ctx context.Context, data io.Reader) error {
	r := csv.NewReader(data)
	r.Comma = ';'
	rowNum := 0

	log := logging.GetFromContext(ctx)

	parseLocation := func(s string) Location {
		parts := strings.Split(s, ",")
		if len(parts) != 2 {
			return Location{}
		}

		parse := func(s string) float64 {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return 0.0
			}
			return f
		}

		return Location{
			Latitude:  parse(parts[0]),
			Longitude: parse(parts[1]),
		}
	}

	parseProps := func(p string) map[string]any {
		parts := strings.Split(p, ",")
		if len(parts) == 0 {
			return map[string]any{}
		}

		m := make(map[string]any)

		for _, part := range parts {
			keyValues := strings.Split(part, "=")
			if len(keyValues) != 2 {
				continue
			}

			key := keyValues[0]
			value := keyValues[1]

			if strings.HasPrefix(value, "'") {
				m[key] = strings.Trim(value, "'")
				continue
			}

			f, err := strconv.ParseFloat(value, 64)
			if err != nil {
				continue
			}

			m[key] = f
		}

		return m
	}

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}

		if rowNum == 0 {
			rowNum++
			continue
		}

		//     0      1          2      3      4           5          6       7
		// thingId,thingType,location,props,relatedId,relatedType,location,tenant

		t := Thing{
			Id:       record[0],
			Type:     record[1],
			Location: parseLocation(record[2]),
			Tenant:   record[7],
		}

		fnct := Thing{
			Id:       record[4],
			Type:     record[5],
			Location: parseLocation(record[6]),
			Tenant:   record[7],
		}

		be, err := json.Marshal(t)
		if err != nil {
			return err
		}

		l := log.With(slog.String("thing_id", t.Id), slog.String("tenant", t.Tenant), slog.String("fnct_id", fnct.Id))
		ctx := logging.NewContextWithLogger(ctx, l)
		ctxWithTenant := auth.WithAllowedTenants(ctx, []string{t.Tenant})

		l.Debug("seed")

		err = a.CreateOrUpdateThing(ctxWithTenant, be)
		if err != nil {
			log.Error("could not create or update thing", "err", err.Error())

			if !errors.Is(err, ErrAlreadyExists) {
				log.Debug("error is not ErrAlreadyExists", "err", err.Error())
				return err
			}
		}

		props := parseProps(record[3])
		if len(props) > 0 {
			if b, err := json.Marshal(props); err == nil {
				err := a.PatchThing(ctx, t.Id, b)
				if err != nil {
					log.Error("patch thing failed", "err", err.Error())
				}
			}
		}

		if fnct.Id != "" {
			bd, err := json.Marshal(fnct)
			if err != nil {
				return err
			}

			err = a.AddRelatedThing(ctxWithTenant, t.Id, bd)
			if err != nil {
				log.Debug("could not add related thing")
				return err
			}
		}
		rowNum++
	}

	return nil
}

func unmarshalThing(data []byte) (string, string, error) {
	var d struct {
		Id   *string `json:"id,omitempty"`
		Type *string `json:"type,omitempty"`
	}
	err := json.Unmarshal(data, &d)
	if err != nil {
		return "", "", err
	}

	if d.Id == nil {
		return "", "", fmt.Errorf("data contains no Thing id")
	}
	if d.Type == nil {
		return "", "", fmt.Errorf("data contains no Thing type")
	}

	return *d.Id, *d.Type, nil
}
