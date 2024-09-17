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
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("iot-things")

var ErrAlreadyExists error = fmt.Errorf("Thing already exists")

type App struct {
	w ThingWriter
	r ThingReader
}

//go:generate moq -rm -out reader_mock.go . ThingReader
type ThingReader interface {
	QueryThings(ctx context.Context, conditions ...storage.ConditionFunc) (storage.QueryResult, error)
	RetrieveThing(ctx context.Context, conditions ...storage.ConditionFunc) ([]byte, string, error)
	RetrieveRelatedThings(ctx context.Context, conditions ...storage.ConditionFunc) ([]byte, error)
	GetTags(ctx context.Context, tenants []string) ([]string, error)
	GetTypes(ctx context.Context, tenants []string) ([]string, error)
}

//go:generate moq -rm -out writer_mock.go . ThingWriter
type ThingWriter interface {
	CreateThing(ctx context.Context, v []byte) error
	UpdateThing(ctx context.Context, v []byte) error
	AddRelatedThing(ctx context.Context, v []byte, conditions ...storage.ConditionFunc) error
}

func New(r ThingReader, w ThingWriter) App {
	return App{
		r: r,
		w: w,
	}
}

func (a App) AddRelatedThing(ctx context.Context, thingId string, data []byte) error {
	tenant := getAllowedTenantsFromContext(ctx)
	return a.w.AddRelatedThing(ctx, data, storage.WithThingID(thingId), storage.WithTenants(tenant))
}

func (a App) RetrieveRelatedThings(ctx context.Context, thingId string) ([]byte, error) {
	tenant := getAllowedTenantsFromContext(ctx)
	return a.r.RetrieveRelatedThings(ctx, storage.WithThingID(thingId), storage.WithTenants(tenant))
}

func parseConditions(conditions map[string][]string, add ...storage.ConditionFunc) ([]storage.ConditionFunc, error) {
	q := make([]storage.ConditionFunc, 0)

	if v, ok := conditions["id"]; ok {
		q = append(q, storage.WithID(v[0]))
	}

	if v, ok := conditions["thing_id"]; ok {
		q = append(q, storage.WithThingID(v[0]))
	}

	if v, ok := conditions["type"]; ok {
		q = append(q, storage.WithType(v))
	}

	if v, ok := conditions["measurements"]; ok {
		q = append(q, storage.WithMeasurements(v[0]))
	}

	if v, ok := conditions["state"]; ok {
		q = append(q, storage.WithState(v[0]))
	}

	if v, ok := conditions["offset"]; ok {
		offset, err := strconv.Atoi(v[0])
		if err != nil || offset < 0 {
			return nil, fmt.Errorf("invalid offset parameter")
		}
		q = append(q, storage.WithOffset(offset))
	}

	if v, ok := conditions["limit"]; ok {
		limit, err := strconv.Atoi(v[0])
		if err != nil || limit < 1 {
			return nil, fmt.Errorf("invalid limit parameter")
		}
		q = append(q, storage.WithLimit(limit))
	}

	if v, ok := conditions["tags"]; ok {
		q = append(q, storage.WithTags(v))
	}

	if len(add) > 0 {
		q = append(q, add...)
	}

	return q, nil
}

func (a App) QueryThings(ctx context.Context, conditions map[string][]string) (QueryResult, error) {
	var err error
	var number, size *int

	tenant := getAllowedTenantsFromContext(ctx)

	q, err := parseConditions(conditions, storage.WithTenants(tenant))
	if err != nil {
		return QueryResult{}, err
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

func (a App) RetrieveThing(ctx context.Context, thingId string, conditions map[string][]string) ([]byte, error) {
	tenant := getAllowedTenantsFromContext(ctx)
	q, err := parseConditions(conditions, storage.WithThingID(thingId), storage.WithTenants(tenant))
	if err != nil {
		return nil, err
	}
	b, _, err := a.r.RetrieveThing(ctx, q...)
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
	id, t, err := unmarshalThing(data)
	if err != nil {
		return err
	}

	tenant := getAllowedTenantsFromContext(ctx)
	_, _, err = a.r.RetrieveThing(ctx, storage.WithID(id), storage.WithType([]string{t}), storage.WithTenants(tenant))
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
	id, t, err := unmarshalThing(data)
	if err != nil {
		return err
	}

	_, _, err = a.r.RetrieveThing(ctx, storage.WithID(id), storage.WithType([]string{t}), storage.WithMeasurements("true"), storage.WithState("true"))
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

	thing, _, err := a.r.RetrieveThing(ctx, storage.WithThingID(thingId), storage.WithMeasurements("true"), storage.WithState("true"))
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

func (a App) GetTags(ctx context.Context) ([]string, error) {
	tenant := getAllowedTenantsFromContext(ctx)
	tags, err := a.r.GetTags(ctx, tenant)
	if err != nil {
		return nil, err
	}

	return tags, nil
}

func (a App) GetTypes(ctx context.Context) ([]string, error) {
	tenant := getAllowedTenantsFromContext(ctx)
	tags, err := a.r.GetTypes(ctx, tenant)
	if err != nil {
		return nil, err
	}

	return tags, nil
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

	parseTags := func(t string) []string {
		if t == "" {
			return []string{}
		}
		if !strings.Contains(t, ",") {
			return []string{t}
		}
		tags := strings.Split(t, ",")
		return tags
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

		//     0      1          2      3      4           5          6       7      8
		// thingId,thingType,location,props,relatedId,relatedType,location,tenant, tags

		thing := Thing{
			ThingID:  fmt.Sprintf("urn:diwise:%s:%s", record[1], record[0]),
			Id:       record[0],
			Type:     record[1],
			Location: parseLocation(record[2]),
			Tenant:   record[7],
			Tags:     parseTags(record[8]),
		}

		relatedThing := Thing{
			ThingID:  fmt.Sprintf("urn:diwise:%s:%s", record[5], record[4]),
			Id:       record[4],
			Type:     record[5],
			Location: parseLocation(record[6]),
			Tenant:   record[7],
		}

		m, err := toMap(thing)
		if err != nil {
			continue
		}
		m = appendMap(m, parseProps(record[3]))

		be, err := json.Marshal(m)
		if err != nil {
			return err
		}

		ctx := logging.NewContextWithLogger(ctx, log, slog.String("thing_id", thing.ThingID), slog.String("tenant", thing.Tenant), slog.String("related_thing_id", relatedThing.ThingID))
		ctxWithTenant := auth.WithAllowedTenants(ctx, []string{thing.Tenant})

		err = a.CreateOrUpdateThing(ctxWithTenant, be)
		if err != nil {
			log.Error("could not create or update thing", "err", err.Error())

			if !errors.Is(err, ErrAlreadyExists) {
				log.Debug("error is not ErrAlreadyExists", "err", err.Error())
				return err
			}
		}

		if relatedThing.Id != "" {
			bd, err := json.Marshal(relatedThing)
			if err != nil {
				return err
			}

			err = a.AddRelatedThing(ctxWithTenant, thing.ThingID, bd)
			if err != nil {
				log.Debug("could not add related thing")
				return err
			}
		}
		rowNum++
	}

	return nil
}

func toMap(t Thing) (map[string]any, error) {
	var err error
	b, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}

	p := make(map[string]any)
	err = json.Unmarshal(b, &p)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal thing, %w", err)
	}

	return p, nil
}

func appendMap(m1 map[string]any, m2 map[string]any) map[string]any {
	for k, v := range m2 {
		m1[k] = v
	}
	return m1
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

func getAllowedTenantsFromContext(ctx context.Context) []string {
	allowed := auth.GetAllowedTenantsFromContext(ctx)
	return allowed
}
