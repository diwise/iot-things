package application

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/diwise/iot-things/internal/pkg/presentation/auth"
	"github.com/diwise/iot-things/internal/pkg/storage"
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

type QueryResult struct {
	Things     []byte
	Count      int
	Limit      int
	Offset     int
	Number     *int
	Size       *int
	TotalCount int64
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

func (a App) Seed(ctx context.Context, data []byte) error {
	r := csv.NewReader(bytes.NewReader(data))
	r.Comma = ';'
	rowNum := 0

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}

		if rowNum == 0 {
			rowNum++
			continue
		}

		e := Thing{
			Id:       record[0],
			Type:     record[1],
			Location: parseLocation(record[2]),
			Tenant:   record[5],
		}

		d := Thing{
			Id:       record[3],
			Type:     "Device",
			Location: parseLocation(record[4]),
			Tenant:   record[5],
		}

		be, err := json.Marshal(e)
		if err != nil {
			return err
		}

		ctxWithTenant := auth.WithAllowedTenants(ctx, []string{d.Tenant})

		err = a.CreateOrUpdateThing(ctxWithTenant, be)
		if err != nil {
			if !errors.Is(err, ErrAlreadyExists) {
				return err
			}
		}

		if d.Id != "" {
			bd, err := json.Marshal(d)
			if err != nil {
				return err
			}

			err = a.AddRelatedThing(ctxWithTenant, e.Id, bd)
			if err != nil {
				return err
			}
		}
		rowNum++
	}

	return nil
}

type Thing struct {
	Id       string   `json:"id"`
	Type     string   `json:"type"`
	Location location `json:"location"`
	Tenant   string   `json:"tenant"`
}

type location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

func parseLocation(s string) location {
	parts := strings.Split(s, ",")
	if len(parts) != 2 {
		return location{}
	}

	parse := func(s string) float64 {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0.0
		}
		return f
	}

	return location{
		Latitude:  parse(parts[0]),
		Longitude: parse(parts[1]),
	}
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
