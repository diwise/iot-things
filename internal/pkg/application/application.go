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

	"github.com/diwise/iot-entities/internal/pkg/presentation/auth"
	"github.com/diwise/iot-entities/internal/pkg/storage"
)

type App struct {
	storage Storage
}

//go:generate moq -rm -out storage_mock.go . Storage
type Storage interface {
	CreateEntity(ctx context.Context, v []byte) error
	UpdateEntity(ctx context.Context, v []byte) error
	AddRelatedEntity(ctx context.Context, entityId string, v []byte) error
	QueryEntities(ctx context.Context, conditions ...storage.ConditionFunc) ([]byte, error)
	RetrieveEntity(ctx context.Context, entityId string) ([]byte, string, error)
	RetrieveRelatedEntities(ctx context.Context, entityId string) ([]byte, error)
}

func New(s Storage) App {
	return App{
		storage: s,
	}
}

func (a App) AddRelatedEntity(ctx context.Context, entityId string, data []byte) error {
	return a.storage.AddRelatedEntity(ctx, entityId, data)
}

func (a App) RetrieveRelatedEntities(ctx context.Context, entityId string) ([]byte, error) {
	return a.storage.RetrieveRelatedEntities(ctx, entityId)
}

func (a App) QueryEntities(ctx context.Context, conditions map[string][]string) ([]byte, error) {
	q := make([]storage.ConditionFunc, 0)

	if v, ok := conditions["entity_id"]; ok {
		q = append(q, storage.EntityID(v[0]))
	}

	if v, ok := conditions["type"]; ok {
		q = append(q, storage.EntityType(v[0]))
	}

	return a.storage.QueryEntities(ctx, q...)
}

func (a App) RetrieveEntity(ctx context.Context, entityId string) ([]byte, error) {
	b, _, err := a.storage.RetrieveEntity(ctx, entityId)
	return b, err
}

var ErrAlreadyExists error = fmt.Errorf("entity already exists")

func (a App) CreateEntity(ctx context.Context, data []byte) error {
	err := a.storage.CreateEntity(ctx, data)
	if errors.Is(err, storage.ErrAlreadyExists) {
		return ErrAlreadyExists
	}
	return err
}

func (a App) IsValidEntity(data []byte) (bool, error) {
	_, _, err := unmarshalEntity(data)
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

		e := entity{
			Id:       record[0],
			Type:     record[1],
			Location: parseLocation(record[2]),
			Tenant:   record[5],
		}

		d := entity{
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

		err = a.CreateOrUpdateEntity(ctxWithTenant, be)
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

			err = a.AddRelatedEntity(ctxWithTenant, e.Id, bd)
			if err != nil {
				return err
			}
		}
		rowNum++
	}

	return nil
}

type entity struct {
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

func (a App) CreateOrUpdateEntity(ctx context.Context, data []byte) error {
	id, _, err := unmarshalEntity(data)
	if err != nil {
		return err
	}

	_, _, err = a.storage.RetrieveEntity(ctx, id)
	if err != nil {
		err := a.storage.CreateEntity(ctx, data)
		if err != nil {
			if errors.Is(err, storage.ErrAlreadyExists) {
				return ErrAlreadyExists
			}
			return err
		}
	}

	return a.storage.UpdateEntity(ctx, data)
}

func (a App) UpdateEntity(ctx context.Context, data []byte) error {
	id, _, err := unmarshalEntity(data)
	if err != nil {
		return err
	}

	_, _, err = a.storage.RetrieveEntity(ctx, id)
	if err != nil {
		return err
	}

	return a.storage.UpdateEntity(ctx, data)
}

func unmarshalEntity(data []byte) (string, string, error) {
	var d struct {
		Id   *string `json:"id,omitempty"`
		Type *string `json:"type,omitempty"`
	}
	err := json.Unmarshal(data, &d)
	if err != nil {
		return "", "", err
	}

	if d.Id == nil {
		return "", "", fmt.Errorf("data contains no entity id")
	}
	if d.Type == nil {
		return "", "", fmt.Errorf("data contains no entity type")
	}

	return *d.Id, *d.Type, nil
}
