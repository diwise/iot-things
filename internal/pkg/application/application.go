package application

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"

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

func (a App) CreateEntity(ctx context.Context, data []byte) error {
	return a.storage.CreateEntity(ctx, data)
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

		rec := struct {
			Entity struct {
				Id       string
				Type     string
				Location []string
			}
			Device struct {
				Id       string
				Type     string
				Location []string
			}
		}{
			Entity: struct {
				Id       string
				Type     string
				Location []string
			}{
				Id:       record[0],
				Type:     record[1],
				Location: strings.Split(record[2], ","),
			},
			Device: struct {
				Id       string
				Type     string
				Location []string
			}{
				Id:       record[3],
				Type:     "Device",
				Location: strings.Split(record[4], ","),
			},
		}

		e, err := json.Marshal(rec.Entity)
		if err != nil {
			return err
		}

		d, err := json.Marshal(rec.Device)
		if err != nil {
			return err
		}

		err = a.CreateOrUpdate(ctx, e)
		if err != nil {
			return err
		}

		err = a.AddRelatedEntity(ctx, rec.Entity.Id, d)
		if err != nil {
			return err
		}

		rowNum++
	}

	return nil
}

func (a App) CreateOrUpdate(ctx context.Context, data []byte) error {
	id, _, err := unmarshalEntity(data)
	if err != nil {
		return err
	}

	_, _, err = a.storage.RetrieveEntity(ctx, id)
	if err != nil {
		return a.storage.CreateEntity(ctx, data)
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
