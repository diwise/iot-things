package application

import (
	"context"
	"encoding/json"
	"fmt"
)

type App struct {
	storage Storage
}

type Storage interface {
	CreateEntity(ctx context.Context, v []byte) error
	AddRelatedEntity(ctx context.Context, entityId string, v []byte) error
	QueryEntities(ctx context.Context, conditions map[string]any) ([]byte, error)
	RetrieveEntity(ctx context.Context, entityId string) ([]byte, string, error)
	RetrieveRelatedEntities(ctx context.Context, entityId string) ([]byte, error)
}

func New(s Storage) App {
	return App{
		storage: s,
	}
}

type ConditionFunc func(map[string]any) map[string]any

func EntityType(t string) ConditionFunc {
	return func(q map[string]any) map[string]any {
		q["entity_type"] = t
		return q
	}
}

func EntityID(id string) ConditionFunc {
	return func(q map[string]any) map[string]any {
		q["entity_id"] = id
		return q
	}
}

func (a App) AddRelatedEntity(ctx context.Context, entityId string, data []byte) error {
	return a.storage.AddRelatedEntity(ctx, entityId, data)
}

func (a App) RetrieveRelatedEntities(ctx context.Context, entityId string) ([]byte, error) {

	return a.storage.RetrieveRelatedEntities(ctx, entityId)
}

func (a App) QueryEntities(ctx context.Context, conditions ...ConditionFunc) ([]byte, error) {
	q := make(map[string]any)

	for _, cond := range conditions {
		cond(q)
	}

	return a.storage.QueryEntities(ctx, q)
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
	return nil
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
