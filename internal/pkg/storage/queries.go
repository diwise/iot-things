package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/diwise/iot-things/internal/pkg/presentation/auth"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type ConditionFunc func(map[string]any) map[string]any

type QueryResult struct {
	Things     []byte
	Count      int
	Limit      int
	Offset     int
	TotalCount int64
}

func WithID(id string) ConditionFunc {
	return func(q map[string]any) map[string]any {
		q["id"] = strings.ToLower(id)
		return q
	}
}
func WithType(t string) ConditionFunc {
	return func(q map[string]any) map[string]any {
		q["type"] = strings.ToLower(t)
		return q
	}
}

func WithThingID(id string) ConditionFunc {
	return func(q map[string]any) map[string]any {
		q["thing_id"] = strings.ToLower(id)
		return q
	}
}

func WithOffset(v int) ConditionFunc {
	return func(q map[string]any) map[string]any {
		q["offset"] = v
		return q
	}
}

func WithLimit(v int) ConditionFunc {
	return func(q map[string]any) map[string]any {
		q["limit"] = v
		return q
	}
}

type thing struct {
	ThingID  string   `json:"thing_id"`
	ID       string   `json:"id"`
	Type     string   `json:"type"`
	Location location `json:"location"`
	Tenant   string   `json:"tenant"`
}

type location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

func (db Db) QueryThings(ctx context.Context, conditions ...ConditionFunc) (QueryResult, error) {
	if len(conditions) == 0 {
		return QueryResult{}, fmt.Errorf("query contains no conditions")
	}

	log := logging.GetFromContext(ctx)

	args := pgx.NamedArgs{
		"tenant": getAllowedTenantsFromContext(ctx),
	}

	for _, condition := range conditions {
		condition(args)
	}

	if _, ok := args["offset"]; !ok {
		args["offset"] = 0
	}

	if _, ok := args["limit"]; !ok {
		args["limit"] = 1000
	}

	query := fmt.Sprintf(`
			  select thing_id, id, type, location, tenant, count(*) OVER () AS total_count 
	 		  from (
				 select thing_id, id, type, location, tenant
				 from things
				 %s
			  ) things
			  limit @limit
			  offset @offset;`, where(args))

	rows, err := db.pool.Query(ctx, query, args)
	if err != nil {
		log.Debug("query things", slog.String("sql", query), slog.Any("args", args))
		log.Error("could not execute query", "err", err.Error())
		return QueryResult{}, err
	}

	things := make([]thing, 0)
	var thing_id, id, thing_type, tenant string
	var thing_location pgtype.Point
	var total_count int64

	_, err = pgx.ForEachRow(rows, []any{&thing_id, &id, &thing_type, &thing_location, &tenant, &total_count}, func() error {
		things = append(things, thing{
			ThingID: thing_id,
			ID:      id,
			Type:    thing_type,
			Location: location{
				Latitude:  thing_location.P.Y,
				Longitude: thing_location.P.X,
			},
			Tenant: tenant,
		})

		return nil
	})
	if err != nil {
		return QueryResult{}, err
	}

	b, err := json.Marshal(things)
	if err != nil {
		return QueryResult{}, err
	}

	result := QueryResult{
		Things:     b,
		Count:      len(things),
		Limit:      args["limit"].(int),
		Offset:     args["offset"].(int),
		TotalCount: total_count,
	}

	return result, nil
}

func (db Db) RetrieveThing(ctx context.Context, conditions ...ConditionFunc) ([]byte, string, error) {

	if len(conditions) == 0 {
		return nil, "", fmt.Errorf("query contains no conditions")
	}

	args := pgx.NamedArgs{
		"tenant": getAllowedTenantsFromContext(ctx),
	}

	for _, condition := range conditions {
		condition(args)
	}

	var thingId, id, thingType string

	if _, ok := args["thing_id"]; ok {
		if s, ok := args["thing_id"].(string); ok {
			thingId = s
		}
	}
	if _, ok := args["id"]; ok {
		if s, ok := args["id"].(string); ok {
			id = s
		}
	}
	if _, ok := args["type"]; ok {
		if s, ok := args["type"].(string); ok {
			thingType = s
		}
	}

	if thingId == "" && id == "" && thingType == "" {
		return nil, "", fmt.Errorf("no id for thing provided")
	}

	if id != "" && thingType == "" {
		return nil, "", fmt.Errorf("id provided but not type")
	}

	log := logging.GetFromContext(ctx)

	var d json.RawMessage
	var t string

	query := "select data, type from things " + where(args)

	err := db.pool.QueryRow(ctx, query, args).Scan(&d, &t)
	if err != nil {
		log.Debug("query things", slog.String("sql", query), slog.Any("args", args))
		if errors.Is(err, pgx.ErrNoRows) {
			log.Debug("thing does not exists", "thing_id", thingId, "err", err.Error())
			return nil, "", ErrNotExist
		}

		log.Error("could not execute query", "err", err.Error())
		return nil, "", err
	}

	return d, t, nil
}

func (db Db) RetrieveRelatedThings(ctx context.Context, thingId string) ([]byte, error) {
	if thingId == "" {
		return nil, fmt.Errorf("no id for thing provided")
	}

	log := logging.GetFromContext(ctx)

	query := `
		select thing_id, id, type, location, tenant from things where node_id IN 
		(
			select distinct node_id from 
			(
				select child as node_id
				from thing_relations er
				join things e on er.parent = e.node_id
				where e.thing_id=$1
				union
				select parent as node_id
				from thing_relations er
				join things e on er.child = e.node_id
				where e.thing_id=$1
			) as related
		)`

	rows, err := db.pool.Query(ctx, query, thingId)
	if err != nil {
		log.Error("could not execute query", "err", err.Error())
		return nil, err
	}

	things := make([]thing, 0)
	var thing_id, id, thing_type, tenant string
	var thing_location pgtype.Point

	_, err = pgx.ForEachRow(rows, []any{&thing_id, &id, &thing_type, &thing_location, &tenant}, func() error {
		things = append(things, thing{
			ThingID: thingId,
			ID:      id,
			Type:    thing_type,
			Location: location{
				Latitude:  thing_location.P.Y,
				Longitude: thing_location.P.X,
			},
			Tenant: tenant,
		})

		return nil
	})
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(things)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func where(args map[string]any) string {
	w := ""

	for k, v := range args {
		if k == "offset" || k == "limit" {
			continue
		}

		if w != "" {
			w += "and "
		}

		if _, ok := v.(string); ok {
			w += fmt.Sprintf("%s=@%s ", k, k)
		}

		if _, ok := v.([]string); ok {
			w += fmt.Sprintf("%s=any(@%s) ", k, k)
		}
	}

	if w == "" {
		return w
	}

	return "where " + w
}

func getAllowedTenantsFromContext(ctx context.Context) []string {
	allowed := auth.GetAllowedTenantsFromContext(ctx)
	return allowed
}
