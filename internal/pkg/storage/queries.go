package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type ConditionFunc func(*Condition) *Condition

type QueryResult struct {
	Things     []byte
	Count      int
	Limit      int
	Offset     int
	TotalCount int64
}

type Condition struct {
	ID           string
	Types        []string
	ThingID      string
	Offset       int
	Limit        int
	Tenants      []string
	Measurements bool
	State        bool
	Tags         []string
}

func NewCondition(conditions ...ConditionFunc) *Condition {
	c := &Condition{
		Offset: 0,
		Limit:  100,
	}
	for _, condition := range conditions {
		condition(c)
	}
	return c
}
func (c Condition) NamedArgs() pgx.NamedArgs {
	args := pgx.NamedArgs{
		"offset": c.Offset,
		"limit":  c.Limit,
	}

	if c.ID != "" {
		args["id"] = c.ID
	}
	if c.ThingID != "" {
		args["thing_id"] = c.ThingID
	}
	if len(c.Types) > 0 {
		if len(c.Types) == 1 {
			args["type"] = c.Types[0]
		} else {
			args["type"] = c.Types
		}
	}
	if len(c.Tenants) > 0 {
		if len(c.Tenants) == 1 {
			args["tenant"] = c.Tenants[0]
		} else {
			args["tenant"] = c.Tenants
		}
	}
	if len(c.Tags) > 0 {
		b, _ := json.Marshal(c.Tags)
		args["tags"] = string(b)
	}

	if c.Offset == 0 {
		args["offset"] = 0
	}
	if c.Limit == 0 {
		args["limit"] = 100
	}

	return args
}

func (c Condition) Where() string {
	w := "where "

	if c.ID != "" {
		w += "id=@id and "
	}
	if c.ThingID != "" {
		w += "thing_id=@thing_id and "
	}
	if len(c.Types) == 1 {
		w += "type=@type and "
	}
	if len(c.Types) > 1 {
		w += "type=any(@type) and "
	}
	if len(c.Tenants) == 1 {
		w += "tenant=@tenant and "
	}
	if len(c.Tenants) > 1 {
		w += "tenant=any(@tenant) and "
	}
	if len(c.Tags) > 0 {
		w += "data ? 'tags' and data->'tags' @> (@tags) and "
	}

	return strings.TrimSuffix(w, " and ")
}
func (c Condition) Validate(fn func(c Condition) bool) error {
	if !fn(c) {
		return fmt.Errorf("condition is not valid")
	}
	return nil
}

func WithTags(tags []string) ConditionFunc {
	return func(c *Condition) *Condition {
		c.Tags = tags
		return c
	}
}

func WithID(id string) ConditionFunc {
	return func(c *Condition) *Condition {
		c.ID = strings.ToLower(id)
		return c
	}
}
func WithType(types []string) ConditionFunc {
	return func(c *Condition) *Condition {
		c.Types = []string{}
		for _, t := range types {
			c.Types = append(c.Types, strings.ToLower(t))
		}
		return c
	}
}
func WithThingID(thingID string) ConditionFunc {
	return func(c *Condition) *Condition {
		c.ThingID = strings.ToLower(thingID)
		return c
	}
}
func WithOffset(offset int) ConditionFunc {
	return func(c *Condition) *Condition {
		c.Offset = offset
		return c
	}
}
func WithLimit(limit int) ConditionFunc {
	return func(c *Condition) *Condition {
		c.Limit = limit
		return c
	}
}
func WithTenants(tenants []string) ConditionFunc {
	return func(c *Condition) *Condition {
		c.Tenants = tenants
		return c
	}
}
func WithMeasurements(withMeasurments string) ConditionFunc {
	return func(c *Condition) *Condition {
		c.Measurements = withMeasurments == "true"
		return c
	}
}
func WithState(withState string) ConditionFunc {
	return func(c *Condition) *Condition {
		c.State = withState == "true"
		return c
	}
}

type thing struct {
	ThingID  string   `json:"thing_id"`
	ID       string   `json:"id"`
	Type     string   `json:"type"`
	Location location `json:"location"`
	Tenant   string   `json:"tenant"`
	Tags     []string `json:"tags,omitempty"`
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

	c := NewCondition(conditions...)

	var rmProps string
	if !c.Measurements {
		rmProps += " - 'measurements' "
	}
	if !c.State {
		rmProps += " - 'state' "
	}

	query := fmt.Sprintf(`
			  select data %s, count(*) OVER () AS total_count 
	 		  from (
				 select data from things %s
			  ) things
			  offset @offset limit @limit;`, rmProps, c.Where())

	args := c.NamedArgs()
	rows, err := db.pool.Query(ctx, query, args)
	if err != nil {
		log.Debug("query things", slog.String("sql", query), slog.Any("args", args))
		log.Error("could not execute query", "err", err.Error())
		return QueryResult{}, err
	}

	things := make([]json.RawMessage, 0)

	var data json.RawMessage
	var total_count int64

	_, err = pgx.ForEachRow(rows, []any{&data, &total_count}, func() error {
		things = append(things, data)
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

	c := NewCondition(conditions...)

	rmProps := ""
	if !c.Measurements {
		rmProps += " - 'measurements' "
	}
	if !c.State {
		rmProps += " - 'state' "
	}

	err := c.Validate(func(c Condition) bool {
		if c.ID == "" && c.ThingID == "" {
			return false
		}
		if c.ID != "" && len(c.Types) == 0 {
			return false
		}
		return true
	})
	if err != nil {
		return nil, "", err
	}

	log := logging.GetFromContext(ctx)

	var d json.RawMessage
	var t string

	where := c.Where()
	query := fmt.Sprintf("select data %s, type from things %s", rmProps, where)

	args := c.NamedArgs()
	err = db.pool.QueryRow(ctx, query, args).Scan(&d, &t)
	if err != nil {
		log.Debug("query things", slog.String("sql", query), slog.Any("args", args))
		if errors.Is(err, pgx.ErrNoRows) {
			log.Debug("thing does not exists", "thing_id", c.ThingID, "err", err.Error())
			return nil, "", ErrNotExist
		}

		log.Error("could not execute query", "err", err.Error())
		return nil, "", err
	}

	return d, t, nil
}

func (db Db) RetrieveRelatedThings(ctx context.Context, conditions ...ConditionFunc) ([]byte, error) {
	c := NewCondition(conditions...)

	if c.ThingID == "" {
		return nil, fmt.Errorf("no id for thing provided")
	}

	log := logging.GetFromContext(ctx)

	query := `
		WITH target_node AS (
			SELECT node_id
			FROM things
			WHERE thing_id = $1
		)
		SELECT t.thing_id, t.id, t.type, t.location, t.tenant
		FROM things t
		INNER JOIN thing_relations tr 
			ON t.node_id = tr.child OR t.node_id = tr.parent
		WHERE (tr.child = (SELECT node_id FROM target_node) 
			OR tr.parent = (SELECT node_id FROM target_node))
		AND t.thing_id != $1;
	`

	rows, err := db.pool.Query(ctx, query, c.ThingID)
	if err != nil {
		log.Error("could not execute query", "err", err.Error())
		return nil, err
	}

	things := make([]thing, 0)
	var thing_id, id, thing_type, tenant string
	var thing_location pgtype.Point

	_, err = pgx.ForEachRow(rows, []any{&thing_id, &id, &thing_type, &thing_location, &tenant}, func() error {
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
		return nil, err
	}

	b, err := json.Marshal(things)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (db Db) GetTags(ctx context.Context, tenants []string) ([]string, error) {
	log := logging.GetFromContext(ctx)

	query := `
		SELECT DISTINCT tag
		FROM things,
		LATERAL jsonb_array_elements_text(data->'tags') AS tag
		WHERE data ? 'tags' AND tenant=ANY(@tenants)
		ORDER BY tag ASC;`

	args := pgx.NamedArgs{
		"tenants": tenants,
	}
	rows, err := db.pool.Query(ctx, query, args)
	if err != nil {
		log.Debug("query tags", slog.String("sql", query))
		log.Error("could not execute query", "err", err.Error())
		return []string{}, err
	}

	tags := make([]string, 0)
	var tag string

	_, err = pgx.ForEachRow(rows, []any{&tag}, func() error {
		tags = append(tags, tag)
		return nil
	})
	if err != nil {
		return []string{}, err
	}

	return tags, nil
}

func (db Db) GetTypes(ctx context.Context, tenants []string) ([]string, error) {
	log := logging.GetFromContext(ctx)

	query := `
		SELECT DISTINCT type 
		FROM things 
		WHERE type IN ('combinedsewageoverflow','wastecontainer','sewer','sewagepumpingstation','passage') AND tenant=ANY(@tenants)
		ORDER BY type ASC;`

	args := pgx.NamedArgs{
		"tenants": tenants,
	}
	rows, err := db.pool.Query(ctx, query, args)
	if err != nil {
		log.Debug("query tags", slog.String("sql", query))
		log.Error("could not execute query", "err", err.Error())
		return []string{}, err
	}

	tags := make([]string, 0)
	var tag string

	_, err = pgx.ForEachRow(rows, []any{&tag}, func() error {
		tags = append(tags, tag)
		return nil
	})
	if err != nil {
		return []string{}, err
	}

	return tags, nil
}
