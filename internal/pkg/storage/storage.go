package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/diwise/iot-things/internal/pkg/presentation/auth"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	host     string
	user     string
	password string
	port     string
	dbname   string
	sslmode  string
}

func LoadConfiguration(ctx context.Context) Config {
	return Config{
		host:     env.GetVariableOrDefault(ctx, "POSTGRES_HOST", ""),
		user:     env.GetVariableOrDefault(ctx, "POSTGRES_USER", ""),
		password: env.GetVariableOrDefault(ctx, "POSTGRES_PASSWORD", ""),
		port:     env.GetVariableOrDefault(ctx, "POSTGRES_PORT", "5432"),
		dbname:   env.GetVariableOrDefault(ctx, "POSTGRES_DBNAME", "diwise"),
		sslmode:  env.GetVariableOrDefault(ctx, "POSTGRES_SSLMODE", "disable"),
	}
}

func (c Config) ConnStr() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", c.user, c.password, c.host, c.port, c.dbname, c.sslmode)
}

type ConditionFunc func(map[string]any) map[string]any

type QueryResult struct {
	Things     []byte
	Count      int
	Limit      int
	Offset     int
	TotalCount int64
}

func WithThingType(t string) ConditionFunc {
	return func(q map[string]any) map[string]any {
		q["thing_type"] = t
		return q
	}
}

func WithThingID(id string) ConditionFunc {
	return func(q map[string]any) map[string]any {
		q["thing_id"] = id
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

var ErrAlreadyExists error = fmt.Errorf("thing already exists")
var ErrNotExist error = fmt.Errorf("thing does not exists")

type thing struct {
	Id       string   `json:"id"`
	Type     string   `json:"type"`
	Location location `json:"location"`
	Tenant   string   `json:"tenant"`
}

type location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type Db struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, cfg Config) (Db, error) {
	p, err := connect(ctx, cfg)
	if err != nil {
		return Db{}, err
	}

	err = initialize(ctx, p)
	if err != nil {
		return Db{}, err
	}

	return Db{
		pool: p,
	}, nil
}

func (db Db) CreateThing(ctx context.Context, v []byte) error {
	log := logging.GetFromContext(ctx)

	thing, err := unmarshalThing(v)
	if err != nil {
		log.Error("could not unmarshal thing", "err", err.Error())
		return fmt.Errorf("could not unmarshal thing")
	}

	lat := thing.Location.Latitude
	lon := thing.Location.Longitude

	insert := `INSERT INTO things(thing_id, thing_type, thing_location, thing_data, tenant) VALUES (@thing_id, @thing_type, point(@lon,@lat), @thing_data, @tenant);`
	_, err = db.pool.Exec(ctx, insert, pgx.NamedArgs{
		"thing_id":   thing.Id,
		"thing_type": thing.Type,
		"lon":        lon,
		"lat":        lat,
		"thing_data": string(v),
		"tenant":     thing.Tenant,
	})
	if err != nil {
		if isDuplicateKeyErr(err) {
			return ErrAlreadyExists
		}

		log.Error("could not execute statement", "err", err.Error())
		return err
	}

	return nil
}

func (db Db) UpdateThing(ctx context.Context, v []byte) error {
	log := logging.GetFromContext(ctx)

	thing, err := unmarshalThing(v)
	if err != nil {
		log.Error("could not unmarshal thing", "err", err.Error())
		return fmt.Errorf("could not unmarshal thing")
	}

	lat := thing.Location.Latitude
	lon := thing.Location.Longitude

	update := `UPDATE things SET thing_location=point(@lon,@lat), thing_data=@thing_data, modified_on=@modified_on WHERE thing_id=@thing_id;`
	_, err = db.pool.Exec(ctx, update, pgx.NamedArgs{
		"thing_id":    thing.Id,
		"modified_on": time.Now(),
		"thing_data":  string(v),
		"lon":         lat,
		"lat":         lon,
	})
	if err != nil {
		log.Error("could not execute statement", "err", err.Error())
		return err
	}

	return nil
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
			  select thing_id, thing_type, thing_location, tenant, count(*) OVER () AS total_count 
	 		  from (
				 select thing_id, thing_type, thing_location, tenant
				 from things
				 %s
			  ) things
			  limit @limit
			  offset @offset;`, where(args))

	rows, err := db.pool.Query(ctx, query, args)
	if err != nil {
		log.Error("could not execute query", "err", err.Error())
		return QueryResult{}, err
	}

	things := make([]thing, 0)
	var thing_id, thing_type, tenant string
	var thing_location pgtype.Point
	var total_count int64

	_, err = pgx.ForEachRow(rows, []any{&thing_id, &thing_type, &thing_location, &tenant, &total_count}, func() error {
		things = append(things, thing{
			Id:   thing_id,
			Type: thing_type,
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

func (db Db) RetrieveThing(ctx context.Context, thingId string) ([]byte, string, error) {
	if thingId == "" {
		return nil, "", fmt.Errorf("no id for thing provided")
	}

	log := logging.GetFromContext(ctx)

	args := pgx.NamedArgs{
		"thing_id": thingId,
		"tenant":   getAllowedTenantsFromContext(ctx),
	}

	var thingData json.RawMessage
	var thingType string

	query := "select thing_data, thing_type from things " + where(args)

	err := db.pool.QueryRow(ctx, query, args).Scan(&thingData, &thingType)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			log.Debug("thing does not exists", "thing_id", thingId, "err", err.Error())
			return nil, "", ErrNotExist
		}

		log.Error("could not execute query", "err", err.Error())
		return nil, "", err
	}

	return thingData, thingType, nil
}

func (db Db) AddRelatedThing(ctx context.Context, thingId string, v []byte) error {
	log := logging.GetFromContext(ctx)

	related, err := unmarshalThing(v)
	if err != nil {
		log.Error("could not unmarshal thing", "err", err.Error())
		return fmt.Errorf("could not unmarshal thing")
	}

	_, _, err = db.RetrieveThing(ctx, thingId)
	if err != nil {
		log.Error("could not retrieve current thing", "err", err.Error())
		return fmt.Errorf("coult not retrieve current thing")
	}

	_, _, err = db.RetrieveThing(ctx, related.Id)
	if err != nil {
		if !errors.Is(err, ErrNotExist) {
			return err
		}

		log.Debug("related thing does not exist, will create it", "id", related.Id)
		err := db.CreateThing(ctx, v)
		if err != nil {
			return err
		}
	}

	insert := `INSERT INTO thing_relations(parent, child)
			   VALUES (
				(SELECT node_id FROM things WHERE thing_id=@thing_id LIMIT 1), 
				(SELECT node_id FROM things WHERE thing_id=@related_id LIMIT 1)
			   );`

	_, err = db.pool.Exec(ctx, insert, pgx.NamedArgs{
		"thing_id":   thingId,
		"related_id": related.Id,
	})
	if err != nil {
		if isDuplicateKeyErr(err) {
			return nil
		}

		log.Error("could not execute statement", "err", err.Error())
		return err
	}

	return nil
}

func (db Db) RetrieveRelatedThings(ctx context.Context, thingId string) ([]byte, error) {
	if thingId == "" {
		return nil, fmt.Errorf("no id for thing provided")
	}

	log := logging.GetFromContext(ctx)

	query := `
		select thing_id, thing_type, thing_location, tenant from things where node_id IN 
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
	var thing_id, thing_type, tenant string
	var thing_location pgtype.Point

	_, err = pgx.ForEachRow(rows, []any{&thing_id, &thing_type, &thing_location, &tenant}, func() error {
		things = append(things, thing{
			Id:   thing_id,
			Type: thing_type,
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

func isDuplicateKeyErr(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "23505" { // duplicate key value violates unique constraint
			return true
		}
	}
	return false
}

func unmarshalThing(v []byte) (thing, error) {
	t := struct {
		Id       *string   `json:"id,omitempty"`
		Type_    *string   `json:"type,omitempty"`
		Location *location `json:"location,omitempty"`
		Tenant   *string   `json:"tenant,omitempty"`
	}{}

	err := json.Unmarshal(v, &t)
	if err != nil {
		return thing{}, err
	}

	if t.Id == nil {
		return thing{}, fmt.Errorf("data contains no thing id")
	}
	if t.Type_ == nil {
		return thing{}, fmt.Errorf("data contains no thing type")
	}
	if t.Tenant == nil {
		return thing{}, fmt.Errorf("data contains no tenant information")
	}

	thing := thing{
		Id:     *t.Id,
		Type:   *t.Type_,
		Tenant: *t.Tenant,
	}

	if t.Location != nil {
		thing.Location = *t.Location
	}

	return thing, nil
}

func initialize(ctx context.Context, pool *pgxpool.Pool) error {
	log := logging.GetFromContext(ctx)

	ddl := `
		CREATE TABLE IF NOT EXISTS things (		
			node_id     	BIGSERIAL,	
			thing_id		TEXT 	NOT NULL UNIQUE,			
			thing_type 		TEXT 	NOT NULL,
			thing_location 	POINT 	NULL,
			thing_data 		JSONB	NULL,	
			tenant			TEXT 	NOT NULL,	
			created_on 		timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,			
			modified_on		timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,	
			PRIMARY KEY (node_id)
		);			
		
		CREATE INDEX IF NOT EXISTS thing_location_idx ON things USING GIST(thing_location);
		
		CREATE TABLE IF NOT EXISTS  thing_relations (
			parent        BIGINT NOT NULL,
			child         BIGINT NOT NULL,
			PRIMARY KEY (parent, child)
		);
	`

	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Error("could not begin transaction", "err", err.Error())
		return err
	}

	_, err = tx.Exec(ctx, ddl)
	if err != nil {
		log.Error("could not execute ddl statement", "err", err.Error())
		tx.Rollback(ctx)
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Error("could not commit transaction", "err", err.Error())
		return err
	}

	return nil
}

func connect(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	conn, err := pgxpool.New(ctx, cfg.ConnStr())
	if err != nil {
		return nil, err
	}

	err = conn.Ping(ctx)
	if err != nil {
		return nil, err
	}

	return conn, err
}
