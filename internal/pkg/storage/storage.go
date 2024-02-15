package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
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

var ErrAlreadyExists error = fmt.Errorf("entity already exists")

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

func logError(log *slog.Logger, err error) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		log.Error("pgx error", "code", pgErr.Code, "message", pgErr.Message)
	} else {
		log.Error("pgx error", "err", err.Error())
	}
}

func (db Db) AddRelatedEntity(ctx context.Context, entityId string, v []byte) error {
	log := logging.GetFromContext(ctx)

	related, err := unmarshalEntity(v)
	if err != nil {
		log.Error("could not unmarshal entity", "err", err.Error())
		return fmt.Errorf("could not unmarshal entity")
	}

	_, _, err = db.RetrieveEntity(ctx, entityId)
	if err != nil {
		log.Error("could not retrieve current entity", "err", err.Error())
		return fmt.Errorf("coult not retrieve current entity")
	}

	_, _, err = db.RetrieveEntity(ctx, related.Id)
	if err != nil {
		log.Debug("related entity does not exist, will create it", "id", related.Id)
		err := db.CreateEntity(ctx, v)
		if err != nil {
			return err
		}
	}

	insert := `INSERT INTO entity_relations(parent, child)
			   VALUES (
				(SELECT node_id FROM entities WHERE entity_id=$1 LIMIT 1), 
				(SELECT node_id FROM entities WHERE entity_id=$2 LIMIT 1)
			   );`

	_, err = db.pool.Exec(ctx, insert, entityId, related.Id)
	if err != nil {
		if IsDuplicateKeyErr(err) {
			return nil
		}

		logError(log, err)
	}

	return err
}

func IsDuplicateKeyErr(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "23505" { // duplicate key value violates unique constraint
			return true
		}
	}
	return false
}

func (db Db) CreateEntity(ctx context.Context, v []byte) error {
	log := logging.GetFromContext(ctx)

	entity, err := unmarshalEntity(v)
	if err != nil {
		log.Error("could not unmarshal entity", "err", err.Error())
		return fmt.Errorf("could not unmarshal entity")
	}

	lat := entity.Location.Latitude
	lon := entity.Location.Longitude

	insert := `INSERT INTO entities(entity_id, entity_type, entity_location, entity_data, tenant) VALUES ($1, $2, point($3,$4), $5, $6);`
	_, err = db.pool.Exec(ctx, insert, entity.Id, entity.Type, lon, lat, string(v), entity.Tenant)
	if err != nil {
		logError(log, err) //23505
		if IsDuplicateKeyErr(err) {
			return ErrAlreadyExists
		}
	}

	return err
}

func (db Db) UpdateEntity(ctx context.Context, v []byte) error {
	log := logging.GetFromContext(ctx)

	entity, err := unmarshalEntity(v)
	if err != nil {
		log.Error("could not unmarshal entity", "err", err.Error())
		return fmt.Errorf("could not unmarshal entity")
	}

	lat := entity.Location.Latitude
	lon := entity.Location.Longitude

	update := `UPDATE entities SET entity_location=point($1,$2), entity_data=$3, modified_on=$4 WHERE entity_id=$5;`
	_, err = db.pool.Exec(ctx, update, lon, lat, string(v), time.Now(), entity.Id)
	if err != nil {
		logError(log, err)
	}

	return err
}

func (db Db) QueryEntities(ctx context.Context, conditions ...ConditionFunc) ([]byte, error) {
	if len(conditions) == 0 {
		return nil, fmt.Errorf("query contains no conditions")
	}

	log := logging.GetFromContext(ctx)

	query := `select entity_id, entity_type, entity_location from entities `
	queryParams := newQueryParams(conditions)

	rows, err := db.pool.Query(ctx, query+queryParams.Where, queryParams.Values...)
	if err != nil {
		logError(log, err)
		return nil, err
	}
	defer rows.Close()

	entities := make([]entity, 0)

	for rows.Next() {
		var entity_id, entity_type string
		var entity_location pgtype.Point

		err := rows.Scan(&entity_id, &entity_type, &entity_location)
		if err != nil {
			logError(log, err)
			return nil, err
		}

		entities = append(entities, entity{
			Id:   entity_id,
			Type: entity_type,
			Location: location{
				Latitude:  entity_location.P.Y,
				Longitude: entity_location.P.X,
			},
		})
	}

	b, err := json.Marshal(entities)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func newQueryParams(conditions []ConditionFunc) queryParams {
	m := make(map[string]any)
	for _, f := range conditions {
		m = f(m)
	}

	where := `where`

	i := 1
	for k := range m {
		where = fmt.Sprintf("%s %s=$%d and", where, k, i)
		i++
	}
	where, _ = strings.CutSuffix(where, "and")
	k, v := keyValues(m)

	return queryParams{
		Where:  where,
		Keys:   k,
		Values: v,
	}
}

type queryParams struct {
	Where  string
	Keys   []string
	Values []any
}

func keyValues[M ~map[K]V, K comparable, V any](m M) ([]K, []V) {
	r := make([]V, 0, len(m))
	t := make([]K, 0, len(m))
	for k, v := range m {
		r = append(r, v)
		t = append(t, k)
	}
	return t, r
}

func (db Db) RetrieveEntity(ctx context.Context, entityId string) ([]byte, string, error) {
	if entityId == "" {
		return nil, "", fmt.Errorf("no id for entity provided")
	}

	log := logging.GetFromContext(ctx)

	query := `select entity_data, entity_type from entities where entity_id=$1`
	row := db.pool.QueryRow(ctx, query, entityId)

	var entityData json.RawMessage
	var entityType string

	err := row.Scan(&entityData, &entityType)
	if err != nil {
		logError(log, err)
		return nil, "", err
	}

	return entityData, entityType, nil
}

func (db Db) RetrieveRelatedEntities(ctx context.Context, entityId string) ([]byte, error) {
	if entityId == "" {
		return nil, fmt.Errorf("no id for entity provided")
	}

	log := logging.GetFromContext(ctx)

	query := `
		select entity_id, entity_type, entity_location from entities where node_id IN 
		(
			select distinct node_id from 
			(
				select child as node_id
				from entity_relations er
				join entities e on er.parent = e.node_id
				where e.entity_id=$1
				union
				select parent as node_id
				from entity_relations er
				join entities e on er.child = e.node_id
				where e.entity_id=$1
			) as related
		)`

	rows, err := db.pool.Query(ctx, query, entityId)
	if err != nil {
		logError(log, err)
		return nil, err
	}
	defer rows.Close()

	entities := make([]entity, 0)

	for rows.Next() {
		var entity_id, entity_type string
		var entity_location pgtype.Point

		err := rows.Scan(&entity_id, &entity_type, &entity_location)
		if err != nil {
			logError(log, err)
			return nil, err
		}

		entities = append(entities, entity{
			Id:   entity_id,
			Type: entity_type,
			Location: location{
				Latitude:  entity_location.P.Y,
				Longitude: entity_location.P.X,
			},
		})
	}

	b, err := json.Marshal(entities)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func unmarshalEntity(v []byte) (entity, error) {
	e := struct {
		Id       *string   `json:"id,omitempty"`
		Type_    *string   `json:"type,omitempty"`
		Location *location `json:"location,omitempty"`
	}{}

	err := json.Unmarshal(v, &e)
	if err != nil {
		return entity{}, err
	}

	if e.Id == nil {
		return entity{}, fmt.Errorf("data contains no entity id")
	}
	if e.Type_ == nil {
		return entity{}, fmt.Errorf("data contains no entity type")
	}

	entity := entity{
		Id:   *e.Id,
		Type: *e.Type_,
	}

	if e.Location != nil {
		entity.Location = *e.Location
	}

	return entity, nil
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

func initialize(ctx context.Context, pool *pgxpool.Pool) error {
	log := logging.GetFromContext(ctx)

	ddl := `
		CREATE TABLE IF NOT EXISTS entities (		
			node_id     	BIGSERIAL,	
			entity_id		TEXT 	NOT NULL UNIQUE,			
			entity_type 	TEXT 	NOT NULL,
			entity_location POINT 	NULL,
			entity_data 	JSONB	NULL,	
			tenant			TEXT 	NOT NULL,	
			created_on 		timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,			
			modified_on		timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,	
			PRIMARY KEY (node_id)
		);			
		
		CREATE INDEX IF NOT EXISTS entity_location_idx ON entities USING GIST(entity_location);
		
		CREATE TABLE IF NOT EXISTS  entity_relations (
			parent        BIGINT NOT NULL,
			child         BIGINT NOT NULL,
			PRIMARY KEY (parent, child)
		);
	`

	tx, err := pool.Begin(ctx)
	if err != nil {
		logError(log, err)
		return err
	}

	_, err = tx.Exec(ctx, ddl)
	if err != nil {
		logError(log, err)
		tx.Rollback(ctx)
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		logError(log, err)
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
