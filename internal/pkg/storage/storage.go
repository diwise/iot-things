package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/diwise/iot-entities/internal/pkg/presentation/auth"
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
var ErrNotExist error = fmt.Errorf("entity does not exists")

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

func (db Db) CreateEntity(ctx context.Context, v []byte) error {
	log := logging.GetFromContext(ctx)

	entity, err := unmarshalEntity(v)
	if err != nil {
		log.Error("could not unmarshal entity", "err", err.Error())
		return fmt.Errorf("could not unmarshal entity")
	}

	lat := entity.Location.Latitude
	lon := entity.Location.Longitude

	insert := `INSERT INTO entities(entity_id, entity_type, entity_location, entity_data, tenant) VALUES (@entity_id, @entity_type, point(@lon,@lat), @entity_data, @tenant);`
	_, err = db.pool.Exec(ctx, insert, pgx.NamedArgs{
		"entity_id":   entity.Id,
		"entity_type": entity.Type,
		"lon":         lon,
		"lat":         lat,
		"entity_data": string(v),
		"tenant":      entity.Tenant,
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

func (db Db) UpdateEntity(ctx context.Context, v []byte) error {
	log := logging.GetFromContext(ctx)

	entity, err := unmarshalEntity(v)
	if err != nil {
		log.Error("could not unmarshal entity", "err", err.Error())
		return fmt.Errorf("could not unmarshal entity")
	}

	lat := entity.Location.Latitude
	lon := entity.Location.Longitude

	update := `UPDATE entities SET entity_location=point(@lon,@lat), entity_data=@entity_data, modified_on=@modified_on WHERE entity_id=@entity_id;`
	_, err = db.pool.Exec(ctx, update, pgx.NamedArgs{
		"entity_id":   entity.Id,
		"modified_on": time.Now(),
		"entity_data": string(v),
		"lon":         lat,
		"lat":         lon,
	})
	if err != nil {
		log.Error("could not execute statement", "err", err.Error())
		return err
	}

	return nil
}

func where(args map[string]any) string {
	w := ""

	for k, v := range args {
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

func (db Db) QueryEntities(ctx context.Context, conditions ...ConditionFunc) ([]byte, error) {
	if len(conditions) == 0 {
		return nil, fmt.Errorf("query contains no conditions")
	}

	log := logging.GetFromContext(ctx)

	args := pgx.NamedArgs{
		"tenant": getAllowedTenantsFromContext(ctx),
	}

	for _, condition := range conditions {
		condition(args)
	}

	query := "select entity_id, entity_type, entity_location, tenant from entities " + where(args)

	rows, err := db.pool.Query(ctx, query, args)
	if err != nil {
		log.Error("could not execute query", "err", err.Error())
		return nil, err
	}

	entities := make([]entity, 0)
	var entity_id, entity_type, tenant string
	var entity_location pgtype.Point

	_, err = pgx.ForEachRow(rows, []any{&entity_id, &entity_type, &entity_location, &tenant}, func() error {
		entities = append(entities, entity{
			Id:   entity_id,
			Type: entity_type,
			Location: location{
				Latitude:  entity_location.P.Y,
				Longitude: entity_location.P.X,
			},
			Tenant: tenant,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(entities)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (db Db) RetrieveEntity(ctx context.Context, entityId string) ([]byte, string, error) {
	if entityId == "" {
		return nil, "", fmt.Errorf("no id for entity provided")
	}

	log := logging.GetFromContext(ctx)

	args := pgx.NamedArgs{
		"entity_id": entityId,
		"tenant":    getAllowedTenantsFromContext(ctx),
	}

	var entityData json.RawMessage
	var entityType string

	query := "select entity_data, entity_type from entities " + where(args)

	err := db.pool.QueryRow(ctx, query, args).Scan(&entityData, &entityType)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			log.Debug("entity does not exists", "entity_id", entityId, "err", err.Error())
			return nil, "", ErrNotExist
		}
		log.Error("could not execute query", "err", err.Error())
		return nil, "", err
	}

	return entityData, entityType, nil
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
		if errors.Is(err, ErrNotExist) {
			log.Debug("related entity does not exist, will create it", "id", related.Id)
			err := db.CreateEntity(ctx, v)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	insert := `INSERT INTO entity_relations(parent, child)
			   VALUES (
				(SELECT node_id FROM entities WHERE entity_id=@entity_id LIMIT 1), 
				(SELECT node_id FROM entities WHERE entity_id=@related_id LIMIT 1)
			   );`

	_, err = db.pool.Exec(ctx, insert, pgx.NamedArgs{
		"entity_id":  entityId,
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

func (db Db) RetrieveRelatedEntities(ctx context.Context, entityId string) ([]byte, error) {
	if entityId == "" {
		return nil, fmt.Errorf("no id for entity provided")
	}

	log := logging.GetFromContext(ctx)

	query := `
		select entity_id, entity_type, entity_location, tenant from entities where node_id IN 
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
		log.Error("could not execute query", "err", err.Error())
		return nil, err
	}

	entities := make([]entity, 0)
	var entity_id, entity_type, tenant string
	var entity_location pgtype.Point

	_, err = pgx.ForEachRow(rows, []any{&entity_id, &entity_type, &entity_location, &tenant}, func() error {
		entities = append(entities, entity{
			Id:   entity_id,
			Type: entity_type,
			Location: location{
				Latitude:  entity_location.P.Y,
				Longitude: entity_location.P.X,
			},
			Tenant: tenant,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(entities)
	if err != nil {
		return nil, err
	}

	return b, nil
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

func unmarshalEntity(v []byte) (entity, error) {
	e := struct {
		Id       *string   `json:"id,omitempty"`
		Type_    *string   `json:"type,omitempty"`
		Location *location `json:"location,omitempty"`
		Tenant   *string   `json:"tenant,omitempty"`
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
	if e.Tenant == nil {
		return entity{}, fmt.Errorf("data contains no tenant information")
	}

	entity := entity{
		Id:     *e.Id,
		Type:   *e.Type_,
		Tenant: *e.Tenant,
	}

	if e.Location != nil {
		entity.Location = *e.Location
	}

	return entity, nil
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
