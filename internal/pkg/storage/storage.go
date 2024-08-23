package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

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

func initialize(ctx context.Context, pool *pgxpool.Pool) error {
	log := logging.GetFromContext(ctx)

	ddl := `
	CREATE TABLE IF NOT EXISTS things (		
		node_id     BIGSERIAL,	
		thing_id	TEXT	NOT NULL,
		id		 	TEXT 	NOT NULL,			
		type 		TEXT 	NOT NULL,
		location 	POINT 	NULL,
		data 		JSONB	NULL,	
		tenant		TEXT 	NOT NULL,	
		created_on 	timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,			
		modified_on	timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,	
		PRIMARY KEY (node_id)
	);			
	
	CREATE UNIQUE INDEX IF NOT EXISTS thing_id_idx ON things (lower(thing_id));
	CREATE INDEX IF NOT EXISTS thing_type_idx ON things (type, id);

	CREATE INDEX IF NOT EXISTS thing_location_idx ON things USING GIST(location);
	
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

func (db Db) CreateThing(ctx context.Context, v []byte) error {
	log := logging.GetFromContext(ctx)

	thing, err := unmarshalThing(v)
	if err != nil {
		log.Error("could not unmarshal thing", "err", err.Error())
		return fmt.Errorf("could not unmarshal thing")
	}

	lat, lon, _ := thing.Location()	

	insert := `INSERT INTO things(thing_id, id, type, location, data, tenant) VALUES (@thing_id, @id, @thing_type, point(@lon,@lat), @thing_data, @tenant);`
	_, err = db.pool.Exec(ctx, insert, pgx.NamedArgs{
		"thing_id":   thing.ThingID(),
		"id":         thing.ID(),
		"thing_type": thing.Type(),
		"lon":        lon,
		"lat":        lat,
		"thing_data": thing.Data(),
		"tenant":     thing.Tenant(),
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			log.Debug("insert statement failed", "err", pgErr.Error(), "code", pgErr.Code, "message", pgErr.Message)
		}

		if isDuplicateKeyErr(err) {
			log.Debug("error is duplicate key")
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

	lat, lon, _ := thing.Location()

	update := `UPDATE things SET location=point(@lon,@lat), data=@thing_data, modified_on=CURRENT_TIMESTAMP WHERE thing_id=@thing_id;`
	_, err = db.pool.Exec(ctx, update, pgx.NamedArgs{
		"thing_id":   thing.ThingID(),
		"thing_data": thing.Data(),
		"lon":        lon,
		"lat":        lat,
	})
	if err != nil {
		log.Error("could not execute statement", "err", err.Error())
		return err
	}

	return nil
}

func (db Db) AddRelatedThing(ctx context.Context, thingId string, v []byte) error {
	log := logging.GetFromContext(ctx)

	related, err := unmarshalThing(v)
	if err != nil {
		log.Error("could not unmarshal thing", "err", err.Error())
		return fmt.Errorf("could not unmarshal thing")
	}

	_, _, err = db.RetrieveThing(ctx, WithThingID(thingId))
	if err != nil {
		log.Error("could not retrieve current thing", "err", err.Error())
		return fmt.Errorf("could not retrieve current thing")
	}

	_, _, err = db.RetrieveThing(ctx, WithThingID(related.ThingID()))
	if err != nil {
		if !errors.Is(err, ErrNotExist) {
			return err
		}

		log.Debug("related thing does not exist, will create it", "id", related.ID())
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
		"related_id": related.ThingID(),
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

func isDuplicateKeyErr(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "23505" { // duplicate key value violates unique constraint
			return true
		}
	}
	return false
}

func unmarshalThing(v []byte) (thingMap, error) {
	t := thingMap{}

	err := json.Unmarshal(v, &t)
	if err != nil {
		return nil, err
	}

	if t.ID() == "" {
		return nil, fmt.Errorf("data contains no thing id")
	}
	if t.Type() == "" {
		return nil, fmt.Errorf("data contains no thing type")
	}
	if t.Tenant() == "" {
		return nil, fmt.Errorf("data contains no tenant information")
	}
	if t.ThingID() == ""  {
		t["thing_id"] = fmt.Sprintf("urn:diwise:%s:%s", t.Type(), t.ID())
	}

	return t, nil
}
