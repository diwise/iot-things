package storage

import (
	"context"
	"errors"
	"fmt"

	app "github.com/diwise/iot-things/internal/app/iot-things"
	"github.com/diwise/iot-things/internal/app/iot-things/things"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type database struct {
	pool *pgxpool.Pool
}

type Storage interface {
	app.ThingsReader
	app.ThingsWriter
}

func New(ctx context.Context, cfg Config) (Storage, error) {
	p, err := connect(ctx, cfg)
	if err != nil {
		return database{}, err
	}

	err = initialize(ctx, p)
	if err != nil {
		return database{}, err
	}

	return database{
		pool: p,
	}, nil
}

func initialize(ctx context.Context, pool *pgxpool.Pool) error {
	log := logging.GetFromContext(ctx)

	ddl := `
		CREATE TABLE IF NOT EXISTS things (		
			id		 	TEXT 	NOT NULL,			
			type 		TEXT 	NOT NULL,
			location 	POINT 	NULL,
			data 		JSONB	NULL,	
			tenant		TEXT 	NOT NULL,	
			created_on 	timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,			
			modified_on	timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
			deleted_on 	timestamp with time zone NULL,	
			PRIMARY KEY (id)
		);			
			
		CREATE INDEX IF NOT EXISTS thing_type_idx ON things (type, id);
		CREATE INDEX IF NOT EXISTS thing_location_idx ON things USING GIST(location);

		CREATE TABLE IF NOT EXISTS things_values (
			time 		TIMESTAMPTZ NOT NULL,
			id  		TEXT NOT NULL,
			urn		  	TEXT NOT NULL,
			location 	POINT NULL,										
			v 			NUMERIC NULL,
			vs 			TEXT NULL,			
			vb 			BOOLEAN NULL,			
			unit 		TEXT NOT NULL DEFAULT '',	
			ref 		TEXT NULL,		
			created_on  timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,			
			UNIQUE ("time", "id"));


		DO $$
		DECLARE
			n INTEGER;
		BEGIN			
			SELECT COUNT(*) INTO n
			FROM timescaledb_information.hypertables
			WHERE hypertable_name = 'things_values';
			
			IF n = 0 THEN				
				PERFORM create_hypertable('things_values', 'time');				
			END IF;
		END $$;
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

func (db database) AddThing(ctx context.Context, t things.Thing) error {
	log := logging.GetFromContext(ctx)

	lat, lon := t.LatLon()

	insert := `INSERT INTO things(id, type, location, data, tenant) VALUES (@id, @thing_type, point(@lon,@lat), @data, @tenant);`
	_, err := db.pool.Exec(ctx, insert, pgx.NamedArgs{
		"id":         t.ID(),
		"thing_type": t.Type(),
		"lon":        lon,
		"lat":        lat,
		"data":       string(t.Byte()),
		"tenant":     t.Tenant(),
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			log.Debug("insert statement failed", "err", pgErr.Error(), "code", pgErr.Code, "message", pgErr.Message)
		}

		if isDuplicateKeyErr(err) {
			log.Debug("error is duplicate key")
			return app.ErrAlreadyExists
		}

		log.Error("could not execute statement", "err", err.Error())
		return err
	}

	return nil
}

func (db database) UpdateThing(ctx context.Context, t things.Thing) error {
	log := logging.GetFromContext(ctx)

	lat, lon := t.LatLon()

	update := `UPDATE things SET location=point(@lon,@lat), data=@data, modified_on=CURRENT_TIMESTAMP WHERE id=@id;`
	_, err := db.pool.Exec(ctx, update, pgx.NamedArgs{
		"id":   t.ID(),
		"lon":  lon,
		"lat":  lat,
		"data": string(t.Byte()),
	})
	if err != nil {
		log.Error("could not execute statement", "err", err.Error())
		return err
	}

	return nil
}

func (db database) QueryThings(ctx context.Context, conditions ...app.ConditionFunc) (app.QueryResult, error) {
	where, args := newQueryParams(conditions...)
	log := logging.GetFromContext(ctx)

	query := fmt.Sprintf("SELECT data, count(*) OVER () AS total FROM things %s", where)

	rows, err := db.pool.Query(ctx, query, args)
	if err != nil {
		log.Error("could not execute query", "err", err.Error())
		return app.QueryResult{}, err
	}

	var t [][]byte
	var total int64
	var data []byte

	_, err = pgx.ForEachRow(rows, []any{&data, &total}, func() error {
		t = append(t, data)
		return nil
	})
	if err != nil {
		return app.QueryResult{}, err
	}

	return app.QueryResult{
		Things:     t,
		Count:      len(t),
		TotalCount: total,
		Limit:      args["limit"].(int),
		Offset:     args["offset"].(int),
	}, nil
}

func (db database) GetTags(ctx context.Context, tenants []string) ([]string, error) {
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

func (db database) AddValue(ctx context.Context, t things.Thing, m things.Value) error {
	log := logging.GetFromContext(ctx)

	insert := `
		INSERT INTO things_values(time, id, urn, location, v, vs, vb, unit, ref)
		VALUES (@time, @id, @urn, point(@lon,@lat), @v, @vs, @vb, @unit, @ref)
		ON CONFLICT (time, id) DO NOTHING;`

	lat, lon := t.LatLon()

	var ref *string
	if m.Ref != "" {
		ref = &m.Ref
	}

	_, err := db.pool.Exec(ctx, insert, pgx.NamedArgs{
		"time": m.Timestamp,
		"id":   m.ID,
		"urn":  m.Urn,
		"lon":  lon,
		"lat":  lat,
		"v":    m.Value,
		"vs":   m.StringValue,
		"vb":   m.BoolValue,
		"unit": m.Unit,
		"ref":  ref,
	})
	if err != nil {
		log.Error("could not execute statement", "err", err.Error())
		return err
	}

	return nil
}

func isDuplicateKeyErr(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505" // duplicate key value violates unique constraint
	}
	return false
}
