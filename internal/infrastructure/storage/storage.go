package storage

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"time"

	app "github.com/diwise/iot-things/internal/application"
	"github.com/diwise/iot-things/internal/application/things"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type database struct {
	pool *pgxpool.Pool
}

type Storage interface {
	app.ThingsReader
	app.ThingsWriter
	Close()
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

func (db database) Close() {
	db.pool.Close()
}

func initialize(ctx context.Context, pool *pgxpool.Pool) error {

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
			ref 			TEXT NULL,
			created_on  timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
			UNIQUE ("time", "id"));

		ALTER TABLE things_values ADD COLUMN IF NOT EXISTS source TEXT NULL;

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

		return fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) // Safe: ignored if tx is committed

	_, err = tx.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("could not execute ddl statement: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return nil
}

func connect(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.ConnStr())
	if err != nil {
		return nil, err
	}

	poolConfig.MaxConns = env.GetVariableOrDefaultAs(ctx, "POSTGRES_MAX_CONNS", int32(10))
	poolConfig.MinConns = env.GetVariableOrDefaultAs(ctx, "POSTGRES_MIN_CONNS", int32(2))
	poolConfig.MaxConnLifetime = env.GetVariableOrDefaultAs(ctx, "POSTGRES_MAX_CONN_LIFETIME", 30*time.Minute)
	poolConfig.MaxConnIdleTime = env.GetVariableOrDefaultAs(ctx, "POSTGRES_MAX_CONN_IDLE_TIME", 5*time.Minute)
	poolConfig.HealthCheckPeriod = env.GetVariableOrDefaultAs(ctx, "POSTGRES_HEALTH_CHECK_PERIOD", 30*time.Second)

	poolConfig.ConnConfig.RuntimeParams["application_name"] = "iot-things"

	conn, err := pgxpool.NewWithConfig(ctx, poolConfig)
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
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("could not acquire connection: %w", err)
	}
	defer conn.Release()

	lat, lon := t.LatLon()
	args := pgx.NamedArgs{
		"id":         t.ID(),
		"thing_type": t.Type(),
		"lon":        lon,
		"lat":        lat,
		"data":       string(t.Byte()),
		"tenant":     t.Tenant(),
	}

	insert := `INSERT INTO things(id, type, location, data, tenant) VALUES (@id, @thing_type, point(@lon,@lat), @data, @tenant);`

	_, err = db.pool.Exec(ctx, insert, args)
	if err != nil {
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
			return fmt.Errorf("AddThing statement failed: %w", pgErr)
		}

		if isDuplicateKeyErr(err) {
			return app.ErrAlreadyExists
		}

		return fmt.Errorf("could not add thing: %w", err)
	}

	return nil
}

func (db database) UpdateThing(ctx context.Context, t things.Thing) error {
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("could not acquire connection: %w", err)
	}
	defer conn.Release()

	lat, lon := t.LatLon()
	args := pgx.NamedArgs{
		"id":     t.ID(),
		"lon":    lon,
		"lat":    lat,
		"tenant": t.Tenant(),
		"data":   string(t.Byte()),
	}

	update := `UPDATE things SET location=point(@lon,@lat), data=@data, tenant=@tenant, modified_on=CURRENT_TIMESTAMP WHERE id=@id;`

	_, err = conn.Exec(ctx, update, args)
	if err != nil {
		return fmt.Errorf("could not update thing: %w", err)
	}

	return nil
}

func (db database) DeleteThing(ctx context.Context, id string) error {
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("could not acquire connection: %w", err)
	}
	defer conn.Release()

	delete := `UPDATE things SET deleted_on=CURRENT_TIMESTAMP WHERE id=@id;`
	_, err = conn.Exec(ctx, delete, pgx.NamedArgs{
		"id": id,
	})
	if err != nil {
		return fmt.Errorf("could not delete thing: %w", err)
	}

	return nil
}

func (db database) QueryThings(ctx context.Context, query app.ThingQuery) (app.QueryResult, error) {
	sql, args, err := buildThingQuerySQL(query)
	if err != nil {
		return app.QueryResult{}, fmt.Errorf("could not build query things sql: %w", err)
	}

	rows, err := db.pool.Query(ctx, sql, args)
	if err != nil {
		return app.QueryResult{}, fmt.Errorf("could not query things: %w", err)
	}
	defer rows.Close()

	var t [][]byte
	var total int64
	var data []byte

	_, err = pgx.ForEachRow(rows, []any{&data, &total}, func() error {
		t = append(t, data)
		return nil
	})
	if err != nil {
		return app.QueryResult{}, fmt.Errorf("could not scan rows for things query: %w", err)
	}

	offset := 0
	limit := len(t)

	if o, ok := args["offset"]; ok {
		offset = o.(int)
	}

	if l, ok := args["limit"]; ok {
		limit = l.(int)
	}

	return app.QueryResult{
		Data:       t,
		Count:      len(t),
		TotalCount: total,
		Limit:      limit,
		Offset:     offset,
	}, nil
}

func (db database) QueryValues(ctx context.Context, query app.ValueQuery) (app.QueryResult, error) {
	switch query.Mode {
	case app.ValueQueryModeCountByTime:
		return db.countValues(ctx, query)
	case app.ValueQueryModeLatest:
		return db.showLatest(ctx, query)
	case app.ValueQueryModeDistinct:
		return db.distinctValues(ctx, query)
	}

	sql, args, err := buildValueQuerySQL(query)
	if err != nil {
		return app.QueryResult{}, fmt.Errorf("could not build value query sql: %w", err)
	}

	rows, err := db.pool.Query(ctx, sql, args)
	if err != nil {
		return app.QueryResult{}, fmt.Errorf("could not execute query: %w", err)
	}
	defer rows.Close()

	var t [][]byte
	var total int64

	var ts time.Time
	var id, urn, unit, ref string
	var location pgtype.Point
	var v *float64
	var vb *bool
	var vs, src *string

	_, err = pgx.ForEachRow(rows, []any{&ts, &id, &urn, &location, &v, &vs, &vb, &unit, &ref, &src, &total}, func() error {
		m := things.Value{
			Measurement: things.Measurement{
				ID:          id,
				Urn:         urn,
				BoolValue:   vb,
				StringValue: vs,
				Value:       v,
				Unit:        unit,
				Source:      src,
				Timestamp:   ts.UTC()},
			Ref: ref,
		}

		b, _ := json.Marshal(m)
		t = append(t, b)

		return nil
	})
	if err != nil {
		return app.QueryResult{}, fmt.Errorf("could not scan rows for value query: %w", err)
	}

	return app.QueryResult{
		Data:       t,
		Count:      len(t),
		TotalCount: total,
		Limit:      args["limit"].(int),
		Offset:     args["offset"].(int),
	}, nil
}

func (db database) showLatest(ctx context.Context, query app.ValueQuery) (app.QueryResult, error) {
	sql, args, err := buildShowLatestValuesSQL(query)
	if err != nil {
		return app.QueryResult{}, fmt.Errorf("could not build show latest values sql: %w", err)
	}

	rows, err := db.pool.Query(ctx, sql, args)
	if err != nil {
		return app.QueryResult{}, fmt.Errorf("could not query latest values: %w", err)
	}
	defer rows.Close()

	var ts time.Time
	var id, urn, unit, ref string
	var v *float64
	var vb *bool
	var vs, src *string

	var t [][]byte

	_, err = pgx.ForEachRow(rows, []any{&ts, &id, &urn, &v, &vs, &vb, &unit, &ref, &src}, func() error {
		m := things.Value{
			Measurement: things.Measurement{
				ID:          id,
				Urn:         urn,
				BoolValue:   vb,
				StringValue: vs,
				Value:       v,
				Unit:        unit,
				Timestamp:   ts.UTC(),
				Source:      src},
			Ref: ref,
		}

		b, _ := json.Marshal(m)
		t = append(t, b)

		return nil
	})
	if err != nil {
		return app.QueryResult{}, fmt.Errorf("could not scan rows for latest values query: %w", err)
	}

	return app.QueryResult{
		Data:       t,
		Count:      len(t),
		TotalCount: int64(len(t)),
		Limit:      0,
		Offset:     len(t),
	}, nil
}

func (db database) distinctValues(ctx context.Context, query app.ValueQuery) (app.QueryResult, error) {
	sql, args, err := buildDistinctValuesSQL(query)
	if err != nil {
		return app.QueryResult{}, fmt.Errorf("could not build distinct values query sql: %w", err)
	}

	rows, err := db.pool.Query(ctx, sql, args)
	if err != nil {
		return app.QueryResult{}, fmt.Errorf("could not query distinct values: %w", err)
	}
	defer rows.Close()

	var t [][]byte
	var total int64

	var ts time.Time
	var id, urn, unit, ref string
	var location pgtype.Point
	var v *float64
	var vb *bool
	var vs, src *string

	_, err = pgx.ForEachRow(rows, []any{&ts, &id, &urn, &location, &v, &vs, &vb, &unit, &ref, &src, &total}, func() error {
		m := things.Value{
			Measurement: things.Measurement{
				ID:          id,
				Urn:         urn,
				BoolValue:   vb,
				StringValue: vs,
				Value:       v,
				Unit:        unit,
				Source:      src,
				Timestamp:   ts.UTC()},
			Ref: ref,
		}

		b, _ := json.Marshal(m)
		t = append(t, b)

		return nil
	})
	if err != nil {
		return app.QueryResult{}, fmt.Errorf("could not scan rows for distinct values query: %w", err)
	}

	return app.QueryResult{
		Data:       t,
		Count:      len(t),
		TotalCount: total,
		Limit:      query.Page.Limit,
		Offset:     query.Page.Offset,
	}, nil
}

func (db database) countValues(ctx context.Context, query app.ValueQuery) (app.QueryResult, error) {
	sql, args, err := buildCountValuesSQL(query)
	if err != nil {
		return app.QueryResult{}, fmt.Errorf("could not build count values sql: %w", err)
	}

	rows, err := db.pool.Query(ctx, sql, args)
	if err != nil {
		return app.QueryResult{}, fmt.Errorf("could not query count values: %w", err)
	}
	defer rows.Close()

	var t [][]byte

	var ts time.Time
	var n int64
	var id, ref string

	_, err = pgx.ForEachRow(rows, []any{&ts, &id, &ref, &n}, func() error {
		count := struct {
			ID        string    `json:"id"`
			Ref       string    `json:"ref"`
			Count     int64     `json:"count"`
			Timestamp time.Time `json:"timestamp"`
		}{
			ID:        id,
			Ref:       ref,
			Count:     n,
			Timestamp: ts.UTC(),
		}

		b, _ := json.Marshal(count)
		t = append(t, b)

		return nil
	})
	if err != nil {
		return app.QueryResult{}, fmt.Errorf("could not scan rows for count values query: %w", err)
	}

	return app.QueryResult{
		Data:       t,
		Count:      len(t),
		TotalCount: int64(len(t)),
		Limit:      len(t),
		Offset:     0,
	}, nil
}

func (db database) GetTags(ctx context.Context, tenants []string) ([]string, error) {
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return []string{}, fmt.Errorf("could not acquire connection: %w", err)
	}
	defer conn.Release()

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
		return []string{}, fmt.Errorf("could not query tags: %w", err)
	}
	defer rows.Close()

	tags := make([]string, 0)
	var tag string

	_, err = pgx.ForEachRow(rows, []any{&tag}, func() error {
		tags = append(tags, tag)
		return nil
	})
	if err != nil {
		return []string{}, fmt.Errorf("could not scan rows for tags query: %w", err)
	}

	return tags, nil
}

func (db database) AddValue(ctx context.Context, t things.Thing, m things.Value) error {
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("could not acquire connection: %w", err)
	}
	defer conn.Release()

	insert := `
		INSERT INTO things_values(time, id, urn, location, v, vs, vb, unit, ref, source)
		VALUES (@time, @id, @urn, point(@lon,@lat), @v, @vs, @vb, @unit, @ref, @source)
		ON CONFLICT (time, id) DO NOTHING;`

	lat, lon := t.LatLon()

	var ref *string
	if m.Ref != "" {
		ref = &m.Ref
	}

	args := pgx.NamedArgs{
		"time":   m.Timestamp.UTC(),
		"id":     m.ID,
		"urn":    m.Urn,
		"lon":    lon,
		"lat":    lat,
		"v":      m.Value,
		"vs":     m.StringValue,
		"vb":     m.BoolValue,
		"unit":   m.Unit,
		"ref":    ref,
		"source": m.Source,
	}

	_, err = db.pool.Exec(ctx, insert, args)
	if err != nil {
		return fmt.Errorf("could not add value: %w", err)
	}

	return nil
}

func isDuplicateKeyErr(err error) bool {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code == "23505" // duplicate key value violates unique constraint
	}
	return false
}

var re = regexp.MustCompile(`[\t\n]`)

func logStr(k, v string) slog.Attr {
	v = re.ReplaceAllString(v, " ")
	v = strings.ReplaceAll(v, "  ", " ")
	return slog.String(k, v)
}
