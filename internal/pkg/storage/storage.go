package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"time"

	app "github.com/diwise/iot-things/internal/app/iot-things"
	"github.com/diwise/iot-things/internal/app/iot-things/things"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
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
	args := pgx.NamedArgs{
		"id":         t.ID(),
		"thing_type": t.Type(),
		"lon":        lon,
		"lat":        lat,
		"data":       string(t.Byte()),
		"tenant":     t.Tenant(),
	}

	insert := `INSERT INTO things(id, type, location, data, tenant) VALUES (@id, @thing_type, point(@lon,@lat), @data, @tenant);`

	log.Debug("AddThing", logStr("sql", insert), slog.Any("args", args))

	_, err := db.pool.Exec(ctx, insert, args)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			log.Debug("AddThing statement failed", "err", pgErr.Error(), "code", pgErr.Code, "message", pgErr.Message)
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
	args := pgx.NamedArgs{
		"id":   t.ID(),
		"lon":  lon,
		"lat":  lat,
		"data": string(t.Byte()),
	}

	update := `UPDATE things SET location=point(@lon,@lat), data=@data, modified_on=CURRENT_TIMESTAMP WHERE id=@id;`

	log.Debug("UpdateThing", logStr("sql", update), slog.Any("args", args))

	_, err := db.pool.Exec(ctx, update, args)
	if err != nil {
		log.Error("could not execute statement", "err", err.Error())
		return err
	}

	return nil
}

func (db database) DeleteThing(ctx context.Context, id string) error {
	log := logging.GetFromContext(ctx)

	delete := `UPDATE things SET deleted_on=CURRENT_TIMESTAMP WHERE id=@id;`
	_, err := db.pool.Exec(ctx, delete, pgx.NamedArgs{
		"id": id,
	})
	if err != nil {
		log.Error("could not execute statement", "err", err.Error())
		return err
	}

	return nil
}

func (db database) QueryThings(ctx context.Context, conditions ...app.ConditionFunc) (app.QueryResult, error) {
	where, args := newQueryThingsParams(conditions...)
	log := logging.GetFromContext(ctx)

	query := fmt.Sprintf("SELECT data, count(*) OVER () AS total FROM things %s", where)

	log.Debug("QueryThings", logStr("sql", query), slog.Any("args", args))

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
		Data:       t,
		Count:      len(t),
		TotalCount: total,
		Limit:      args["limit"].(int),
		Offset:     args["offset"].(int),
	}, nil
}

func (db database) QueryValues(ctx context.Context, conditions ...app.ConditionFunc) (app.QueryResult, error) {
	where, args := newQueryValuesParams(conditions...)
	log := logging.GetFromContext(ctx)

	if _, ok := args["timeunit"]; ok {
		return db.countValues(ctx, where, args)
	}

	if _, ok := args["showlatest"]; ok {
		return db.showLatest(ctx, args["thingid"].(string))
	}

	if _, ok := args["distinct"]; ok {
		return db.distinctValues(ctx, where, args)
	}

	query := fmt.Sprintf("SELECT time,id,urn,location,v,vs,vb,unit,ref, count(*) OVER () AS total FROM things_values %s ", where)

	log.Debug("QueryValues", logStr("sql", query), slog.Any("args", args))

	rows, err := db.pool.Query(ctx, query, args)
	if err != nil {
		log.Error("could not execute query", "err", err.Error())
		return app.QueryResult{}, err
	}

	var t [][]byte
	var total int64

	var ts time.Time
	var id, urn, unit, ref string
	var location pgtype.Point
	var v *float64
	var vb *bool
	var vs *string

	_, err = pgx.ForEachRow(rows, []any{&ts, &id, &urn, &location, &v, &vs, &vb, &unit, &ref, &total}, func() error {
		m := things.Value{
			Measurement: things.Measurement{
				ID:          id,
				Urn:         urn,
				BoolValue:   vb,
				StringValue: vs,
				Value:       v,
				Unit:        unit,
				Timestamp:   ts.UTC()},
			Ref: ref,
		}

		b, _ := json.Marshal(m)
		t = append(t, b)

		return nil
	})
	if err != nil {
		return app.QueryResult{}, err
	}

	return app.QueryResult{
		Data:       t,
		Count:      len(t),
		TotalCount: total,
		Limit:      args["limit"].(int),
		Offset:     args["offset"].(int),
	}, nil
}

func (db database) showLatest(ctx context.Context, thingID string) (app.QueryResult, error) {
	log := logging.GetFromContext(ctx)

	thingID = fmt.Sprintf("%s/%%", thingID)

	query := fmt.Sprintf(`
		SELECT DISTINCT ON (id) time, id, urn, v, vs, vb, unit, ref
		FROM things_values
		WHERE id LIKE '%s'
		ORDER BY id, "time" DESC;	
	`, thingID)

	log.Debug("showLatest", logStr("sql", query))

	rows, err := db.pool.Query(ctx, query)
	if err != nil {
		log.Error("could not execute query", "err", err.Error())
		return app.QueryResult{}, err
	}

	var ts time.Time
	var id, urn, unit, ref string
	var v *float64
	var vb *bool
	var vs *string

	var t [][]byte

	_, err = pgx.ForEachRow(rows, []any{&ts, &id, &urn, &v, &vs, &vb, &unit, &ref}, func() error {
		m := things.Value{
			Measurement: things.Measurement{
				ID:          id,
				Urn:         urn,
				BoolValue:   vb,
				StringValue: vs,
				Value:       v,
				Unit:        unit,
				Timestamp:   ts.UTC()},
			Ref: ref,
		}

		b, _ := json.Marshal(m)
		t = append(t, b)

		return nil
	})
	if err != nil {
		return app.QueryResult{}, err
	}

	return app.QueryResult{
		Data:       t,
		Count:      len(t),
		TotalCount: int64(len(t)),
		Limit:      0,
		Offset:     len(t),
	}, nil
}

func (db database) distinctValues(ctx context.Context, where string, args pgx.NamedArgs) (app.QueryResult, error) {
	log := logging.GetFromContext(ctx)

	distinct := args["distinct"].(string)
	offset := args["offset"].(int)
	limit := args["limit"].(int)
	offsetLimit := fmt.Sprintf("OFFSET %d LIMIT %d", offset, limit)

	query := fmt.Sprintf(`
WITH changed AS (SELECT time, id, urn, location, v, vs, vb, unit, ref, LAG(%s) OVER (ORDER BY time) AS prev_vb FROM things_values %s)
SELECT time, id, urn, location, v, vs, vb, unit, ref, COUNT(*) OVER () AS total FROM changed WHERE %s <> prev_vb OR prev_vb IS NULL %s;
`, distinct, where, distinct, offsetLimit)

	// set offset to 0 and limit to 1000 to not limit the window
	args["limit"] = 1000
	args["offset"] = 0

	log.Debug("distinctValues", logStr("sql", query), slog.Any("args", args))

	rows, err := db.pool.Query(ctx, query, args)
	if err != nil {
		log.Error("could not execute query", "err", err.Error())
		return app.QueryResult{}, err
	}

	var t [][]byte
	var total int64

	var ts time.Time
	var id, urn, unit, ref string
	var location pgtype.Point
	var v *float64
	var vb *bool
	var vs *string

	_, err = pgx.ForEachRow(rows, []any{&ts, &id, &urn, &location, &v, &vs, &vb, &unit, &ref, &total}, func() error {
		m := things.Value{
			Measurement: things.Measurement{
				ID:          id,
				Urn:         urn,
				BoolValue:   vb,
				StringValue: vs,
				Value:       v,
				Unit:        unit,
				Timestamp:   ts.UTC()},
			Ref: ref,
		}

		b, _ := json.Marshal(m)
		t = append(t, b)

		return nil
	})
	if err != nil {
		return app.QueryResult{}, err
	}

	return app.QueryResult{
		Data:       t,
		Count:      len(t),
		TotalCount: total,
		Limit:      limit,
		Offset:     offset,
	}, nil
}

func (db database) countValues(ctx context.Context, where string, args pgx.NamedArgs) (app.QueryResult, error) {
	log := logging.GetFromContext(ctx)

	timeUnit := args["timeunit"].(string)

	if !slices.Contains([]string{"hour", "day"}, timeUnit) {
		timeUnit = "hour"
	}

	query := fmt.Sprintf(`
		SELECT DATE_TRUNC('%s', time) e, id, ref, count(*) n
		FROM things_values
		%s
		GROUP BY e, id, ref 
		ORDER BY e ASC;
	`, timeUnit, where)

	log.Debug("countValues", logStr("sql", query), slog.Any("args", args))

	rows, err := db.pool.Query(ctx, query, args)
	if err != nil {
		log.Error("could not execute query", "err", err.Error())
		return app.QueryResult{}, err
	}

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
		return app.QueryResult{}, err
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

	log.Debug("GetTags", logStr("sql", query), slog.Any("args", args))

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

	args := pgx.NamedArgs{
		"time": m.Timestamp.UTC(),
		"id":   m.ID,
		"urn":  m.Urn,
		"lon":  lon,
		"lat":  lat,
		"v":    m.Value,
		"vs":   m.StringValue,
		"vb":   m.BoolValue,
		"unit": m.Unit,
		"ref":  ref,
	}

	log.Debug("AddValue", logStr("sql", insert), slog.Any("args", args))

	_, err := db.pool.Exec(ctx, insert, args)
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

var re = regexp.MustCompile(`[\t\n]`)

func logStr(k, v string) slog.Attr {
	v = re.ReplaceAllString(v, " ")
	v = strings.ReplaceAll(v, "  ", " ")
	return slog.String(k, v)
}
