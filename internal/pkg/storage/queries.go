package storage

import (
	"encoding/json"
	"fmt"

	app "github.com/diwise/iot-things/internal/app/iot-things"
	"github.com/jackc/pgx/v5"
)

func newConditions(conditions ...app.ConditionFunc) map[string]any {
	m := make(map[string]any)

	for _, f := range conditions {
		m = f(m)
	}

	if _, ok := m["limit"]; !ok {
		m["limit"] = 100
	}

	if _, ok := m["offset"]; !ok {
		m["offset"] = 0
	}

	return m
}

func newQueryThingsParams(conditions ...app.ConditionFunc) (string, pgx.NamedArgs) {
	c := newConditions(conditions...)

	query := "WHERE 1=1"
	args := pgx.NamedArgs{}

	if id, ok := c["id"]; ok {
		query += " AND id=@id"
		args["id"] = id
	}

	if tenants, ok := c["tenants"]; ok {
		query += " AND tenant=ANY(@tenants)"
		args["tenants"] = tenants
	}

	if types, ok := c["types"]; ok {
		query += " AND type=ANY(@types)"
		args["types"] = types
	}

	if subType, ok := c["sub_type"]; ok {
		query += " AND data->>'sub_type'=@sub_type"
		args["sub_type"] = subType
	}

	if tags, ok := c["tags"]; ok {
		query += " AND data ? 'tags' and data->'tags' @> (@tags)"
		b, _ := json.Marshal(tags)
		args["tags"] = string(b)
	}

	if refDevice, ok := c["ref_device"]; ok {
		query += fmt.Sprintf(` AND data->'ref_devices' @> '[{"device_id": "%s"}]'`, refDevice)
	}

	if offset, ok := c["offset"]; ok {
		query += " OFFSET @offset"
		args["offset"] = offset
	}

	if limit, ok := c["limit"]; ok {
		query += " LIMIT @limit"
		args["limit"] = limit
	}

	return query, args
}

func newQueryValuesParams(conditions ...app.ConditionFunc) (string, pgx.NamedArgs) {
	c := newConditions(conditions...)

	query := "WHERE 1=1"
	args := pgx.NamedArgs{}


	if offset, ok := c["offset"]; ok {
		query += " OFFSET @offset"
		args["offset"] = offset
	}

	if limit, ok := c["limit"]; ok {
		query += " LIMIT @limit"
		args["limit"] = limit
	}

	return query, args
}
