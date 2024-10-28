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

	query := "WHERE deleted_on IS NULL"
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

	if subType, ok := c["subtype"]; ok {
		query += " AND data->>'subType'=@sub_type"
		args["sub_type"] = subType
	}

	if tags, ok := c["tags"]; ok {
		query += " AND data ? 'tags' and data->'tags' @> (@tags)"
		b, _ := json.Marshal(tags)
		args["tags"] = string(b)
	}

	if refDevice, ok := c["refdevice"]; ok {
		query += fmt.Sprintf(` AND data ? 'refDevices' AND data->'refDevices' @> '[{"deviceID": "%s"}]'`, refDevice)
	}

	query += " ORDER BY type ASC, data->>'subType' ASC, data->>'name' ASC"

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

	if id, ok := c["id"]; ok {
		query += " AND id=@id"
		args["id"] = id
	}

	if thingID, ok := c["thingid"]; ok {
		query += fmt.Sprintf(" AND id LIKE '%s/%%'", thingID)
	}

	if urn, ok := c["urn"]; ok {
		query += " AND urn=ANY(@urn)"
		args["urn"] = urn
	}

	if timerel, ok := c["timerel"]; ok {
		switch timerel {
		case "before":
			query += " AND time < @ts"
			args["ts"] = c["timeat"]
		case "after":
			query += " AND time > @ts"
			args["ts"] = c["timeat"]
		case "between":
			query += " AND time > @ts1 AND time < @ts2"
			args["ts1"] = c["timeat"]
			args["ts2"] = c["endtimeat"]
		}
	}

	if v, ok := c["value"]; ok {
		op, opOk := c["operator"]
		if opOk {
			switch op {
			case "eq":
				query += " AND v IS NOT NULL AND v=@v"
				args["v"] = v
			case "gt":
				query += " AND v IS NOT NULL AND v>@v"
				args["v"] = v
			case "lt":
				query += " AND v IS NOT NULL AND v<@v"
				args["v"] = v
			case "ne":
				query += " AND v IS NOT NULL AND v<>@v"
				args["v"] = v
			}
		}
	}

	if vb, ok := c["vb"]; ok {
		query += " AND vb IS NOT NULL AND vb=@vb"
		args["vb"] = vb
	}

	if ref, ok := c["refdevice"]; ok {
		query += " AND ref=@ref"
		args["ref"] = ref
	}

	if n, ok := c["n"]; ok {
		query += fmt.Sprintf(" AND id LIKE '%%/%s'", n)
	}

	// if timeunit is present, we are counting rows gouped by timeunit (hour, day)
	if timeunit, ok := c["timeunit"]; ok {
		args["timeunit"] = timeunit
	} else {
		query += " ORDER BY time ASC"

		if offset, ok := c["offset"]; ok {
			query += " OFFSET @offset"
			args["offset"] = offset
		}

		if limit, ok := c["limit"]; ok {
			query += " LIMIT @limit"
			args["limit"] = limit
		}
	}

	return query, args
}
