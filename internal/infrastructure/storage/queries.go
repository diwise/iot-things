package storage

import (
	"encoding/json"
	"fmt"
	"strings"

	app "github.com/diwise/iot-things/internal/application"
	"github.com/jackc/pgx/v5"
)

var allowedThingNumericFields = map[string]string{
	"angle":  "angle",
	"maxd":   "maxd",
	"maxl":   "maxl",
	"meanl":  "meanl",
	"offset": "offset",
}

var allowedDistinctFields = map[string]string{
	"v":  "v",
	"vb": "vb",
}

func buildThingQuerySQL(query app.ThingQuery) (string, pgx.NamedArgs, error) {
	b := newSQLBuilder()
	b.Where("deleted_on IS NULL")

	if query.ID != nil {
		b.Where("id = " + b.Bind("id", *query.ID))
	}
	if len(query.Tenants) > 0 {
		b.Where("tenant = ANY(" + b.Bind("tenants", query.Tenants) + ")")
	}
	if len(query.Types) > 0 {
		b.Where("type = ANY(" + b.Bind("types", query.Types) + ")")
	}
	if query.SubType != nil {
		b.Where("data->>'subType' = " + b.Bind("sub_type", *query.SubType))
	}
	if len(query.Tags) > 0 {
		tagsJSON, err := json.Marshal(query.Tags)
		if err != nil {
			return "", nil, err
		}
		b.Where("data ? 'tags' AND data->'tags' @> CAST(" + b.Bind("tags", string(tagsJSON)) + " AS jsonb)")
	}
	if query.RefDeviceID != nil {
		refDevicesJSON, err := json.Marshal([]map[string]string{{"deviceID": *query.RefDeviceID}})
		if err != nil {
			return "", nil, err
		}
		b.Where("data ? 'refDevices' AND data->'refDevices' @> CAST(" + b.Bind("refdevice_filter", string(refDevicesJSON)) + " AS jsonb)")
	}

	for index, filter := range query.NumericFilters {
		field, ok := allowedThingNumericFields[filter.Field]
		if !ok {
			return "", nil, fmt.Errorf("unsupported thing numeric filter: %s", filter.Field)
		}

		op, err := sqlCompareOperator(filter.Op)
		if err != nil {
			return "", nil, err
		}

		paramName := fmt.Sprintf("thing_field_%d", index)
		b.Where(fmt.Sprintf("data ? '%s' AND (data->>'%s')::numeric %s %s", field, field, op, b.Bind(paramName, filter.Value)))
	}

	b.OrderBy("type ASC")
	b.OrderBy("data->>'subType' ASC")
	b.OrderBy("data->>'name' ASC")

	querySQL := strings.TrimSpace(strings.Join([]string{
		"SELECT data, count(*) OVER () AS total FROM things",
		b.WhereClause(),
		b.OrderByClause(),
	}, " "))

	if !query.Page.Export {
		querySQL += " OFFSET " + b.Bind("offset", query.Page.Offset)
		querySQL += " LIMIT " + b.Bind("limit", query.Page.Limit)
	}

	return querySQL, b.args, nil
}

func buildValueQuerySQL(query app.ValueQuery) (string, pgx.NamedArgs, error) {
	b, err := buildValueFilterBuilder(query)
	if err != nil {
		return "", nil, err
	}

	b.OrderBy("time ASC")

	querySQL := strings.TrimSpace(strings.Join([]string{
		"SELECT time,id,urn,location,v,vs,vb,unit,ref,source, count(*) OVER () AS total FROM things_values",
		b.WhereClause(),
		b.OrderByClause(),
	}, " "))

	querySQL += " OFFSET " + b.Bind("offset", query.Page.Offset)
	querySQL += " LIMIT " + b.Bind("limit", query.Page.Limit)

	return querySQL, b.args, nil
}

func buildShowLatestValuesSQL(query app.ValueQuery) (string, pgx.NamedArgs, error) {
	if query.ThingID == nil {
		return "", nil, fmt.Errorf("thingid is required for latest values query")
	}

	b := newSQLBuilder()
	b.Where("id LIKE " + b.Bind("thingid_pattern", fmt.Sprintf("%s/%%", *query.ThingID)))

	querySQL := strings.TrimSpace(strings.Join([]string{
		"SELECT DISTINCT ON (id) time, id, urn, v, vs, vb, unit, ref, source FROM things_values",
		b.WhereClause(),
		`ORDER BY id, "time" DESC`,
	}, " "))

	return querySQL, b.args, nil
}

func buildDistinctValuesSQL(query app.ValueQuery) (string, pgx.NamedArgs, error) {
	b, err := buildValueFilterBuilder(query)
	if err != nil {
		return "", nil, err
	}

	if query.DistinctBy == nil {
		return "", nil, fmt.Errorf("distinct field is required")
	}

	field, ok := allowedDistinctFields[*query.DistinctBy]
	if !ok {
		return "", nil, fmt.Errorf("unsupported distinct field: %s", *query.DistinctBy)
	}

	args := b.args
	args["distinct_offset"] = query.Page.Offset
	args["distinct_limit"] = query.Page.Limit

	querySQL := fmt.Sprintf(`
WITH changed AS (
	SELECT time, id, urn, location, v, vs, vb, unit, ref, source, LAG(%s) OVER (ORDER BY time) AS prev_value
	FROM things_values
	%s
)
SELECT time, id, urn, location, v, vs, vb, unit, ref, source, COUNT(*) OVER () AS total
FROM changed
WHERE %s <> prev_value OR prev_value IS NULL
OFFSET @distinct_offset LIMIT @distinct_limit;`, field, b.WhereClause(), field)

	return strings.TrimSpace(querySQL), args, nil
}

func buildCountValuesSQL(query app.ValueQuery) (string, pgx.NamedArgs, error) {
	b, err := buildValueFilterBuilder(query)
	if err != nil {
		return "", nil, err
	}
	if query.TimeUnit == nil {
		return "", nil, fmt.Errorf("timeunit is required")
	}
	if *query.TimeUnit != "hour" && *query.TimeUnit != "day" {
		return "", nil, fmt.Errorf("unsupported timeunit: %s", *query.TimeUnit)
	}

	querySQL := fmt.Sprintf(`
		SELECT DATE_TRUNC('%s', time) e, id, ref, count(*) n
		FROM things_values
		%s
		GROUP BY e, id, ref
		ORDER BY e ASC;`, *query.TimeUnit, b.WhereClause())

	return strings.TrimSpace(querySQL), b.args, nil
}

func buildValueFilterBuilder(query app.ValueQuery) (*sqlBuilder, error) {
	b := newSQLBuilder()

	if query.ID != nil {
		b.Where("id = " + b.Bind("id", *query.ID))
	}
	if query.ThingID != nil {
		b.Where("id LIKE " + b.Bind("thingid_pattern", fmt.Sprintf("%s/%%", *query.ThingID)))
	}
	if len(query.URNs) > 0 {
		b.Where("urn = ANY(" + b.Bind("urn", query.URNs) + ")")
	}
	if query.Time != nil {
		switch query.Time.Relation {
		case app.TimeBefore:
			b.Where("time < " + b.Bind("ts", query.Time.At))
		case app.TimeAfter:
			b.Where("time > " + b.Bind("ts", query.Time.At))
		case app.TimeBetween:
			if query.Time.EndAt == nil {
				return nil, fmt.Errorf("end time is required for between relation")
			}
			b.Where("time > " + b.Bind("ts1", query.Time.At))
			b.Where("time < " + b.Bind("ts2", *query.Time.EndAt))
		default:
			return nil, fmt.Errorf("unsupported time relation: %s", query.Time.Relation)
		}
	}
	if query.Value != nil {
		op, err := sqlCompareOperator(query.Value.Op)
		if err != nil {
			return nil, err
		}
		b.Where("v IS NOT NULL AND v " + op + " " + b.Bind("value", query.Value.Value))
	}
	if query.BoolValue != nil {
		b.Where("vb IS NOT NULL AND vb = " + b.Bind("vb", *query.BoolValue))
	}
	if query.RefDeviceID != nil {
		b.Where("ref = " + b.Bind("ref", *query.RefDeviceID))
	}
	if query.ValueName != nil {
		b.Where("id LIKE " + b.Bind("value_name_pattern", fmt.Sprintf("%%/%s", *query.ValueName)))
	}

	return b, nil
}

func sqlCompareOperator(op app.CompareOperator) (string, error) {
	switch op {
	case app.CompareEqual:
		return "=", nil
	case app.CompareNotEqual:
		return "<>", nil
	case app.CompareGreaterThan:
		return ">", nil
	case app.CompareLessThan:
		return "<", nil
	default:
		return "", fmt.Errorf("unsupported compare operator: %s", op)
	}
}
