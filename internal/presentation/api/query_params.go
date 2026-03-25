package api

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	app "github.com/diwise/iot-things/internal/application"
)

func parseThingQuery(values url.Values, tenants []string) (app.ThingQuery, error) {
	normalized := normalizeParams(values)
	query := app.NewThingQuery()
	query.Tenants = append([]string{}, tenants...)

	if id, ok := first(normalized, "id"); ok {
		query.ID = &id
	}
	if types, ok := normalized["type"]; ok {
		query.Types = append([]string{}, types...)
	}
	if subType, ok := first(normalized, "subtype"); ok {
		query.SubType = &subType
	}
	if tags, ok := normalized["tags"]; ok {
		query.Tags = append([]string{}, tags...)
	}
	if refDeviceID, ok := first(normalized, "refdevice"); ok {
		query.RefDeviceID = &refDeviceID
	}

	if limit, ok, err := parseOptionalInt(normalized, "limit"); err != nil {
		return app.ThingQuery{}, err
	} else if ok {
		query.Page.Limit = limit
	}
	if offset, ok, err := parseOptionalInt(normalized, "offset"); err != nil {
		return app.ThingQuery{}, err
	} else if ok {
		query.Page.Offset = offset
	}
	if export, ok, err := parseOptionalBool(normalized, "export"); err != nil {
		return app.ThingQuery{}, err
	} else if ok {
		query.Page.Export = export
	}

	op := app.CompareGreaterThan
	if opValue, ok := first(normalized, "op"); ok {
		parsedOp, err := parseCompareOperator(opValue)
		if err != nil {
			return app.ThingQuery{}, err
		}
		op = parsedOp
	}

	for key, rawValues := range normalized {
		if !strings.HasPrefix(key, "v[") || !strings.HasSuffix(key, "]") {
			continue
		}

		field := key[2 : len(key)-1]
		value, err := strconv.ParseFloat(rawValues[0], 64)
		if err != nil {
			return app.ThingQuery{}, fmt.Errorf("invalid numeric filter for %s: %w", field, err)
		}

		query.NumericFilters = append(query.NumericFilters, app.NumericFieldFilter{
			Field: field,
			Op:    op,
			Value: value,
		})
	}

	return query, nil
}

func parseValueQuery(values url.Values) (app.ValueQuery, error) {
	normalized := normalizeParams(values)
	query := app.NewValueQuery()

	if id, ok := first(normalized, "id"); ok {
		query.ID = &id
	}
	if thingID, ok := first(normalized, "thingid"); ok {
		query.ThingID = &thingID
	}
	if urns, ok := normalized["urn"]; ok {
		query.URNs = append([]string{}, urns...)
	}
	if refDeviceID, ok := first(normalized, "refdevice"); ok {
		query.RefDeviceID = &refDeviceID
	}
	if valueName, ok := first(normalized, "n"); ok {
		query.ValueName = &valueName
	}

	if limit, ok, err := parseOptionalInt(normalized, "limit"); err != nil {
		return app.ValueQuery{}, err
	} else if ok {
		query.Page.Limit = limit
	}
	if offset, ok, err := parseOptionalInt(normalized, "offset"); err != nil {
		return app.ValueQuery{}, err
	} else if ok {
		query.Page.Offset = offset
	}

	if timerel, ok := first(normalized, "timerel"); ok {
		relation, err := parseTimeRelation(timerel)
		if err != nil {
			return app.ValueQuery{}, err
		}
		timeAtValue, ok := first(normalized, "timeat")
		if !ok {
			return app.ValueQuery{}, fmt.Errorf("timeat is required when timerel is set")
		}
		at, err := time.Parse(time.RFC3339, timeAtValue)
		if err != nil {
			return app.ValueQuery{}, fmt.Errorf("invalid timeat: %w", err)
		}

		timeFilter := &app.TimeFilter{Relation: relation, At: at}
		if relation == app.TimeBetween {
			endAtValue, ok := first(normalized, "endtimeat")
			if !ok {
				return app.ValueQuery{}, fmt.Errorf("endtimeat is required when timerel=between")
			}
			endAt, err := time.Parse(time.RFC3339, endAtValue)
			if err != nil {
				return app.ValueQuery{}, fmt.Errorf("invalid endtimeat: %w", err)
			}
			timeFilter.EndAt = &endAt
		}
		query.Time = timeFilter
	}

	if value, ok := first(normalized, "value"); ok {
		numericValue, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return app.ValueQuery{}, fmt.Errorf("invalid value: %w", err)
		}

		op := app.CompareEqual
		if opValue, ok := first(normalized, "op"); ok {
			parsedOp, err := parseCompareOperator(opValue)
			if err != nil {
				return app.ValueQuery{}, err
			}
			op = parsedOp
		}

		query.Value = &app.NumericValueFilter{Op: op, Value: numericValue}
	}

	if vb, ok, err := parseOptionalBool(normalized, "vb"); err != nil {
		return app.ValueQuery{}, err
	} else if ok {
		query.BoolValue = &vb
	}

	if timeUnit, ok := first(normalized, "timeunit"); ok {
		if timeUnit != "hour" && timeUnit != "day" {
			return app.ValueQuery{}, fmt.Errorf("invalid timeunit: %s", timeUnit)
		}
		query.TimeUnit = &timeUnit
		query.Mode = app.ValueQueryModeCountByTime
	}

	if distinctBy, ok := first(normalized, "distinct"); ok {
		if distinctBy != "v" && distinctBy != "vb" {
			return app.ValueQuery{}, fmt.Errorf("invalid distinct field: %s", distinctBy)
		}
		query.DistinctBy = &distinctBy
		query.Mode = app.ValueQueryModeDistinct
	}

	if latest, ok, err := parseOptionalBool(normalized, "latest"); err != nil {
		return app.ValueQuery{}, err
	} else if ok && latest {
		if query.ThingID == nil {
			return app.ValueQuery{}, fmt.Errorf("thingid is required when latest=true")
		}
		if query.TimeUnit != nil || query.DistinctBy != nil {
			return app.ValueQuery{}, fmt.Errorf("latest=true cannot be combined with distinct or timeunit")
		}
		query.Mode = app.ValueQueryModeLatest
	}

	return query, nil
}

func normalizeParams(values url.Values) map[string][]string {
	normalized := map[string][]string{}
	for key, currentValues := range values {
		normalizedKey := strings.ReplaceAll(strings.ToLower(key), "_", "")
		if normalizedKey == "v" {
			normalizedKey = "value"
		}
		normalized[normalizedKey] = append([]string{}, currentValues...)
	}
	return normalized
}

func first(values map[string][]string, key string) (string, bool) {
	entries, ok := values[key]
	if !ok || len(entries) == 0 {
		return "", false
	}
	return entries[0], true
}

func parseOptionalInt(values map[string][]string, key string) (int, bool, error) {
	raw, ok := first(values, key)
	if !ok {
		return 0, false, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false, fmt.Errorf("invalid %s: %w", key, err)
	}
	if key == "limit" && value <= 0 {
		return 0, false, fmt.Errorf("invalid %s: must be > 0", key)
	}
	if key == "offset" && value < 0 {
		return 0, false, fmt.Errorf("invalid %s: must be >= 0", key)
	}
	return value, true, nil
}

func parseOptionalBool(values map[string][]string, key string) (bool, bool, error) {
	raw, ok := first(values, key)
	if !ok {
		return false, false, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, false, fmt.Errorf("invalid %s: %w", key, err)
	}
	return value, true, nil
}

func parseCompareOperator(raw string) (app.CompareOperator, error) {
	switch strings.ToLower(raw) {
	case string(app.CompareEqual):
		return app.CompareEqual, nil
	case string(app.CompareNotEqual):
		return app.CompareNotEqual, nil
	case string(app.CompareGreaterThan):
		return app.CompareGreaterThan, nil
	case string(app.CompareLessThan):
		return app.CompareLessThan, nil
	default:
		return "", fmt.Errorf("invalid operator: %s", raw)
	}
}

func parseTimeRelation(raw string) (app.TimeRelation, error) {
	switch strings.ToLower(raw) {
	case string(app.TimeBefore):
		return app.TimeBefore, nil
	case string(app.TimeAfter):
		return app.TimeAfter, nil
	case string(app.TimeBetween):
		return app.TimeBetween, nil
	default:
		return "", fmt.Errorf("invalid timerel: %s", raw)
	}
}
