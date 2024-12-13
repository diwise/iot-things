package iotthings

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

type ConditionFunc func(map[string]any) map[string]any

type QueryResult struct {
	Data       [][]byte
	Count      int
	Limit      int
	Offset     int
	TotalCount int64
}

func WithID(id string) ConditionFunc {
	return func(m map[string]any) map[string]any {
		m["id"] = id
		return m
	}
}

func WithTenants(tenants []string) ConditionFunc {
	return func(m map[string]any) map[string]any {
		m["tenants"] = tenants
		return m
	}
}

func WithTypes(types []string) ConditionFunc {
	return func(m map[string]any) map[string]any {
		m["types"] = types
		return m
	}
}

func WithSubType(subType string) ConditionFunc {
	return func(m map[string]any) map[string]any {
		m["subtype"] = subType
		return m
	}
}

func WithTags(tags []string) ConditionFunc {
	return func(m map[string]any) map[string]any {
		m["tags"] = tags
		return m
	}
}

func WithRefDevice(refDevice string) ConditionFunc {
	return func(m map[string]any) map[string]any {
		m["refdevice"] = refDevice
		return m
	}
}

func WithOffset(offset int) ConditionFunc {
	return func(m map[string]any) map[string]any {
		m["offset"] = offset
		return m
	}
}

func WithLimit(limit int) ConditionFunc {
	return func(m map[string]any) map[string]any {
		m["limit"] = limit
		return m
	}
}

func WithThingID(thingID string) ConditionFunc {
	return func(m map[string]any) map[string]any {
		m["thingid"] = thingID
		return m
	}
}

func WithUrn(urn []string) ConditionFunc {
	return func(m map[string]any) map[string]any {
		m["urn"] = urn
		return m
	}
}

func WithTimeRel(timeRel string) ConditionFunc {
	return func(m map[string]any) map[string]any {
		timeRel = strings.ToLower(timeRel)
		if slices.Contains([]string{"before", "after", "between"}, timeRel) {
			m["timerel"] = timeRel
		}
		return m
	}
}

func WithTimeAt(timeAt string) ConditionFunc {
	ts, err := time.Parse(time.RFC3339, timeAt)
	if err != nil {
		return func(m map[string]any) map[string]any {
			return m
		}
	}

	return func(m map[string]any) map[string]any {
		m["timeat"] = ts
		return m
	}
}

func WithEndTimeAt(endTimeAt string) ConditionFunc {
	ts, err := time.Parse(time.RFC3339, endTimeAt)
	if err != nil {
		return func(m map[string]any) map[string]any {
			return m
		}
	}

	return func(m map[string]any) map[string]any {
		m["endtimeat"] = ts
		return m
	}
}

func WithOperator(operator string) ConditionFunc {
	return func(m map[string]any) map[string]any {
		operator = strings.ToLower(operator)
		if slices.Contains([]string{"eq", "ne", "gt", "lt"}, operator) {
			m["operator"] = operator
		}
		return m
	}
}

func WithValue(value string) ConditionFunc {
	v, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return func(m map[string]any) map[string]any {
			return m
		}
	}

	return func(m map[string]any) map[string]any {
		m["value"] = v
		return m
	}
}

func WithBoolValue(vb string) ConditionFunc {
	b, err := strconv.ParseBool(vb)
	if err != nil {
		return func(m map[string]any) map[string]any {
			return m
		}
	}

	return func(m map[string]any) map[string]any {
		m["vb"] = b
		return m
	}
}

func WithValueName(n string) ConditionFunc {
	return func(m map[string]any) map[string]any {
		m["n"] = n
		return m
	}
}

func WithTimeUnit(timeUnit string) ConditionFunc {
	return func(m map[string]any) map[string]any {
		if slices.Contains([]string{"hour", "day"}, timeUnit) {
			m["timeunit"] = timeUnit
		}
		return m
	}
}

func WithFieldNameValue(fieldName string, value any) ConditionFunc {
	return func(m map[string]any) map[string]any {
		key := fmt.Sprintf("<%s>", fieldName)
		m[key] = value
		return m
	}
}

func WithShowLatest(showLatest bool) ConditionFunc {
	return func(m map[string]any) map[string]any {
		m["showlatest"] = showLatest
		return m
	}
}

func WithParams(query map[string][]string) []ConditionFunc {
	conditions := make([]ConditionFunc, 0)

	params := map[string][]string{}
	for k, v := range query {
		key := strings.ReplaceAll(strings.ToLower(k), "_", "")
		if key == "v" {
			key = "value"
		}
		params[key] = v
	}

	for key, values := range params {
		switch key {
		case "id":
			conditions = append(conditions, WithID(values[0]))
		case "tenant":
			conditions = append(conditions, WithTenants(values))
		case "type":
			conditions = append(conditions, WithTypes(values))
		case "subtype":
			conditions = append(conditions, WithSubType(values[0]))
		case "tags":
			conditions = append(conditions, WithTags(values))
		case "refdevice":
			conditions = append(conditions, WithRefDevice(values[0]))
		case "offset":
			if i, err := strconv.Atoi(values[0]); err == nil {
				conditions = append(conditions, WithOffset(i))
			}
		case "limit":
			if i, err := strconv.Atoi(values[0]); err == nil {
				conditions = append(conditions, WithLimit(i))
			}
		case "thingid":
			conditions = append(conditions, WithThingID(values[0]))
		case "urn":
			conditions = append(conditions, WithUrn(values))
		case "timerel":
			conditions = append(conditions, WithTimeRel(values[0]))
			if timeAt, ok := params["timeat"]; ok {
				conditions = append(conditions, WithTimeAt(timeAt[0]))
			}
			if endTimeAt, ok := params["endtimeat"]; ok {
				conditions = append(conditions, WithEndTimeAt(endTimeAt[0]))
			}
		case "op":
			conditions = append(conditions, WithOperator(values[0]))
		case "value":
			if _, ok := params["op"]; !ok {
				conditions = append(conditions, WithOperator("eq"))
			}
			conditions = append(conditions, WithValue(values[0]))
		case "vb":
			conditions = append(conditions, WithBoolValue(values[0]))
		case "n":
			conditions = append(conditions, WithValueName(values[0]))
		case "timeunit":
			conditions = append(conditions, WithTimeUnit(values[0]))
		case "latest":
			if values[0] == "true" {
				if _, ok := params["thingid"]; ok {
					conditions = append(conditions, WithShowLatest(true))
				}
			}
		}

		if strings.HasPrefix(key, "v[") && strings.HasSuffix(key, "]") {
			fieldname := key[2 : len(key)-1]
			conditions = append(conditions, WithFieldNameValue(fieldname, values))
			if _, ok := params["op"]; !ok {
				conditions = append(conditions, WithOperator("gt"))
			}
		}
	}

	return conditions
}
