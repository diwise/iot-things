package iotthings

import (
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
		m["sub_type"] = subType
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
		m["ref_device"] = refDevice
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

	if id, ok := params["id"]; ok {
		conditions = append(conditions, WithID(id[0]))
	}

	if tenants, ok := params["tenant"]; ok {
		conditions = append(conditions, WithTenants(tenants))
	}

	if types, ok := params["type"]; ok {
		conditions = append(conditions, WithTypes(types))
	}

	if subType, ok := params["subtype"]; ok {
		conditions = append(conditions, WithSubType(subType[0]))
	}

	if tags, ok := params["tags"]; ok {
		conditions = append(conditions, WithTags(tags))
	}

	if refDevice, ok := params["refdevice"]; ok {
		conditions = append(conditions, WithRefDevice(refDevice[0]))
	}

	if offset, ok := params["offset"]; ok {
		i, err := strconv.Atoi(offset[0])
		if err == nil {
			conditions = append(conditions, WithOffset(i))
		}
	}

	if limit, ok := params["limit"]; ok {
		i, err := strconv.Atoi(limit[0])
		if err == nil {
			conditions = append(conditions, WithLimit(i))
		}
	}

	if thing_id, ok := params["thingid"]; ok {
		conditions = append(conditions, WithThingID(thing_id[0]))
	}

	if urn, ok := params["urn"]; ok {
		conditions = append(conditions, WithUrn(urn))
	}

	if timeRel, ok := params["timerel"]; ok {
		conditions = append(conditions, WithTimeRel(timeRel[0]))

		if timeAt, ok := params["timeat"]; ok {
			conditions = append(conditions, WithTimeAt(timeAt[0]))
		}

		if endTimeAt, ok := params["endtimeat"]; ok {
			conditions = append(conditions, WithEndTimeAt(endTimeAt[0]))
		}
	}

	if operator, ok := params["op"]; ok {
		conditions = append(conditions, WithOperator(operator[0]))
	}

	if value, ok := params["value"]; ok {
		if _, ok := params["op"]; !ok {
			conditions = append(conditions, WithOperator("eq"))
		}
		conditions = append(conditions, WithValue(value[0]))
	}

	if vb, ok := params["vb"]; ok {
		conditions = append(conditions, WithBoolValue(vb[0]))
	}

	if n,ok := params["n"]; ok {
		conditions = append(conditions, WithValueName(n[0]))
	}

	return conditions
}
