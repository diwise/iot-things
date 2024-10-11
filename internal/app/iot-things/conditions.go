package iotthings

import "strconv"

type ConditionFunc func(map[string]any) map[string]any

type QueryResult struct {
	Things     [][]byte
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

func WithParams(params map[string][]string) []ConditionFunc {
	conditions := make([]ConditionFunc, 0)

	if id, ok := params["id"]; ok {
		conditions = append(conditions, WithID(id[0]))
	}

	if tenants, ok := params["tenant"]; ok {
		conditions = append(conditions, WithTenants(tenants))
	}

	if types, ok := params["type"]; ok {
		conditions = append(conditions, WithTypes(types))
	}

	if subType, ok := params["sub_type"]; ok {
		conditions = append(conditions, WithSubType(subType[0]))
	}

	if tags, ok := params["tags"]; ok {
		conditions = append(conditions, WithTags(tags))
	}

	if refDevice, ok := params["ref_device"]; ok {
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

	return conditions
}
