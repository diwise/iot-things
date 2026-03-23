package application

import "time"

const DefaultQueryLimit = 100

type CompareOperator string

const (
	CompareEqual       CompareOperator = "eq"
	CompareNotEqual    CompareOperator = "ne"
	CompareGreaterThan CompareOperator = "gt"
	CompareLessThan    CompareOperator = "lt"
)

type TimeRelation string

const (
	TimeBefore  TimeRelation = "before"
	TimeAfter   TimeRelation = "after"
	TimeBetween TimeRelation = "between"
)

type ValueQueryMode string

const (
	ValueQueryModeDefault     ValueQueryMode = "default"
	ValueQueryModeLatest      ValueQueryMode = "latest"
	ValueQueryModeDistinct    ValueQueryMode = "distinct"
	ValueQueryModeCountByTime ValueQueryMode = "count_by_time"
)

type Pagination struct {
	Limit  int
	Offset int
	Export bool
}

func DefaultPagination() Pagination {
	return Pagination{
		Limit:  DefaultQueryLimit,
		Offset: 0,
	}
}

type NumericFieldFilter struct {
	Field string
	Op    CompareOperator
	Value float64
}

type NumericValueFilter struct {
	Op    CompareOperator
	Value float64
}

type TimeFilter struct {
	Relation TimeRelation
	At       time.Time
	EndAt    *time.Time
}

type ThingQuery struct {
	ID             *string
	Tenants        []string
	Types          []string
	SubType        *string
	Tags           []string
	RefDeviceID    *string
	NumericFilters []NumericFieldFilter
	Page           Pagination
}

type ValueQuery struct {
	Mode        ValueQueryMode
	ID          *string
	ThingID     *string
	URNs        []string
	Time        *TimeFilter
	Value       *NumericValueFilter
	BoolValue   *bool
	RefDeviceID *string
	ValueName   *string
	TimeUnit    *string
	DistinctBy  *string
	Page        Pagination
}

type QueryResult struct {
	Data       [][]byte
	Count      int
	Limit      int
	Offset     int
	TotalCount int64
}

func NewThingQuery() ThingQuery {
	return ThingQuery{Page: DefaultPagination()}
}

func ThingByIDQuery(id string, tenants []string) ThingQuery {
	query := NewThingQuery()
	query.ID = &id
	query.Tenants = append([]string{}, tenants...)
	return query
}

func ThingsByRefDeviceQuery(deviceID string) ThingQuery {
	query := NewThingQuery()
	query.RefDeviceID = &deviceID
	return query
}

func NewValueQuery() ValueQuery {
	return ValueQuery{
		Mode: ValueQueryModeDefault,
		Page: DefaultPagination(),
	}
}
