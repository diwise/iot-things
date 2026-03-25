package application

import (
	"context"
	"sync"
)

var _ ThingsReader = &ThingsReaderMock{}

type ThingsReaderMock struct {
	GetTagsFunc     func(ctx context.Context, tenants []string) ([]string, error)
	QueryThingsFunc func(ctx context.Context, query ThingQuery) (QueryResult, error)
	QueryValuesFunc func(ctx context.Context, query ValueQuery) (QueryResult, error)

	calls struct {
		GetTags []struct {
			Ctx     context.Context
			Tenants []string
		}
		QueryThings []struct {
			Ctx   context.Context
			Query ThingQuery
		}
		QueryValues []struct {
			Ctx   context.Context
			Query ValueQuery
		}
	}
	lockGetTags     sync.RWMutex
	lockQueryThings sync.RWMutex
	lockQueryValues sync.RWMutex
}

func (mock *ThingsReaderMock) GetTags(ctx context.Context, tenants []string) ([]string, error) {
	if mock.GetTagsFunc == nil {
		panic("ThingsReaderMock.GetTagsFunc: method is nil but ThingsReader.GetTags was just called")
	}
	callInfo := struct {
		Ctx     context.Context
		Tenants []string
	}{Ctx: ctx, Tenants: tenants}
	mock.lockGetTags.Lock()
	mock.calls.GetTags = append(mock.calls.GetTags, callInfo)
	mock.lockGetTags.Unlock()
	return mock.GetTagsFunc(ctx, tenants)
}

func (mock *ThingsReaderMock) GetTagsCalls() []struct {
	Ctx     context.Context
	Tenants []string
} {
	mock.lockGetTags.RLock()
	defer mock.lockGetTags.RUnlock()
	return append([]struct {
		Ctx     context.Context
		Tenants []string
	}{}, mock.calls.GetTags...)
}

func (mock *ThingsReaderMock) QueryThings(ctx context.Context, query ThingQuery) (QueryResult, error) {
	if mock.QueryThingsFunc == nil {
		panic("ThingsReaderMock.QueryThingsFunc: method is nil but ThingsReader.QueryThings was just called")
	}
	callInfo := struct {
		Ctx   context.Context
		Query ThingQuery
	}{Ctx: ctx, Query: query}
	mock.lockQueryThings.Lock()
	mock.calls.QueryThings = append(mock.calls.QueryThings, callInfo)
	mock.lockQueryThings.Unlock()
	return mock.QueryThingsFunc(ctx, query)
}

func (mock *ThingsReaderMock) QueryThingsCalls() []struct {
	Ctx   context.Context
	Query ThingQuery
} {
	mock.lockQueryThings.RLock()
	defer mock.lockQueryThings.RUnlock()
	return append([]struct {
		Ctx   context.Context
		Query ThingQuery
	}{}, mock.calls.QueryThings...)
}

func (mock *ThingsReaderMock) QueryValues(ctx context.Context, query ValueQuery) (QueryResult, error) {
	if mock.QueryValuesFunc == nil {
		panic("ThingsReaderMock.QueryValuesFunc: method is nil but ThingsReader.QueryValues was just called")
	}
	callInfo := struct {
		Ctx   context.Context
		Query ValueQuery
	}{Ctx: ctx, Query: query}
	mock.lockQueryValues.Lock()
	mock.calls.QueryValues = append(mock.calls.QueryValues, callInfo)
	mock.lockQueryValues.Unlock()
	return mock.QueryValuesFunc(ctx, query)
}

func (mock *ThingsReaderMock) QueryValuesCalls() []struct {
	Ctx   context.Context
	Query ValueQuery
} {
	mock.lockQueryValues.RLock()
	defer mock.lockQueryValues.RUnlock()
	return append([]struct {
		Ctx   context.Context
		Query ValueQuery
	}{}, mock.calls.QueryValues...)
}
