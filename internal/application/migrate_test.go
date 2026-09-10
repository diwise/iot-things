package application

import (
	"context"
	"testing"

	"github.com/diwise/iot-things/internal/application/things"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

const legacyRoomJSON = `{"id":"room-1","type":"Room","name":"","location":{"latitude":0,"longitude":0},"tenant":"default","observedAt":"2024-01-01T00:00:00Z","refDevices":[{"deviceID":"device-1"}]}`

// T7.4: en gammal post (refDevices utan bindningar) konverteras till
// bindningar och markeraras, och migreringen är idempotent.
func TestMigrateBindingsConvertsLegacyThings(t *testing.T) {
	is := is.New(t)

	current := []byte(legacyRoomJSON)

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			return QueryResult{Data: [][]byte{current}, Count: 1, TotalCount: 1}, nil
		},
	}
	var saved things.Thing
	w := &ThingsWriterMock{
		UpdateThingFunc: func(ctx context.Context, u things.Thing) error {
			saved = u
			current = marshalThing(u)
			return nil
		},
	}
	m := &messaging.MsgContextMock{}

	a := New(r, w, m)

	n, err := a.MigrateBindings(context.Background())
	is.NoErr(err)
	is.Equal(n, 1)
	is.True(saved != nil)

	var hasTemperature bool
	for _, b := range saved.Bindings() {
		if b.DeviceID != "device-1" {
			t.Fatalf("unexpected device id %q", b.DeviceID)
		}
		if b.Input == "temperature" {
			hasTemperature = true
		}
	}
	is.True(hasTemperature)

	// Andra körningen ska inte migrera något igen.
	n, err = a.MigrateBindings(context.Background())
	is.NoErr(err)
	is.Equal(n, 0)
}

// T7.4: med migreringen avstängd ska kvarvarande omigrerade poster upptäckas.
func TestHasUnmigratedThings(t *testing.T) {
	is := is.New(t)

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			return QueryResult{Data: [][]byte{[]byte(legacyRoomJSON)}, Count: 1, TotalCount: 1}, nil
		},
	}
	a := New(r, &ThingsWriterMock{}, &messaging.MsgContextMock{})

	pending, err := a.HasUnmigratedThings(context.Background())
	is.NoErr(err)
	is.True(pending)
}
