package application

import (
	"context"
	"testing"
	"time"

	"github.com/diwise/iot-things/internal/application/things"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

func lifecycleMocks() (*ThingsReaderMock, *ThingsWriterMock, *messaging.MsgContextMock) {
	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			return QueryResult{Data: [][]byte{}}, nil
		},
	}
	w := &ThingsWriterMock{}
	m := &messaging.MsgContextMock{
		PublishOnTopicFunc: func(ctx context.Context, message messaging.TopicMessage) error {
			return nil
		},
	}

	return r, w, m
}

// REV-006: Start and Stop are safe to repeat and Stop is safe before
// Start.
func TestPublisherStartStopIsIdempotent(t *testing.T) {
	is := is.New(t)

	r, w, m := lifecycleMocks()
	a := New(r, w, m)

	a.Stop()
	a.Start(context.Background())
	a.Start(context.Background())
	a.Stop()
	a.Stop()

	is.True(true)
}

// REV-006: Stop joins a blocked publisher within budget instead of
// hanging shutdown.
func TestStopJoinsBlockedPublisherWithinBudget(t *testing.T) {
	is := is.New(t)

	release := make(chan struct{})
	entered := make(chan struct{}, 1)

	room := things.NewRoom("room-001", things.DefaultLocation, "default")
	room.AddDevice("device-1")

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			if query.RefDeviceID != nil {
				return QueryResult{Data: [][]byte{room.Byte()}}, nil
			}
			entered <- struct{}{}
			select {
			case <-release:
				return QueryResult{Data: [][]byte{room.Byte()}}, nil
			case <-ctx.Done():
				return QueryResult{}, ctx.Err()
			}
		},
	}
	w := &ThingsWriterMock{
		AddValueFunc: func(ctx context.Context, t things.Thing, m things.Value) error {
			return nil
		},
		UpdateThingFunc: func(ctx context.Context, u things.Thing) error {
			return nil
		},
	}
	m := &messaging.MsgContextMock{
		PublishOnTopicFunc: func(ctx context.Context, message messaging.TopicMessage) error {
			return nil
		},
	}

	a := New(r, w, m)
	a.Start(context.Background())

	value := 21.0
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.HandleMeasurements(context.Background(), []things.Measurement{{
			ID:        "device-1/3303/5700",
			Urn:       things.TemperatureURN,
			Value:     &value,
			Timestamp: time.Now().UTC(),
		}})
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("publisher did not pick up the update")
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("HandleMeasurements did not return")
	}

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		a.Stop()
	}()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not join the publisher within budget")
	}

	close(release)
	is.True(true)
}

// REV-006: measurements handed over after Stop must not hang the
// caller; they are dropped with a warning instead.
func TestHandleMeasurementsAfterStopReturnsPromptly(t *testing.T) {
	is := is.New(t)

	room := things.NewRoom("room-001", things.DefaultLocation, "default")
	room.AddDevice("device-1")

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			return QueryResult{Data: [][]byte{room.Byte()}}, nil
		},
	}
	w := &ThingsWriterMock{
		AddValueFunc: func(ctx context.Context, t things.Thing, m things.Value) error {
			return nil
		},
		UpdateThingFunc: func(ctx context.Context, u things.Thing) error {
			return nil
		},
	}
	m := &messaging.MsgContextMock{}

	a := New(r, w, m)
	a.Start(context.Background())
	a.Stop()

	value := 21.0
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.HandleMeasurements(context.Background(), []things.Measurement{{
			ID:        "device-1/3303/5700",
			Urn:       things.TemperatureURN,
			Value:     &value,
			Timestamp: time.Now().UTC(),
		}})
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("HandleMeasurements blocked after Stop")
	}

	is.True(true)
}
