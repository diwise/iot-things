package application

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/diwise/iot-things/internal/application/things"
	"github.com/diwise/iot-things/pkg/types"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

// En behandlad rapport ska publicera thing.updated för den kopplade saken,
// direkt i hanteringsflödet (ingen publisher-goroutine, ingen Start/Stop).
func TestHandleMeasurementsPublishesThingUpdated(t *testing.T) {
	is := is.New(t)

	room := things.NewRoom("room-001", things.DefaultLocation, "default")
	room.AddDevice("device-1")

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			return QueryResult{Data: [][]byte{marshalThing(room)}}, nil
		},
	}
	stores := map[string]things.Thing{}
	w := &ThingsWriterMock{
		AddValueFunc: func(ctx context.Context, t things.Thing, m things.Value) error {
			return nil
		},
		UpdateThingFunc: func(ctx context.Context, u things.Thing) error {
			stores[u.ID()] = u
			return nil
		},
	}

	published := make(chan *types.ThingUpdated, 4)
	m := &messaging.MsgContextMock{
		PublishOnTopicFunc: func(ctx context.Context, message messaging.TopicMessage) error {
			if tm, ok := message.(*types.ThingUpdated); ok {
				published <- tm
			}
			return nil
		},
	}

	a := New(r, w, m)

	value := 21.0
	a.HandleMeasurements(context.Background(), []things.Measurement{{
		ID:        "device-1/3303/5700",
		Urn:       things.TemperatureURN,
		Value:     &value,
		Timestamp: time.Now().UTC(),
	}})

	select {
	case msg := <-published:
		is.Equal(msg.ID, "room-001")
		is.Equal(msg.TopicName(), "thing.updated")
		is.Equal(msg.ContentType(), "application/vnd.diwise.room+json")
		b, err := json.Marshal(msg.Thing)
		is.NoErr(err)
		is.True(strings.Contains(string(b), "room-001"))
	case <-time.After(5 * time.Second):
		t.Fatal("no thing.updated published for the connected thing")
	}
}

// En rapport med flera mätningar för samma sak ska ge exakt en publicering,
// med sakens ackumulerade tillstånd. Ingen väntan mellan rapporter.
func TestHandleMeasurementsPublishesOncePerThingAndReport(t *testing.T) {
	is := is.New(t)

	room := things.NewRoom("room-001", things.DefaultLocation, "default")
	room.AddDevice("device-1")

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			return QueryResult{Data: [][]byte{marshalThing(room)}}, nil
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

	var count int
	published := make(chan *types.ThingUpdated, 8)
	m := &messaging.MsgContextMock{
		PublishOnTopicFunc: func(ctx context.Context, message messaging.TopicMessage) error {
			if tm, ok := message.(*types.ThingUpdated); ok {
				count++
				published <- tm
			}
			return nil
		},
	}

	a := New(r, w, m)

	now := time.Now().UTC()
	temp, hum, lux := 21.0, 55.0, 300.0
	a.HandleMeasurements(context.Background(), []things.Measurement{
		{ID: "device-1/3303/5700", Urn: things.TemperatureURN, Value: &temp, Timestamp: now},
		{ID: "device-1/3304/5700", Urn: things.HumidityURN, Value: &hum, Timestamp: now},
		{ID: "device-1/3301/5700", Urn: things.IlluminanceURN, Value: &lux, Timestamp: now},
	})

	is.Equal(count, 1)

	msg := <-published
	b, err := json.Marshal(msg.Thing)
	is.NoErr(err)
	is.True(strings.Contains(string(b), `"temperature"`))
	is.True(strings.Contains(string(b), `"humidity":55`))
	is.True(strings.Contains(string(b), `"illuminance":300`))
}
