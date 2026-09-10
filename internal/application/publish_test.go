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

// T0: en behandlad rapport ska resultera i thing.updated för den kopplade
// saken. Provet fungerar både före och efter att publiceringen flyttats in
// i handle-flödet (det väntar med timeout i stället för att anta synkronism).
func TestHandleMeasurementsPublishesThingUpdated(t *testing.T) {
	is := is.New(t)

	room := things.NewRoom("room-001", things.DefaultLocation, "default")
	room.AddDevice("device-1")
	room.Byte() // säkerställ att den serialiseras som publisher gjorde

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			return QueryResult{Data: [][]byte{room.Byte()}}, nil
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
	a.Start(context.Background())
	defer a.Stop()

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
