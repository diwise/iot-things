package application

import (
	"context"
	"encoding/json"
	"errors"
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
	a.HandleMeasurements(context.Background(), "default", "", []things.Measurement{{
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

// T7: temperatur från en enhet och fukt från en annan kan binda till samma
// rum, och en obunden signal påverkar inte saken.
func TestBindingsAcrossDevices(t *testing.T) {
	is := is.New(t)

	room := things.NewRoom("room-1", things.DefaultLocation, "default").(*things.Room)
	room.AddBinding(things.Binding{DeviceID: "device-a", Object: things.TemperatureURN, Resource: "5700", Input: "temperature"})
	room.AddBinding(things.Binding{DeviceID: "device-b", Object: things.HumidityURN, Resource: "5700", Input: "humidity"})

	current := things.Thing(room)
	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			return QueryResult{Data: [][]byte{marshalThing(current)}}, nil
		},
	}
	w := &ThingsWriterMock{
		AddValueFunc: func(ctx context.Context, t things.Thing, m things.Value) error { return nil },
		UpdateThingFunc: func(ctx context.Context, u things.Thing) error {
			current = u
			return nil
		},
	}
	m := &messaging.MsgContextMock{
		PublishOnTopicFunc: func(ctx context.Context, message messaging.TopicMessage) error { return nil },
	}

	a := New(r, w, m)

	temp := 21.0
	is.NoErr(a.HandleMeasurements(context.Background(), "default", "m1", []things.Measurement{
		{ID: "device-a/3303/5700", Urn: things.TemperatureURN, Value: &temp, Timestamp: time.Now().UTC()},
	}))
	is.Equal(*current.(*things.Room).Temperature.Value, 21.0)

	hum := 55.0
	is.NoErr(a.HandleMeasurements(context.Background(), "default", "m2", []things.Measurement{
		{ID: "device-b/3304/5700", Urn: things.HumidityURN, Value: &hum, Timestamp: time.Now().UTC()},
	}))
	is.Equal(current.(*things.Room).Humidity, 55.0)

	// Obunden enhet: ingen påverkan.
	unbound := 99.0
	is.NoErr(a.HandleMeasurements(context.Background(), "default", "m3", []things.Measurement{
		{ID: "device-c/3303/5700", Urn: things.TemperatureURN, Value: &unbound, Timestamp: time.Now().UTC()},
	}))
	is.Equal(*current.(*things.Room).Temperature.Value, 21.0)
}

// Fynd 1: en återlevererad rapport får inte köra tillståndsmaskinen igen.
// Passage-räknaren ska vara 1 även efter en retry med samma meddelande-id.
func TestRetryDoesNotDoubleCountState(t *testing.T) {
	is := is.New(t)

	passage := things.NewPassage("passage-1", things.DefaultLocation, "default").(*things.Passage)
	passage.ValidURN = things.PassageURNs
	passage.AddDevice("device-1")

	current := things.Thing(passage)
	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			return QueryResult{Data: [][]byte{marshalThing(current)}}, nil
		},
	}
	w := &ThingsWriterMock{
		AddValueFunc: func(ctx context.Context, t things.Thing, m things.Value) error { return nil },
		UpdateThingFunc: func(ctx context.Context, u things.Thing) error {
			current = u
			return nil
		},
	}
	m := &messaging.MsgContextMock{
		PublishOnTopicFunc: func(ctx context.Context, message messaging.TopicMessage) error { return nil },
	}

	a := New(r, w, m)

	on, off := true, false
	now := time.Now().UTC()
	report := []things.Measurement{
		{ID: "device-1/3200/5500", Urn: things.DigitalInputURN, BoolValue: &on, Timestamp: now},
		{ID: "device-1/3200/5500", Urn: things.DigitalInputURN, BoolValue: &off, Timestamp: now.Add(time.Second)},
	}

	is.NoErr(a.HandleMeasurements(context.Background(), "default", "report-1", report))
	afterFirst := current.(*things.Passage).CumulatedNumberOfPassages
	is.Equal(afterFirst, int64(1))

	// Samma meddelande-id igen: dedupliceras, ingen ny räkning.
	is.NoErr(a.HandleMeasurements(context.Background(), "default", "report-1", report))
	is.Equal(current.(*things.Passage).CumulatedNumberOfPassages, int64(1))
}

// Fynd 2: en äldre mätning får inte ändra aktuellt tillstånd.
func TestLateMeasurementDoesNotChangeState(t *testing.T) {
	is := is.New(t)

	room := things.NewRoom("room-001", things.DefaultLocation, "default").(*things.Room)
	room.ValidURN = things.RoomURNs
	room.AddDevice("device-1")

	current := things.Thing(room)
	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			return QueryResult{Data: [][]byte{marshalThing(current)}}, nil
		},
	}
	w := &ThingsWriterMock{
		AddValueFunc: func(ctx context.Context, t things.Thing, m things.Value) error { return nil },
		UpdateThingFunc: func(ctx context.Context, u things.Thing) error {
			current = u
			return nil
		},
	}
	m := &messaging.MsgContextMock{
		PublishOnTopicFunc: func(ctx context.Context, message messaging.TopicMessage) error { return nil },
	}

	a := New(r, w, m)

	newer := time.Now().UTC()
	older := newer.Add(-time.Hour)
	newVal, oldVal := 30.0, 10.0

	is.NoErr(a.HandleMeasurements(context.Background(), "default", "m-new", []things.Measurement{
		{ID: "device-1/3303/5700", Urn: things.TemperatureURN, Value: &newVal, Timestamp: newer},
	}))
	is.NoErr(a.HandleMeasurements(context.Background(), "default", "m-old", []things.Measurement{
		{ID: "device-1/3303/5700", Urn: things.TemperatureURN, Value: &oldVal, Timestamp: older},
	}))

	is.Equal(*current.(*things.Room).Temperature.Value, 30.0)
}

// Fynd 3: flera kanaler i samma pack ska aggregeras mot nya värden,
// inte mot gammal cache. (40 + 50) / 2 = 45.
func TestMultipleChannelsInOneReportAggregate(t *testing.T) {
	is := is.New(t)

	room := things.NewRoom("room-001", things.DefaultLocation, "default").(*things.Room)
	room.ValidURN = things.RoomURNs
	room.AddDevice("device-1")
	// Gammal cache: A=20, B=30.
	room.Signals_ = map[string]things.Measurement{
		"device-1/0/3303/5700": {ID: "device-1/0/3303/5700", Urn: things.TemperatureURN, Value: floatPtrApp(20)},
		"device-1/1/3303/5700": {ID: "device-1/1/3303/5700", Urn: things.TemperatureURN, Value: floatPtrApp(30)},
	}

	current := things.Thing(room)
	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			return QueryResult{Data: [][]byte{marshalThing(current)}}, nil
		},
	}
	w := &ThingsWriterMock{
		AddValueFunc: func(ctx context.Context, t things.Thing, m things.Value) error { return nil },
		UpdateThingFunc: func(ctx context.Context, u things.Thing) error {
			current = u
			return nil
		},
	}
	m := &messaging.MsgContextMock{
		PublishOnTopicFunc: func(ctx context.Context, message messaging.TopicMessage) error { return nil },
	}

	a := New(r, w, m)

	now := time.Now().UTC()
	a40, b50 := 40.0, 50.0
	is.NoErr(a.HandleMeasurements(context.Background(), "default", "m1", []things.Measurement{
		{ID: "device-1/0/3303/5700", Urn: things.TemperatureURN, Value: &a40, Timestamp: now},
		{ID: "device-1/1/3303/5700", Urn: things.TemperatureURN, Value: &b50, Timestamp: now},
	}))

	is.Equal(*current.(*things.Room).Temperature.Value, 45.0)
}

func floatPtrApp(v float64) *float64 { return &v }

// T7: en bindning med fel signal för ingången avvisas (t.ex. humidity-objekt
// kopplat till ingången temperature).
func TestAddRejectsBindingWithWrongSignal(t *testing.T) {
	is := is.New(t)

	a := New(&ThingsReaderMock{}, &ThingsWriterMock{}, &messaging.MsgContextMock{})
	room := things.NewRoom("room-1", things.DefaultLocation, "default")
	room.AddBinding(things.Binding{
		DeviceID: "device-1",
		Object:   things.HumidityURN,
		Resource: "5700",
		Input:    "temperature",
	})

	err := a.Add(context.Background(), marshalThing(room))
	is.True(errors.Is(err, ErrInvalidBinding))
}

// Fynd 5: thing-ID med "/" avvisas så att värdeägarskapet är entydigt.
func TestAddRejectsThingIDWithSlash(t *testing.T) {
	is := is.New(t)

	a := New(&ThingsReaderMock{}, &ThingsWriterMock{}, &messaging.MsgContextMock{})
	thing := things.NewWasteContainer("room/annex", things.DefaultLocation, "default")

	err := a.Add(context.Background(), marshalThing(thing))
	is.True(errors.Is(err, ErrInvalidThingID))
}

// Fynd 4: ett behandlingsfel ska propageras så att rapporten kan
// återlevereras, i stället för att tyst ackas.
func TestHandleMeasurementsPropagatesError(t *testing.T) {
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
			return errors.New("storage unavailable")
		},
		UpdateThingFunc: func(ctx context.Context, t things.Thing) error { return nil },
	}
	m := &messaging.MsgContextMock{
		PublishOnTopicFunc: func(ctx context.Context, message messaging.TopicMessage) error { return nil },
	}

	a := New(r, w, m)

	temp := 21.0
	err := a.HandleMeasurements(context.Background(), "default", "", []things.Measurement{{
		ID:        "device-1/3303/5700",
		Urn:       things.TemperatureURN,
		Value:     &temp,
		Timestamp: time.Now().UTC(),
	}})

	is.True(err != nil)
}

// En sak i en annan tenant än rapportens får aldrig uppdateras, även om
// enheten är kopplad till den.
func TestThingWithMismatchingTenantIsSkipped(t *testing.T) {
	is := is.New(t)

	room := things.NewRoom("room-other", things.DefaultLocation, "other")
	room.AddDevice("device-1")

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			return QueryResult{Data: [][]byte{marshalThing(room)}}, nil
		},
	}
	updated := false
	w := &ThingsWriterMock{
		AddValueFunc: func(ctx context.Context, t things.Thing, m things.Value) error { return nil },
		UpdateThingFunc: func(ctx context.Context, t things.Thing) error {
			updated = true
			return nil
		},
	}
	published := false
	m := &messaging.MsgContextMock{
		PublishOnTopicFunc: func(ctx context.Context, message messaging.TopicMessage) error {
			published = true
			return nil
		},
	}

	a := New(r, w, m)

	temp := 21.0
	a.HandleMeasurements(context.Background(), "default", "", []things.Measurement{{
		ID:        "device-1/3303/5700",
		Urn:       things.TemperatureURN,
		Value:     &temp,
		Timestamp: time.Now().UTC(),
	}})

	is.True(!updated)
	is.True(!published)
}

// Flera enheter kopplade till samma sak: värdena aggregeras på saken och
// thing.updated publiceras. Detta är det vanliga fallet.
func TestMultipleDevicesOneThingAggregates(t *testing.T) {
	is := is.New(t)

	current := things.NewRoom("room-001", things.DefaultLocation, "default")
	current.AddDevice("device-1")
	current.AddDevice("device-2")

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			return QueryResult{Data: [][]byte{marshalThing(current)}}, nil
		},
	}
	w := &ThingsWriterMock{
		AddValueFunc: func(ctx context.Context, t things.Thing, m things.Value) error {
			return nil
		},
		UpdateThingFunc: func(ctx context.Context, u things.Thing) error {
			current = u
			return nil
		},
	}

	var publishes int
	m := &messaging.MsgContextMock{
		PublishOnTopicFunc: func(ctx context.Context, message messaging.TopicMessage) error {
			if _, ok := message.(*types.ThingUpdated); ok {
				publishes++
			}
			return nil
		},
	}

	a := New(r, w, m)

	t20 := 20.0
	a.HandleMeasurements(context.Background(), "default", "", []things.Measurement{
		{ID: "device-1/3303/5700", Urn: things.TemperatureURN, Value: &t20, Timestamp: time.Now().UTC()},
	})
	is.Equal(*current.(*things.Room).Temperature.Value, 20.0)

	t30 := 30.0
	a.HandleMeasurements(context.Background(), "default", "", []things.Measurement{
		{ID: "device-2/3303/5700", Urn: things.TemperatureURN, Value: &t30, Timestamp: time.Now().UTC()},
	})

	// (30 + 20) / 2 = 25, ackumulerat över båda enheterna.
	is.Equal(*current.(*things.Room).Temperature.Value, 25.0)
	is.Equal(publishes, 2)
}

// En enhet som är kopplad till flera saker ska uppdatera alla, med en
// publicering per sak.
func TestMultipleThingsForSameDevice(t *testing.T) {
	is := is.New(t)

	roomA := things.NewRoom("room-a", things.DefaultLocation, "default")
	roomB := things.NewRoom("room-b", things.DefaultLocation, "default")
	roomA.AddDevice("device-1")
	roomB.AddDevice("device-1")

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			if query.RefDeviceID != nil && *query.RefDeviceID == "device-1" {
				return QueryResult{Data: [][]byte{marshalThing(roomA), marshalThing(roomB)}}, nil
			}
			return QueryResult{Data: [][]byte{}}, nil
		},
	}
	w := &ThingsWriterMock{
		AddValueFunc:    func(ctx context.Context, t things.Thing, m things.Value) error { return nil },
		UpdateThingFunc: func(ctx context.Context, t things.Thing) error { return nil },
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

	temp := 21.0
	a.HandleMeasurements(context.Background(), "default", "", []things.Measurement{{
		ID:        "device-1/3303/5700",
		Urn:       things.TemperatureURN,
		Value:     &temp,
		Timestamp: time.Now().UTC(),
	}})

	ids := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case msg := <-published:
			ids[msg.ID] = true
		case <-time.After(5 * time.Second):
			t.Fatal("expected one thing.updated per connected thing")
		}
	}
	is.True(ids["room-a"])
	is.True(ids["room-b"])
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
	a.HandleMeasurements(context.Background(), "default", "", []things.Measurement{
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
