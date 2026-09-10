package things

import (
	"context"
	"testing"
	"time"

	"github.com/matryer/is"
)

func collectValues(sink *[]Value) func(m ValueProvider) error {
	return func(m ValueProvider) error {
		*sink = append(*sink, m.Values()...)
		return nil
	}
}

func TestBuildingEnergyPowerTemperature(t *testing.T) {
	is := is.New(t)

	building := NewBuilding("building-1", Location{Latitude: 62, Longitude: 17}, "default").(*Building)

	energyJoules := 3_600_000.0
	powerWatts := 2000.0
	temp := 21.5

	var emitted []Value
	onchange := collectValues(&emitted)

	err := building.Handle(context.Background(), []Measurement{
		{ID: "d/3331/5700", Urn: EnergyURN, Value: &energyJoules, Timestamp: time.Now()},
		{ID: "d/3328/5700", Urn: PowerURN, Value: &powerWatts, Timestamp: time.Now()},
		{ID: "d/3303/5700", Urn: TemperatureURN, Value: &temp, Timestamp: time.Now()},
	}, onchange)
	is.NoErr(err)

	is.Equal(building.Energy, 1.0) // 3 600 000 J => 1 kWh
	is.Equal(building.Power, 2.0)  // 2000 W => 2 kW
	is.Equal(*building.Temperature.Value, 21.5)
	is.Equal(len(emitted), 3)
}

func TestDeskPresence(t *testing.T) {
	is := is.New(t)

	desk := NewDesk("desk-1", DefaultLocation, "default").(*Desk)

	on := true
	is.NoErr(desk.Handle(context.Background(), []Measurement{
		{ID: "d/3200/5500", Urn: DigitalInputURN, BoolValue: &on, Timestamp: time.Now()},
	}, func(m ValueProvider) error { return nil }))
	is.Equal(desk.Presence, true)

	off := false
	is.NoErr(desk.Handle(context.Background(), []Measurement{
		{ID: "d/3200/5500", Urn: DigitalInputURN, BoolValue: &off, Timestamp: time.Now()},
	}, func(m ValueProvider) error { return nil }))
	is.Equal(desk.Presence, false)
}

func TestLifebuoyPresence(t *testing.T) {
	is := is.New(t)

	lifebuoy := NewLifebuoy("lifebuoy-1", DefaultLocation, "default").(*Lifebuoy)

	on := true
	var emitted []Value
	is.NoErr(lifebuoy.Handle(context.Background(), []Measurement{
		{ID: "d/3302/5500", Urn: PresenceURN, BoolValue: &on, Timestamp: time.Now()},
	}, collectValues(&emitted)))
	is.Equal(lifebuoy.Presence, true)
	is.Equal(len(emitted), 1)
}

func TestWatermeter(t *testing.T) {
	is := is.New(t)

	wm := NewWatermeter("wm-1", DefaultLocation, "default").(*Watermeter)
	is.Equal(wm.Type(), "Watermeter") // regression: var tidigare "Room"

	volume := 10.5
	leak := true
	backflow := true
	fraud := true
	now := time.Now()

	var emitted []Value
	is.NoErr(wm.Handle(context.Background(), []Measurement{
		{ID: "d/3424/1", Urn: WaterMeterURN, Value: &volume, Timestamp: now},
		{ID: "d/3424/10", Urn: WaterMeterURN, BoolValue: &leak, Timestamp: now},
		{ID: "d/3424/11", Urn: WaterMeterURN, BoolValue: &backflow, Timestamp: now},
		{ID: "d/3424/13", Urn: WaterMeterURN, BoolValue: &fraud, Timestamp: now},
	}, collectValues(&emitted)))

	is.Equal(wm.CumulativeVolume, 10.5)
	is.Equal(wm.Leakage, true)
	is.Equal(wm.Backflow, true)
	is.Equal(wm.Fraud, true)
	is.True(len(emitted) > 0)
}

func TestSinkDigitalInput(t *testing.T) {
	is := is.New(t)

	sink := NewSink("sink-1", DefaultLocation, "default").(*Sink)

	now := time.Now()
	on := true
	off := false

	is.NoErr(sink.Handle(context.Background(), []Measurement{
		{ID: "d/3200/5500", Urn: DigitalInputURN, BoolValue: &on, Timestamp: now},
	}, func(m ValueProvider) error { return nil }))
	is.Equal(sink.On, true)

	is.NoErr(sink.Handle(context.Background(), []Measurement{
		{ID: "d/3200/5500", Urn: DigitalInputURN, BoolValue: &off, Timestamp: now.Add(time.Minute)},
	}, func(m ValueProvider) error { return nil }))
	is.Equal(sink.On, false)
	is.True(sink.CumulativeTime > 0)
}
