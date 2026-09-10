package things

import (
	"testing"

	"github.com/matryer/is"
)

func TestInputTableResolvesSignals(t *testing.T) {
	is := is.New(t)

	cases := []struct {
		thingType string
		id        string
		urn       string
		wantInput string
		wantOK    bool
	}{
		{"room", "d/3303/5700", TemperatureURN, "temperature", true},
		{"room", "d/3304/5700", HumidityURN, "humidity", true},
		{"room", "d/3301/5700", IlluminanceURN, "illuminance", true},
		{"room", "d/3428/17", AirQualityURN, "co2", true},
		// Partikel-resurs i samma objekt ska inte matcha CO2.
		{"room", "d/3428/1", AirQualityURN, "", false},
		{"building", "d/3331/5700", EnergyURN, "energy", true},
		{"building", "d/3328/5700", PowerURN, "power", true},
		{"watermeter", "d/3424/1", WaterMeterURN, "volume", true},
		{"watermeter", "d/3424/10", WaterMeterURN, "leakage", true},
		{"watermeter", "d/3424/11", WaterMeterURN, "backflow", true},
		{"watermeter", "d/3424/13", WaterMeterURN, "fraud", true},
		{"sewer", "d/3330/5700", DistanceURN, "distance", true},
		{"sewer", "d/3200/5500", DigitalInputURN, "digitalInput", true},
		{"desk", "d/3200/5500", DigitalInputURN, "presence", true},
		{"desk", "d/3302/5500", PresenceURN, "presence", true},
		{"unknown", "d/3303/5700", TemperatureURN, "", false},
	}

	for _, tc := range cases {
		m := Measurement{ID: tc.id, Urn: tc.urn}
		input, ok := resolveInput(tc.thingType, m)
		is.Equal(ok, tc.wantOK)
		is.Equal(input, tc.wantInput)
	}
}

func TestInputExists(t *testing.T) {
	is := is.New(t)

	is.True(InputExists("Room", "temperature"))
	is.True(InputExists("room", "co2"))
	is.True(!InputExists("room", "volume"))
	is.True(!InputExists("unknown", "temperature"))
}
