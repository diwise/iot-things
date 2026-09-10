package things

import (
	"testing"
	"time"

	"github.com/matryer/is"
)

func floatPtr(v float64) *float64 { return &v }

// T2: avg ska inte dubbelräkna den aktuella enhetens redan cachade värde.
func TestAvgDoesNotDoubleCountCurrentDevice(t *testing.T) {
	is := is.New(t)

	r := NewRoom("room-1", DefaultLocation, "default")
	r.AddDevice("device-1")
	r.AddDevice("device-2")

	room := r.(*Room)
	room.RefDevices[0].Measurements = map[string]Measurement{
		"device-1/3303/5700": {ID: "device-1/3303/5700", Urn: TemperatureURN, Value: floatPtr(20.0)},
	}
	room.RefDevices[1].Measurements = map[string]Measurement{
		"device-2/3303/5700": {ID: "device-2/3303/5700", Urn: TemperatureURN, Value: floatPtr(30.0)},
	}

	current := Measurement{ID: "device-1/3303/5700", Urn: TemperatureURN, Value: floatPtr(20.0)}

	// (20 + 30) / 2 = 25, inte (20 + 20 + 30) / 3.
	is.Equal(avg(room, current, 20.0, hasTemperature), 25.0)
}

// T2: hasChanged ska fungera även när det sparade värdet är en Measurement.
func TestHasChangedMeasurement(t *testing.T) {
	is := is.New(t)

	stored := Measurement{Value: floatPtr(20.0)}
	is.True(!hasChanged(stored, 20.0))
	is.True(hasChanged(stored, 20.5))

	empty := Measurement{}
	is.True(hasChanged(empty, 20.0))

	storedBool := Measurement{BoolValue: boolPtrT2(true)}
	is.True(!hasChanged(storedBool, true))
	is.True(hasChanged(storedBool, false))
	is.True(hasChanged(Measurement{}, true))
}

func boolPtrT2(v bool) *bool { return &v }

// Fynd 1: ändringskontrollen måste jämföra aggregerat värde mot aggregerat
// värde. Här rapporterar sensor A exakt det sparade medelvärdet (25); det nya
// medelvärdet blir ändå 27.5 och måste publiceras.
func TestAggregateChangeDetection(t *testing.T) {
	is := is.New(t)

	room := NewRoom("room", DefaultLocation, "default").(*Room)
	room.AddDevice("device-a")
	room.AddDevice("device-b")

	// device-b har redan ett cachat värde.
	room.RefDevices[1].Measurements = map[string]Measurement{
		"device-b/3303/5700": {ID: "device-b/3303/5700", Urn: TemperatureURN, Value: floatPtr(30)},
	}
	room.Temperature = Measurement{Value: floatPtr(25)} // sparat medelvärde

	reported := 25.0
	current := Measurement{ID: "device-a/3303/5700", Urn: TemperatureURN, Value: &reported, Timestamp: time.Now()}

	var emitted []Value
	is.NoErr(room.handleTemperature(current, collectValues(&emitted)))

	is.Equal(len(emitted), 1)
	is.Equal(*room.Temperature.Value, 27.5)
}

// Samma sak för skalära fält (fuktighet är float64).
func TestAggregateChangeDetectionScalar(t *testing.T) {
	is := is.New(t)

	room := NewRoom("room", DefaultLocation, "default").(*Room)
	room.AddDevice("device-a")
	room.AddDevice("device-b")

	room.RefDevices[1].Measurements = map[string]Measurement{
		"device-b/3304/5700": {ID: "device-b/3304/5700", Urn: HumidityURN, Value: floatPtr(30)},
	}
	room.Humidity = 25 // sparat medelvärde

	reported := 25.0
	current := Measurement{ID: "device-a/3304/5700", Urn: HumidityURN, Value: &reported, Timestamp: time.Now()}

	var emitted []Value
	is.NoErr(room.handleHumidity(current, collectValues(&emitted)))

	is.Equal(len(emitted), 1)
	is.Equal(room.Humidity, 27.5)
}
