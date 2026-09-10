package things

import (
	"testing"

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
