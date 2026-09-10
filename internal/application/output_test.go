package application

import (
	"testing"

	"github.com/diwise/iot-things/internal/application/things"
	"github.com/matryer/is"
)

// T7.5: refDevices härleds ur bindningarna i utdata så att thing.updated och
// API är oförändrade, och bindningar (och interna fält) hanteras korrekt.
func TestStripInternalStateDerivesRefDevicesFromBindings(t *testing.T) {
	is := is.New(t)

	room := things.NewRoom("room-1", things.DefaultLocation, "default")
	room.AddBinding(things.Binding{DeviceID: "device-a", Object: things.TemperatureURN, Resource: "5700", Input: "temperature"})
	room.AddBinding(things.Binding{DeviceID: "device-a", Object: things.HumidityURN, Resource: "5700", Input: "humidity"})
	room.AddBinding(things.Binding{DeviceID: "device-b", Object: things.TemperatureURN, Resource: "5700", Input: "temperature"})

	m := removeInternalState(room)

	refDevices, ok := m["refDevices"].([]any)
	is.True(ok)
	is.Equal(len(refDevices), 2) // distinkta enheter

	ids := map[string]bool{}
	for _, rd := range refDevices {
		ids[rd.(map[string]any)["deviceID"].(string)] = true
	}
	is.True(ids["device-a"])
	is.True(ids["device-b"])

	// Interna fält ska vara borta.
	for k := range m {
		if len(k) > 0 && k[0] == '_' {
			t.Fatalf("internal field %q leaked to output", k)
		}
	}
}
