package types

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/matryer/is"
)

// HARM-003: locks the thing.updated wire contract produced by iot-things
// and consumed by iot-transform-fiware (which decodes the same envelope
// shape). Topic name, content type pattern and JSON field names must not
// change without a compatibility/migration plan and consumer-side tests.
func TestThingUpdatedWireContract(t *testing.T) {
	is := is.New(t)

	thing := map[string]any{
		"id":     "2bf440f4",
		"type":   "Container",
		"tenant": "default",
	}

	m := &ThingUpdated{
		ID:        "2bf440f4",
		Type:      "Container",
		Thing:     thing,
		Tenant:    "default",
		Timestamp: time.Date(2024, 11, 19, 10, 49, 59, 0, time.UTC),
	}

	is.Equal(m.TopicName(), "thing.updated")
	is.Equal(m.ContentType(), "application/vnd.diwise.container+json")

	var decoded map[string]any
	is.NoErr(json.Unmarshal(m.Body(), &decoded))

	is.Equal(decoded["id"], "2bf440f4")
	is.Equal(decoded["type"], "Container")
	is.Equal(decoded["tenant"], "default")

	_, ok := decoded["thing"]
	is.True(ok)
	_, ok = decoded["timestamp"]
	is.True(ok)
}

// REV-016: locks the complete thing.updated wire representation with a
// non-default tenant, fixed measurement time and a nested thing
// payload. Nested values, tenant and timestamp changes fail here.
func TestThingUpdatedGoldenBody(t *testing.T) {
	is := is.New(t)

	thing := map[string]any{
		"id":       "2bf440f4",
		"type":     "Container",
		"tenant":   "acme",
		"percent":  56.0,
		"location": map[string]any{"latitude": 62.39, "longitude": 17.31},
	}

	m := &ThingUpdated{
		ID:        "2bf440f4",
		Type:      "Container",
		Thing:     thing,
		Tenant:    "acme",
		Timestamp: time.Date(2024, 11, 19, 10, 49, 59, 0, time.UTC),
	}

	const golden = `{"id":"2bf440f4","type":"Container","thing":{"id":"2bf440f4","location":{"latitude":62.39,"longitude":17.31},"percent":56,"tenant":"acme","type":"Container"},"tenant":"acme","timestamp":"2024-11-19T10:49:59Z"}`
	is.Equal(string(m.Body()), golden)

	var decoded map[string]any
	is.NoErr(json.Unmarshal([]byte(golden), &decoded))

	nested, ok := decoded["thing"].(map[string]any)
	is.True(ok)
	is.Equal(nested["percent"], 56.0)
	is.Equal(nested["tenant"], "acme")

	location, ok := nested["location"].(map[string]any)
	is.True(ok)
	is.Equal(location["latitude"], 62.39)
	is.Equal(location["longitude"], 17.31)

	is.Equal(decoded["timestamp"], "2024-11-19T10:49:59Z")
}

// HARM-003: locks the per-type content type pattern derived from the
// thing type name.
func TestThingUpdatedContentTypePattern(t *testing.T) {
	is := is.New(t)

	for thingType, expected := range map[string]string{
		"Building":        "application/vnd.diwise.building+json",
		"Container":       "application/vnd.diwise.container+json",
		"Desk":            "application/vnd.diwise.desk+json",
		"Lifebuoy":        "application/vnd.diwise.lifebuoy+json",
		"Passage":         "application/vnd.diwise.passage+json",
		"PointOfInterest": "application/vnd.diwise.pointofinterest+json",
		"PumpingStation":  "application/vnd.diwise.pumpingstation+json",
		"Room":            "application/vnd.diwise.room+json",
		"Sewer":           "application/vnd.diwise.sewer+json",
	} {
		m := &ThingUpdated{ID: "id", Type: thingType, Tenant: "default"}
		is.Equal(m.ContentType(), expected)
	}
}
