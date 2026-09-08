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
