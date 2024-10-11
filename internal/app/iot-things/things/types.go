package things

import (
	"fmt"
	"time"
)



func newActualFillingPercentage(id, ref string, ts time.Time, value float64) Measurement {
	id = fmt.Sprintf("%s/%s/%s", id, "3435", "2")
	return newValue(id, "urn:oma:lwm2m:ext:3435", ref, "%", ts, value)
}

func newActualFillingLevel(id, ref string, ts time.Time, value float64) Measurement {
	id = fmt.Sprintf("%s/%s/%s", id, "3435", "3")
	return newValue(id, "urn:oma:lwm2m:ext:3435", ref, "m", ts, value)
}

func newValue(id, urn, ref, unit string, ts time.Time, value float64) Measurement {
	return Measurement{
		ID:        id,
		Urn:       urn,
		Value:     &value,
		Unit:      unit,
		Timestamp: ts,
		Ref:       ref,
	}
}

type Measurement struct {
	ID          string    `json:"id"`
	Urn         string    `json:"urn"`
	BoolValue   *bool     `json:"vb,omitempty"`
	StringValue *string   `json:"vs,omitempty"`
	Value       *float64  `json:"v,omitempty"`
	Unit        string    `json:"unit,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
	Ref         string    `json:"ref,omitempty"`
}

func (m Measurement) HasDistance() bool {
	return m.Urn == "urn:oma:lwm2m:ext:3330" && m.Value != nil
}

type Device struct {
	DeviceID string `json:"device_id"`
}

type PumpingStation struct {
	thingImpl
}

type Room struct {
	thingImpl
	Temperature float64 `json:"temperature"`
}

type Sewer struct {
	thingImpl
	FillingLevel *Measurement `json:"filling_level,omitempty"`
	Percent      *Measurement `json:"percent,omitempty"`
}

type Passage struct {
	thingImpl
	Count *Measurement `json:"count,omitempty"`
}

type Lifebuoy struct {
	thingImpl
	Presence *bool `json:"presence,omitempty"`
}

type WaterMeter struct {
	thingImpl
	CumulativeVolume Measurement `json:"cumulative_volume"`
	Leakage          *bool       `json:"leakage,omitempty"`
	Burst            *bool       `json:"burst,omitempty"`
	Backflow         *bool       `json:"backflow,omitempty"`
	Fraud            *bool       `json:"fraud,omitempty"`
}
