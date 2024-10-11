package things

import (
	"fmt"
	"strings"
	"time"
)

type Measurements interface {
	Measurements() []Value
}

func NewFillingLevel(id, ref string, percentage, level float64, ts time.Time) FillingLevel {
	return FillingLevel{
		Percentage: newActualFillingPercentage(id, ref, ts, percentage),
		Level:      newActualFillingLevel(id, ref, ts, level),
	}
}

type FillingLevel struct {
	Percentage Value
	Level      Value
}

func (f FillingLevel) Measurements() []Value {
	return []Value{f.Percentage, f.Level}
}

func newActualFillingPercentage(id, ref string, ts time.Time, value float64) Value {
	id = fmt.Sprintf("%s/%s/%s", id, "3435", "2")
	return newValue(id, "urn:oma:lwm2m:ext:3435", ref, "%", ts, value)
}

func newActualFillingLevel(id, ref string, ts time.Time, value float64) Value {
	id = fmt.Sprintf("%s/%s/%s", id, "3435", "3")
	return newValue(id, "urn:oma:lwm2m:ext:3435", ref, "m", ts, value)
}

func NewPeopleCounter(id, ref string, daily, cumulated int64, ts time.Time) PeopleCounter {
	return PeopleCounter{
		DailyNumberOfPassages:     newDailyNumberOfPassages(id, ref, ts, daily),
		CumulatedNumberOfPassages: newCumulatedNumberOfPassages(id, ref, ts, cumulated),
	}
}

type PeopleCounter struct {
	DailyNumberOfPassages     Value
	CumulatedNumberOfPassages Value
}

func (p PeopleCounter) Measurements() []Value {
	return []Value{p.DailyNumberOfPassages, p.CumulatedNumberOfPassages}
}

func newDailyNumberOfPassages(id, ref string, ts time.Time, value int64) Value {
	id = fmt.Sprintf("%s/%s/%s", id, "3434", "5")
	return newValue(id, "urn:oma:lwm2m:ext:3434", ref, "", ts, float64(value))
}

func newCumulatedNumberOfPassages(id, ref string, ts time.Time, value int64) Value {
	id = fmt.Sprintf("%s/%s/%s", id, "3434", "6")
	return newValue(id, "urn:oma:lwm2m:ext:3434", ref, "", ts, float64(value))
}

func NewDoor(id, ref string, state bool, ts time.Time) Door {
	return Door{
		Status: newDoorState(id, ref, ts, state),
	}
}

type Door struct {
	Status Value
}

func (d Door) Measurements() []Value {
	return []Value{d.Status}
}

func newDoorState(id, ref string, ts time.Time, state bool) Value {
	id = fmt.Sprintf("%s/%s/%s", id, "10351", "50")
	return newBoolValue(id, "urn:oma:lwm2m:x:10351", ref, "", ts, state)
}

type Temperature struct {
	Value Value `json:"value"`
}

func (t Temperature) Measurements() []Value {
	return []Value{t.Value}
}

func NewTemperature(id, ref string, value float64, ts time.Time) Temperature {
	id = fmt.Sprintf("%s/%s/%s", id, "3303", "5700")
	return Temperature{
		Value: newValue(id, "urn:oma:lwm2m:ext:3303", ref, "Cel", ts, value),
	}
}

func newValue(id, urn, ref, unit string, ts time.Time, value float64) Value {
	return Value{
		ID:        id,
		Urn:       urn,
		Value:     &value,
		Unit:      unit,
		Timestamp: ts,
		Ref:       ref,
	}
}

func newBoolValue(id, urn, ref, unit string, ts time.Time, value bool) Value {
	return Value{
		ID:        id,
		Urn:       urn,
		BoolValue: &value,
		Unit:      unit,
		Timestamp: ts,
		Ref:       ref,
	}
}

type Value struct {
	ID          string    `json:"id"`
	Urn         string    `json:"urn"`
	BoolValue   *bool     `json:"vb,omitempty"`
	StringValue *string   `json:"vs,omitempty"`
	Value       *float64  `json:"v,omitempty"`
	Unit        string    `json:"unit,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
	Ref         string    `json:"ref,omitempty"`
}

func (m Value) HasDistance() bool {
	return m.Urn == "urn:oma:lwm2m:ext:3330" && m.Value != nil
}
func (m Value) HasDigitalInput() bool {
	return m.Urn == "urn:oma:lwm2m:ext:3200" && m.BoolValue != nil
}
func (m Value) HasTemperature() bool {
	return m.Urn == "urn:oma:lwm2m:ext:3303" && m.Value != nil
}
func (m Value) DeviceID() string {
	return strings.Split(m.ID, "/")[0]
}

type Device struct {
	DeviceID string           `json:"device_id"`
	Values   map[string]Value `json:"values,omitempty"`
}

type PumpingStation struct {
	thingImpl
}

type Sewer struct {
	thingImpl
	FillingLevel *Value `json:"filling_level,omitempty"`
	Percent      *Value `json:"percent,omitempty"`
}

type Lifebuoy struct {
	thingImpl
	Presence *bool `json:"presence,omitempty"`
}

type WaterMeter struct {
	thingImpl
	CumulativeVolume Value `json:"cumulative_volume"`
	Leakage          *bool `json:"leakage,omitempty"`
	Burst            *bool `json:"burst,omitempty"`
	Backflow         *bool `json:"backflow,omitempty"`
	Fraud            *bool `json:"fraud,omitempty"`
}
