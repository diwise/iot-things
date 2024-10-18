package things

import (
	"encoding/json"
	"slices"
	"strings"
	"time"
)

type Thing interface {
	ID() string
	Type() string
	Tenant() string
	LatLon() (float64, float64)
	Handle(m Measurement, onchange func(m ValueProvider) error) error
	Byte() []byte

	SetLastObserved(v Measurement, ts time.Time)
	AddDevice(deviceID string)
	AddTag(tag string)
}

func newThingImpl(id, t string, l Location, tenant string) thingImpl {
	return thingImpl{
		ID_:      id,
		Type_:    t,
		Location: l,
		Tenant_:  tenant,
	}
}

type thingImpl struct {
	ID_         string        `json:"id"`
	Type_       string        `json:"type"`
	SubType     *string       `json:"subType,omitempty"`
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Location    Location      `json:"location,omitempty"`
	Area        *LineSegments `json:"area,omitempty"`
	RefDevices  []Device      `json:"refDevices,omitempty"`
	Tags        []string      `json:"tags,omitempty"`
	Tenant_     string        `json:"tenant"`
	ObservedAt  time.Time     `json:"observedAt,omitempty"`
}

// TODO: LineString

type Point []float64     // [x, y]
type Line []Point        // [Point, Point]
type LineSegments []Line // [Line, Line, ...]

type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type Device struct {
	DeviceID     string                 `json:"device_id"`
	Measurements map[string]Measurement `json:"values,omitempty"`
}

func (t *thingImpl) ID() string {
	return t.ID_
}
func (t *thingImpl) Type() string {
	return t.Type_
}
func (t *thingImpl) Tenant() string {
	return t.Tenant_
}
func (t *thingImpl) LatLon() (float64, float64) {
	return t.Location.Latitude, t.Location.Longitude
}
func (t *thingImpl) AddDevice(deviceID string) {
	exists := slices.ContainsFunc(t.RefDevices, func(device Device) bool {
		return device.DeviceID == deviceID
	})
	if !exists {
		t.RefDevices = append(t.RefDevices, Device{DeviceID: deviceID})
	}
}

func (t *thingImpl) AddTag(tag string) {
	exists := slices.Contains(t.Tags, tag)
	if !exists {
		t.Tags = append(t.Tags, tag)
	}
}

func (c *thingImpl) SetLastObserved(m Measurement, ts time.Time) {
	for i := range c.RefDevices {
		if c.RefDevices[i].DeviceID == m.DeviceID() {
			if c.RefDevices[i].Measurements == nil {
				c.RefDevices[i].Measurements = make(map[string]Measurement)
			}
			c.RefDevices[i].Measurements[m.ID] = m
		}
	}

	if ts.After(c.ObservedAt) {
		c.ObservedAt = ts
	}
}

func (c *thingImpl) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
func (c *thingImpl) Handle(v Value, onchange func(m ValueProvider) error) error {
	return nil
}

/* --------------------- Measurements --------------------- */

type ValueProvider interface {
	Values() []Value
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

type Measurement struct {
	ID          string    `json:"id"`
	Urn         string    `json:"urn"`
	BoolValue   *bool     `json:"vb,omitempty"`
	StringValue *string   `json:"vs,omitempty"`
	Value       *float64  `json:"v,omitempty"`
	Unit        string    `json:"unit,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
}

func (m Measurement) HasDistance() bool {
	return m.Urn == DistanceURN && m.Value != nil
}
func (m Measurement) HasDigitalInput() bool {
	return m.Urn == DigitalInputURN && m.BoolValue != nil
}
func (m Measurement) HasTemperature() bool {
	return m.Urn == TemperatureURN && m.Value != nil
}
func (m Measurement) HasPresence() bool {
	return m.Urn == PresenceURN && m.BoolValue != nil
}
func (m Measurement) HasPower() bool {
	return m.Urn == PowerURN && m.Value != nil
}
func (m Measurement) HasEnergy() bool {
	return m.Urn == EnergyURN && m.Value != nil
}
func (m Measurement) HasWaterMeter() bool {
	return m.Urn == WaterMeterURN && (m.Value != nil || m.BoolValue != nil)
}

func (m Measurement) DeviceID() string {
	return strings.Split(m.ID, "/")[0]
}
