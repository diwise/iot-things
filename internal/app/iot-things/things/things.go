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
	Handle(v Value, onchange func(m Measurements) error) error
	Byte() []byte

	SetValue(v Value)
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
	ID_         string   `json:"id"`
	Type_       string   `json:"type"`
	SubType     *string  `json:"sub_type,omitempty"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Location    Location `json:"location,omitempty"`
	RefDevices  []Device `json:"ref_devices,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	Tenant_     string   `json:"tenant"`
}

type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type Device struct {
	DeviceID string           `json:"device_id"`
	Values   map[string]Value `json:"values,omitempty"`
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

func (c *thingImpl) SetValue(v Value) {
	for i, ref := range c.RefDevices {
		if strings.EqualFold(ref.DeviceID, v.DeviceID()) {
			if c.RefDevices[i].Values == nil {
				c.RefDevices[i].Values = make(map[string]Value)
			}
			c.RefDevices[i].Values[v.ID] = v
		}
	}
}

func (c *thingImpl) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
func (c *thingImpl) Handle(v Value, onchange func(m Measurements) error) error {
	return nil
}

/* --------------------- Measurements --------------------- */

type Measurements interface {
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

func (m Value) HasDistance() bool {
	return m.Urn == "urn:oma:lwm2m:ext:3330" && m.Value != nil
}
func (m Value) HasDigitalInput() bool {
	return m.Urn == "urn:oma:lwm2m:ext:3200" && m.BoolValue != nil
}
func (m Value) HasTemperature() bool {
	return m.Urn == "urn:oma:lwm2m:ext:3303" && m.Value != nil
}
func (m Value) HasPresence() bool {
	return m.Urn == "urn:oma:lwm2m:ext:3302" && m.BoolValue != nil
}

func (m Value) DeviceID() string {
	return strings.Split(m.ID, "/")[0]
}
