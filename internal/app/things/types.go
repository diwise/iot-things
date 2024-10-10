package things

import (
	"encoding/json"
	"slices"
	"time"
)

type Thing interface {
	ID() string
	Type() string
	Tenant() string
	LatLon() (float64, float64)
	Handle(m Measurement, onchange func(m Measurement) error) error
	Byte() []byte

	AddDevice(deviceID string)
	AddTag(tag string)
}

type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
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
	return m.Urn == "urn:oma:lwm2m:ext:3330" && m.Value != nil
}

type Device struct {
	DeviceID string `json:"device_id"`
}

func newThing(id, t string, l Location, tenant string) thing {
	return thing{
		ID_:      id,
		Type_:    t,
		Location: l,
		Tenant_:  tenant,
	}
}

type thing struct {
	ID_          string        `json:"id"`
	Type_        string        `json:"type"`
	SubType      *string       `json:"sub_type,omitempty"`
	Name         string        `json:"name"`
	Description  string        `json:"description"`
	Location     Location      `json:"location,omitempty"`
	Measurements []Measurement `json:"-"`
	RefDevices   []Device      `json:"ref_devices,omitempty"`
	Tags         []string      `json:"tags,omitempty"`
	Tenant_      string        `json:"tenant"`
}

func (t *thing) ID() string {
	return t.ID_
}
func (t *thing) Type() string {
	return t.Type_
}
func (t *thing) Tenant() string {
	return t.Tenant_
}
func (t *thing) LatLon() (float64, float64) {
	return t.Location.Latitude, t.Location.Longitude
}
func (t *thing) AddDevice(deviceID string) {
	exists := slices.ContainsFunc(t.RefDevices, func(device Device) bool {
		return device.DeviceID == deviceID
	})
	if !exists {
		t.RefDevices = append(t.RefDevices, Device{DeviceID: deviceID})
	}
}
func (t *thing) AddTag(tag string) {
	exists := slices.Contains(t.Tags, tag)
	if !exists {
		t.Tags = append(t.Tags, tag)
	}
}
func (c *thing) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
func (c *thing) Handle(m Measurement, onchange func(m Measurement) error) error {
	return nil
}

type Container struct {
	thing
	FillingLevel *Measurement `json:"filling_level,omitempty"`
	Percent      *Measurement `json:"percent,omitempty"`
	MaxDistance  float64      `json:"max_depth"`
	MaxLevel     float64      `json:"max_level"`
	MeanLevel    float64      `json:"mean_level"`
	Offset       float64      `json:"offset"`
	Angle        float64      `json:"angle"`
}

type PumpingStation struct {
	thing
}

type Room struct {
	thing
	Temperature float64 `json:"temperature"`
}

type Sewer struct {
	thing
	FillingLevel *Measurement `json:"filling_level,omitempty"`
	Percent      *Measurement `json:"percent,omitempty"`
}

type Passage struct {
	thing
	Count *Measurement `json:"count,omitempty"`
}

type Lifebuoy struct {
	thing
	Presence *bool `json:"presence,omitempty"`
}

type WaterMeter struct {
	thing
	CumulativeVolume Measurement `json:"cumulative_volume"`
	Leakage          *bool       `json:"leakage,omitempty"`
	Burst            *bool       `json:"burst,omitempty"`
	Backflow         *bool       `json:"backflow,omitempty"`
	Fraud            *bool       `json:"fraud,omitempty"`
}
