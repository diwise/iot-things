package things

import (
	"encoding/json"
	"slices"
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

func newThingImpl(id, t string, l Location, tenant string) thingImpl {
	return thingImpl{
		ID_:      id,
		Type_:    t,
		Location: l,
		Tenant_:  tenant,
	}
}

type thingImpl struct {
	ID_          string        `json:"id"`
	Type_        string        `json:"type"`
	SubType      *string       `json:"sub_type,omitempty"`
	Name         string        `json:"name"`
	Description  string        `json:"description"`
	Location     Location      `json:"location,omitempty"`	
	RefDevices   []Device      `json:"ref_devices,omitempty"`
	Tags         []string      `json:"tags,omitempty"`
	Tenant_      string        `json:"tenant"`
}

type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
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
func (c *thingImpl) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
func (c *thingImpl) Handle(m Measurement, onchange func(m Measurement) error) error {
	return nil
}

func (c *PumpingStation) Handle(m Measurement, onchange func(m Measurement) error) error {
	return nil
}

func (c *PumpingStation) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}

func (c *Room) Handle(m Measurement, onchange func(m Measurement) error) error {
	return nil
}

func (c *Room) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}

func (c *Sewer) Handle(m Measurement, onchange func(m Measurement) error) error {
	return nil
}

func (c *Sewer) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}

func (c *Passage) Handle(m Measurement, onchange func(m Measurement) error) error {
	return nil
}

func (c *Passage) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}

func (c *Lifebuoy) Handle(m Measurement, onchange func(m Measurement) error) error {
	return nil
}

func (c *Lifebuoy) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}

func (c *WaterMeter) Handle(m Measurement, onchange func(m Measurement) error) error {
	return nil
}

func (c *WaterMeter) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
