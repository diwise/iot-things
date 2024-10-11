package things

import (
	"encoding/json"
	"slices"
	"strings"
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

func (c *PumpingStation) Handle(v Value, onchange func(m Measurements) error) error {
	return nil
}

func (c *PumpingStation) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}

func (c *Sewer) Handle(v Value, onchange func(m Measurements) error) error {
	return nil
}

func (c *Sewer) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}

func (c *Lifebuoy) Handle(v Value, onchange func(m Measurements) error) error {
	return nil
}

func (c *Lifebuoy) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}

func (c *WaterMeter) Handle(v Value, onchange func(m Measurements) error) error {
	return nil
}

func (c *WaterMeter) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
