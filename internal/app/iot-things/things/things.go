package things

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"
)

type Thing interface {
	ID() string
	Type() string
	Tenant() string
	LatLon() (float64, float64)
	Handle(ctx context.Context, m []Measurement, onchange func(m ValueProvider) error) error
	Byte() []byte
	Refs() []Device

	SetLastObserved(measurements []Measurement)
	AddDevice(deviceID string)
	AddTag(tag string)
}

type ThingType struct {
	Type    string `json:"type"`
	SubType string `json:"subType,omitempty"`
	Name    string `json:"name"`
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
	ID_             string        `json:"id"`
	Type_           string        `json:"type"`
	SubType         *string       `json:"subType,omitempty"`
	Name            string        `json:"name"`
	AlternativeName string        `json:"alternativeName,omitempty"`
	Description     string        `json:"description,omitempty"`
	Location        Location      `json:"location"`
	Area            *LineSegments `json:"area,omitempty"`
	RefDevices      []Device      `json:"refDevices,omitempty"`
	Tags            []string      `json:"tags,omitempty"`
	Tenant_         string        `json:"tenant"`
	ObservedAt      time.Time     `json:"observedAt"`
	ValidURN        []string      `json:"validURN,omitempty"`
}

type Point []float64     // [x, y]
type Line []Point        // [Point, Point]
type LineSegments []Line // [Line, Line, ...]

type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

var DefaultLocation = Location{Latitude: 0, Longitude: 0}

type Device struct {
	DeviceID     string                 `json:"deviceID"`
	Measurements map[string]Measurement `json:"measurements,omitempty"`
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
func (t *thingImpl) Refs() []Device {
	return t.RefDevices
}

func (t *thingImpl) AddTag(tag string) {
	exists := slices.Contains(t.Tags, tag)
	if !exists {
		t.Tags = append(t.Tags, tag)
	}
}

func (c *thingImpl) SetLastObserved(measurements []Measurement) {
	lastObserved := c.ObservedAt

	for _, m := range measurements {
		if slices.Contains(c.ValidURN, m.Urn) {
			if m.Timestamp.After(lastObserved) {
				lastObserved = m.Timestamp
			}

			for i := range c.RefDevices {
				if c.RefDevices[i].DeviceID == m.DeviceID() {
					if c.RefDevices[i].Measurements == nil {
						c.RefDevices[i].Measurements = make(map[string]Measurement)
					}

					c.RefDevices[i].Measurements[m.ID] = m
				}
			}
		}
	}

	if lastObserved.IsZero() {
		lastObserved = time.Now()
	}

	c.ObservedAt = lastObserved
}

func (c *thingImpl) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}

func (c *thingImpl) Handle(v []Measurement, onchange func(m ValueProvider) error) error {
	return nil
}

/* --------------------- Measurements --------------------- */

type ValueProvider interface {
	Values() []Value
}

func newValue(id, urn, ref, unit string, ts time.Time, value float64) Value {
	return Value{
		Measurement: Measurement{
			ID:        id,
			Urn:       urn,
			Value:     &value,
			Unit:      unit,
			Timestamp: ts.UTC()},
		Ref: ref,
	}
}

func newBoolValue(id, urn, ref, unit string, ts time.Time, value bool) Value {
	return Value{
		Measurement: Measurement{
			ID:        id,
			Urn:       urn,
			BoolValue: &value,
			Unit:      unit,
			Timestamp: ts.UTC()},
		Ref: ref,
	}
}

type Value struct {
	Measurement
	Ref string `json:"ref,omitempty"`
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

func hasDistance(m *Measurement) bool {
	return m.Urn == DistanceURN && m.Value != nil
}
func hasDigitalInput(m *Measurement) bool {
	return m.Urn == DigitalInputURN && m.BoolValue != nil
}
func hasTemperature(m *Measurement) bool {
	return m.Urn == TemperatureURN && m.Value != nil
}
func hasPresence(m *Measurement) bool {
	return m.Urn == PresenceURN && m.BoolValue != nil
}
func hasHumidity(m *Measurement) bool {
	return m.Urn == HumidityURN && m.Value != nil
}
func hasIlluminance(m *Measurement) bool {
	return m.Urn == IlluminanceURN && m.Value != nil
}
func hasAirQuality(m *Measurement) bool {
	return m.Urn == AirQualityURN && m.Value != nil
}
func hasPower(m *Measurement) bool {
	return m.Urn == PowerURN && m.Value != nil
}
func hasEnergy(m *Measurement) bool {
	return m.Urn == EnergyURN && m.Value != nil
}
func hasWaterMeter(m *Measurement) bool {
	return m.Urn == WaterMeterURN && (m.Value != nil || m.BoolValue != nil)
}

func avg[T *Thing](r Thing, currentDeviceID string, v float64, has func(m *Measurement) bool) float64 {
	n := 1

	for _, refDevice := range r.Refs() {
		if refDevice.DeviceID != currentDeviceID {
			for _, m := range refDevice.Measurements {
				if has(&m) {
					v += *m.Value
					n++
				}
			}
		}
	}

	return v / float64(n)
}

func (m Measurement) DeviceID() string {
	return strings.Split(m.ID, "/")[0]
}

func ConvToThing(b []byte) (Thing, error) {
	t := struct {
		Type string `json:"type"`
	}{}
	err := json.Unmarshal(b, &t)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(t.Type) {
	case "building":
		building, err := unmarshal[Building](b)
		building.ValidURN = BuildingURNs
		return &building, err
	case "container":
		c, err := unmarshal[Container](b)
		c.ValidURN = ContainerURNs
		return &c, err
	case "lifebuoy":
		l, err := unmarshal[Lifebuoy](b)
		l.ValidURN = LifebuoyURNs
		return &l, err
	case "passage":
		p, err := unmarshal[Passage](b)
		p.ValidURN = PassageURNs
		return &p, err
	case "pointofinterest":
		poi, err := unmarshal[PointOfInterest](b)
		poi.ValidURN = PointOfInterestURNs
		return &poi, err
	case "pumpingstation":
		ps, err := unmarshal[PumpingStation](b)
		ps.ValidURN = PumpingStationURNs
		return &ps, err
	case "room":
		r, err := unmarshal[Room](b)
		r.ValidURN = RoomURNs
		return &r, err
	case "sewer":
		s, err := unmarshal[Sewer](b)
		s.ValidURN = SewerURNs
		return &s, err
	case "watermeter":
		l, err := unmarshal[Watermeter](b)
		l.ValidURN = WaterMeterURNs
		return &l, err
	case "desk":
		d, err := unmarshal[Desk](b)
		d.ValidURN = DeskURNs
		return &d, err
	case "sink":
		s, err := unmarshal[Sink](b)
		s.ValidURN = SinkURNs
		return &s, err
	default:
		return nil, errors.New("unknown thing type [" + t.Type + "]")
	}
}

func unmarshal[T any](b []byte) (T, error) {
	var m T
	err := json.Unmarshal(b, &m)
	if err != nil {
		return m, err
	}
	return m, nil
}
