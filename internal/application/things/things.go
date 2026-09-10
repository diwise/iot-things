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
	// Apply kör typens logik för en namngiven ingång. Ingången kommer från en
	// bindning (eller, i bryggläget, från typens ingångstabell).
	Apply(ctx context.Context, input string, m Measurement, onchange func(m ValueProvider) error) error
	Refs() []Device

	SetLastObserved(measurements []Measurement)
	AddDevice(deviceID string)
	AddTag(tag string)

	// LastMessageID/SetLastMessageID används för deduplicering: en rapport
	// som redan behandlats ska inte köra tillståndsmaskinen igen vid retry.
	LastMessageID() string
	SetLastMessageID(id string)
}

type ThingType struct {
	Type    string `json:"type"`
	SubType string `json:"subType,omitempty"`
	Name    string `json:"name"`
}

func newBase(id, t string, l Location, tenant string) Base {
	return Base{
		ID_:      id,
		Type_:    t,
		Location: l,
		Tenant_:  tenant,
	}
}

type Base struct {
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

	// LastMessageID_ spårar senast behandlade rapport. Internt fält (strippas
	// före publicering) och används för idempotens vid återleverans.
	LastMessageID_ string `json:"_lastMessageId,omitempty"`
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

func (t *Base) ID() string {
	return t.ID_
}
func (t *Base) Type() string {
	return t.Type_
}
func (t *Base) Tenant() string {
	return t.Tenant_
}
func (t *Base) LatLon() (float64, float64) {
	return t.Location.Latitude, t.Location.Longitude
}
func (t *Base) AddDevice(deviceID string) {
	exists := slices.ContainsFunc(t.RefDevices, func(device Device) bool {
		return device.DeviceID == deviceID
	})
	if !exists {
		t.RefDevices = append(t.RefDevices, Device{DeviceID: deviceID})
	}
}
func (t *Base) Refs() []Device {
	return t.RefDevices
}

func (t *Base) LastMessageID() string {
	return t.LastMessageID_
}

func (t *Base) SetLastMessageID(id string) {
	t.LastMessageID_ = id
}

func (t *Base) AddTag(tag string) {
	exists := slices.Contains(t.Tags, tag)
	if !exists {
		t.Tags = append(t.Tags, tag)
	}
}

func (c *Base) SetLastObserved(measurements []Measurement) {
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

					// En sen anländande (äldre) mätning får inte skriva
					// över en nyare i cachen. Lika tidsstämpel får uppdatera.
					existing, ok := c.RefDevices[i].Measurements[m.ID]
					if !ok || !m.Timestamp.Before(existing.Timestamp) {
						c.RefDevices[i].Measurements[m.ID] = m
					}
				}
			}
		}
	}

	if lastObserved.IsZero() {
		lastObserved = time.Now()
	}

	c.ObservedAt = lastObserved
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
			Timestamp: ts.UTC(),
			Ref:       ref,
		},
	}
}

func newBoolValue(id, urn, ref, unit string, ts time.Time, value bool) Value {
	return Value{
		Measurement: Measurement{
			ID:        id,
			Urn:       urn,
			BoolValue: &value,
			Unit:      unit,
			Timestamp: ts.UTC(),
			Ref:       ref,
		},
	}
}

// Value är en mätning som kan skickas till lagring. Ref finns på
// Measurement; Value har inget eget skuggande fält.
type Value struct {
	Measurement
}

type Measurement struct {
	ID          string    `json:"id,omitzero"`
	Urn         string    `json:"urn,omitzero"`
	BoolValue   *bool     `json:"vb,omitempty"`
	StringValue *string   `json:"vs,omitempty"`
	Value       *float64  `json:"v,omitempty"`
	Unit        string    `json:"unit,omitempty"`
	Timestamp   time.Time `json:"timestamp,omitzero"`
	Source      *string   `json:"source,omitzero"`
	Ref         string    `json:"ref,omitempty"`
}

// hasX avgör om en mätning hör till en viss signaltyp. Regeln är: matcha på
// observationens URN. Objekt med flera resurser (t.ex. Watermeter 3424 och
// Room 3428) särskiljs dessutom med resurs-suffix i respektive hanterare.
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

// hasCO2 matchar AirQuality-objektets CO2-resurs (17) så att aggregeringen
// inte blandar in andra resurser i samma objekt, t.ex. partiklar.
func hasCO2(m *Measurement) bool {
	return m.Urn == AirQualityURN && m.Value != nil && strings.HasSuffix(m.ID, "/17")
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

// avg beräknar medelvärdet av aktuell mätning och de senast cachade
// mätningarna som hör till samma signal (namngivna ingång). Den aktuella
// signalen undantas via sitt fulla recordnamn, så den inte dubbelräknas,
// medan andra signaler på samma enhet (t.ex. en annan kanal) räknas med.
func avg(r Thing, current Measurement, v float64, has func(m *Measurement) bool) float64 {
	n := 1

	for _, refDevice := range r.Refs() {
		for _, m := range refDevice.Measurements {
			if m.ID == current.ID {
				continue
			}
			if has(&m) {
				v += *m.Value
				n++
			}
		}
	}

	return v / float64(n)
}

// ShouldApply rapporterar om mätningen är nyare än, eller lika gammal som,
// det cachade värdet för samma signal. Äldre mätningar får inte ändra
// aktuellt tillstånd, även om de sparas som historik.
func ShouldApply(t Thing, m Measurement) bool {
	for _, d := range t.Refs() {
		if cached, ok := d.Measurements[m.ID]; ok && cached.Timestamp.After(m.Timestamp) {
			return false
		}
	}
	return true
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
