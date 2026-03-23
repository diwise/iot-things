package things

import (
	"context"
	"encoding/json"
	"errors"
)

type PointOfInterest struct {
	thingImpl
	Temperature Measurement `json:"temperature"`
	Current     Measurement `json:"current"`
}

func NewBeach(id string, l Location, tenant string) Thing {
	poi := newThingImpl(id, "PointOfInterest", l, tenant)
	beach := "Beach"
	poi.SubType = &beach

	return &PointOfInterest{
		thingImpl: poi,
	}
}

func NewPointOfInterest(id string, l Location, tenant string) Thing {
	return &PointOfInterest{
		thingImpl: newThingImpl(id, "PointOfInterest", l, tenant),
	}
}
func (poi *PointOfInterest) Handle(ctx context.Context, m []Measurement, onchange func(m ValueProvider) error) error {
	errs := []error{}

	for _, v := range m {
		errs = append(errs, poi.handle(v, onchange))
	}

	return errors.Join(errs...)
}

func (poi *PointOfInterest) handle(m Measurement, onchange func(m ValueProvider) error) error {
	if !hasTemperature(&m) {
		return nil
	}

	if !hasChanged(poi.Temperature, *m.Value) {
		return nil
	}

	temp := newTemperatureFromMeasurement(poi.ID(), m)
	err := onchange(temp)
	if err != nil {
		return err
	}

	if m.Timestamp.After(poi.Temperature.Timestamp) {
		poi.Temperature = Measurement{
			Value:     m.Value,
			Source:    m.Source,
			Timestamp: m.Timestamp,
			Ref:       m.Ref,
		}
	}

	poi.Current = Measurement{
		Value:     m.Value,
		Source:    m.Source,
		Timestamp: m.Timestamp,
		Ref:       m.Ref,
	}

	return nil
}

func (poi *PointOfInterest) Byte() []byte {
	b, _ := json.Marshal(poi)
	return b
}
