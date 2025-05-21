package things

import (
	"context"
	"encoding/json"
	"errors"
)

type PointOfInterest struct {
	thingImpl
	Temperature Measurement `json:"temperature"`
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

	temp := NewTemperatureFromMeasurement(poi.ID(), m)
	err := onchange(temp)
	if err != nil {
		return err
	}

	/*
		t := *m.Value
		n := 1

		for _, ref := range poi.RefDevices {
			if ref.DeviceID != m.ID {
				for _, v := range ref.Measurements {
					if hasTemperature(&v) {
						t += *v.Value
						n++
					}
				}
			}
		}

		avgTemp := t / float64(n)
	*/

	poi.Temperature = Measurement{
		Value:     m.Value,
		Source:    m.Source,
		Timestamp: m.Timestamp,
	}

	return nil
}

func (poi *PointOfInterest) Byte() []byte {
	b, _ := json.Marshal(poi)
	return b
}
