package things

import (
	"context"
	"encoding/json"
	"errors"
)

type PointOfInterest struct {
	thingImpl
	Temperature float64 `json:"temperature"`
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

	temp := NewTemperature(poi.ID(), m.ID, *m.Value, m.Timestamp)
	err := onchange(temp)
	if err != nil {
		return err
	}

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

	poi.Temperature = t / float64(n)

	return nil
}

func (poi *PointOfInterest) Byte() []byte {
	b, _ := json.Marshal(poi)
	return b
}
