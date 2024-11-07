package things

import (
	"encoding/json"
	"errors"
)

type Room struct {
	thingImpl
	Temperature float64 `json:"temperature"`
}

func NewRoom(id string, l Location, tenant string) Thing {
	return &Room{
		thingImpl: newThingImpl(id, "Room", l, tenant),
	}
}

func (r *Room) Handle(m []Measurement, onchange func(m ValueProvider) error) error {
	errs := []error{}

	for _, v := range m {
		errs = append(errs, r.handle(v, onchange))
	}

	return errors.Join(errs...)
}

func (r *Room) handle(m Measurement, onchange func(m ValueProvider) error) error {
	if !m.HasTemperature() {
		return nil
	}

	if !hasChanged(r.Temperature, *m.Value) {
		return nil
	}

	temp := NewTemperature(r.ID(), m.ID, *m.Value, m.Timestamp)
	err := onchange(temp)
	if err != nil {
		return err
	}

	t := *m.Value
	n := 1

	for _, ref := range r.RefDevices {
		if ref.DeviceID != m.ID {
			for _, v := range ref.Measurements {
				if v.HasTemperature() {
					t += *v.Value
					n++
				}
			}
		}
	}

	r.Temperature = t / float64(n)

	return nil
}

func (r *Room) Byte() []byte {
	b, _ := json.Marshal(r)
	return b
}
