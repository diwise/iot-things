package things

import (
	"encoding/json"
	"errors"
)

type Desk struct {
	thingImpl
	Presence bool `json:"presence"`
}

func NewDesk(id string, l Location, tenant string) Thing {
	thing := newThingImpl(id, "Desk", l, tenant)
	return &Lifebuoy{
		thingImpl: thing,
	}
}

func (d *Desk) Handle(m []Measurement, onchange func(m ValueProvider) error) error {
	errs := []error{}

	for _, v := range m {
		errs = append(errs, d.handle(v, onchange))
	}

	return errors.Join(errs...)
}

func (d *Desk) handle(m Measurement, onchange func(m ValueProvider) error) error {
	if !(hasDigitalInput(&m) || hasPresence(&m)) {
		return nil
	}

	if !hasChanged(d.Presence, *m.BoolValue) {
		return nil
	}

	d.Presence = *m.BoolValue
	presence := NewPresence(d.ID(), m.ID, d.Presence, m.Timestamp)

	return onchange(presence)
}

func (l *Desk) Byte() []byte {
	b, _ := json.Marshal(l)
	return b
}
