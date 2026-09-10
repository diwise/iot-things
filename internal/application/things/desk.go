package things

import (
	"context"
	"errors"
)

type Desk struct {
	Base
	Presence bool `json:"presence"`
}

func NewDesk(id string, l Location, tenant string) Thing {
	thing := newBase(id, "Desk", l, tenant)
	return &Desk{
		Base: thing,
	}
}

func (d *Desk) Handle(ctx context.Context, m []Measurement, onchange func(m ValueProvider) error) error {
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
