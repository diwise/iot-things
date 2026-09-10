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
		errs = append(errs, d.handle(ctx, v, onchange))
	}

	return errors.Join(errs...)
}

func (d *Desk) handle(ctx context.Context, m Measurement, onchange func(m ValueProvider) error) error {
	if input, ok := resolveInput("desk", m); ok {
		return d.Apply(ctx, input, m, onchange)
	}
	return nil
}

func (d *Desk) Apply(ctx context.Context, input string, m Measurement, onchange func(m ValueProvider) error) error {
	if input != "presence" {
		return nil
	}

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
