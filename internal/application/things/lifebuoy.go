package things

import (
	"context"
	"errors"
)

type Lifebuoy struct {
	Base
	Presence bool `json:"presence"`
}

func NewLifebuoy(id string, l Location, tenant string) Thing {
	thing := newBase(id, "Lifebuoy", l, tenant)
	return &Lifebuoy{
		Base: thing,
	}
}

func (l *Lifebuoy) Handle(ctx context.Context, m []Measurement, onchange func(m ValueProvider) error) error {
	errs := []error{}

	for _, v := range m {
		errs = append(errs, l.handle(ctx, v, onchange))
	}

	return errors.Join(errs...)
}

func (l *Lifebuoy) handle(ctx context.Context, m Measurement, onchange func(m ValueProvider) error) error {
	if input, ok := resolveInput("lifebuoy", m); ok {
		return l.Apply(ctx, input, m, onchange)
	}
	return nil
}

func (l *Lifebuoy) Apply(ctx context.Context, input string, m Measurement, onchange func(m ValueProvider) error) error {
	if input != "presence" {
		return nil
	}

	if !(hasDigitalInput(&m) || hasPresence(&m)) {
		return nil
	}

	if !hasChanged(l.Presence, *m.BoolValue) {
		return nil
	}

	l.Presence = *m.BoolValue
	presence := NewPresence(l.ID(), m.ID, l.Presence, m.Timestamp)

	return onchange(presence)
}
