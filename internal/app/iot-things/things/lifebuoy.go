package things

import (
	"context"
	"encoding/json"
	"errors"
)

type Lifebuoy struct {
	thingImpl
	Presence bool `json:"presence"`
}

func NewLifebuoy(id string, l Location, tenant string) Thing {
	thing := newThingImpl(id, "Lifebuoy", l, tenant)
	return &Lifebuoy{
		thingImpl: thing,
	}
}

func (l *Lifebuoy) Handle(ctx context.Context, m []Measurement, onchange func(m ValueProvider) error) error {
	errs := []error{}

	for _, v := range m {
		errs = append(errs, l.handle(v, onchange))
	}

	return errors.Join(errs...)
}

func (l *Lifebuoy) handle(m Measurement, onchange func(m ValueProvider) error) error {
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

func (l *Lifebuoy) Byte() []byte {
	b, _ := json.Marshal(l)
	return b
}
