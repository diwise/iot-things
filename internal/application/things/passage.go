package things

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type Passage struct {
	thingImpl
	CumulatedNumberOfPassages int64 `json:"cumulatedNumberOfPassages"`
	PassagesToday             int   `json:"passagesToday"`
	CurrentState              bool  `json:"currentState"`

	Passages map[int]int `json:"_passages"`
}

func NewPassage(id string, l Location, tenant string) Thing {
	thing := newThingImpl(id, "Passage", l, tenant)
	return &Passage{
		thingImpl: thing,
	}
}
func (p *Passage) increasePassages(ts time.Time) {
	p.CumulatedNumberOfPassages++

	if p.Passages == nil {
		p.Passages = make(map[int]int)
	}

	dayNr := ts.Year() + ts.YearDay()
	if _, ok := p.Passages[dayNr]; !ok {
		p.Passages[dayNr] = 0
	}

	p.Passages[dayNr]++

	today := time.Now().Year() + time.Now().YearDay()

	p.PassagesToday = p.Passages[today]
}

func (p *Passage) Handle(ctx context.Context, m []Measurement, onchange func(m ValueProvider) error) error {
	errs := []error{}

	for _, v := range m {
		errs = append(errs, p.handle(v, onchange))
	}

	return errors.Join(errs...)
}

func (p *Passage) handle(m Measurement, onchange func(m ValueProvider) error) error {
	if !hasDigitalInput(&m) {
		return nil
	}

	if !hasChanged(p.CurrentState, *m.BoolValue) {
		return nil
	}

	var err error

	if *m.BoolValue {
		p.increasePassages(m.Timestamp)

		peopleCounter := NewPeopleCounter(p.ID(), m.ID, p.PassagesToday, p.CumulatedNumberOfPassages, m.Timestamp)

		err = onchange(peopleCounter)
		if err != nil {
			return err
		}
	}

	p.CurrentState = *m.BoolValue

	door := NewDoor(p.ID(), m.ID, p.CurrentState, m.Timestamp)

	return onchange(door)
}

func (p *Passage) Byte() []byte {
	b, _ := json.Marshal(p)
	return b
}
