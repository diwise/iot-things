package things

import (
	"encoding/json"
	"errors"
	"time"
)

type Passage struct {
	thingImpl
	CumulatedNumberOfPassages int64 `json:"cumulatedNumberOfPassages"`
	PassagesToday             int   `json:"passagesToday"`
	CurrentState              bool  `json:"currentState"`

	passages map[int]int `json:"-"`
}

func NewPassage(id string, l Location, tenant string) Thing {
	thing := newThingImpl(id, "Passage", l, tenant)
	return &Passage{
		thingImpl: thing,
	}
}
func (p *Passage) increasePassages(ts time.Time) {
	p.CumulatedNumberOfPassages++

	if p.passages == nil {
		p.passages = make(map[int]int)
	}

	current := ts.Year() + ts.YearDay()
	if _, ok := p.passages[current]; !ok {
		p.passages[current] = 0
	}

	p.passages[current]++

	today := time.Now().Year() + time.Now().YearDay()

	p.PassagesToday = p.passages[today]
}

func (p *Passage) Handle(m []Measurement, onchange func(m ValueProvider) error) error {
	errs := []error{}

	for _, v := range m {
		errs = append(errs, p.handle(v, onchange))
	}

	return errors.Join(errs...)
}

func (p *Passage) handle(m Measurement, onchange func(m ValueProvider) error) error {
	if !m.HasDigitalInput() {
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
