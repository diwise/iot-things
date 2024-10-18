package things

import (
	"encoding/json"
	"time"
)

type Passage struct {
	thingImpl
	CumulatedNumberOfPassages int64 `json:"cumulatedNumberOfPassages"`
	PassagesToday             int64 `json:"passagesToday"`
	CurrentState              bool  `json:"currentState"`
}

func NewPassage(id string, l Location, tenant string) Thing {
	thing := newThingImpl(id, "Passage", l, tenant)
	return &Passage{
		thingImpl: thing,
	}
}

func (c *Passage) Handle(m Measurement, onchange func(m ValueProvider) error) error {
	if !m.HasDigitalInput() {
		return nil
	}

	if !hasChanged(c.CurrentState, *m.BoolValue) {
		return nil
	}

	var err error

	if *m.BoolValue {
		c.CumulatedNumberOfPassages++

		if m.Timestamp.YearDay() == time.Now().YearDay() {
			c.PassagesToday++
		} else {
			c.PassagesToday = 1
		}

		peopleCounter := NewPeopleCounter(c.ID(), m.ID, c.PassagesToday, c.CumulatedNumberOfPassages, m.Timestamp)

		err = onchange(peopleCounter)
		if err != nil {
			return err
		}
	}

	c.CurrentState = *m.BoolValue

	door := NewDoor(c.ID(), m.ID, c.CurrentState, m.Timestamp)

	return onchange(door)
}

func (c *Passage) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
