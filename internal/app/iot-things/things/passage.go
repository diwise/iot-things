package things

import (
	"encoding/json"
	"time"
)

type Passage struct {
	thingImpl
	Count int64 `json:"count"`
	Today int64 `json:"today"`
	State bool  `json:"state"`
}

func NewPassage(id string, l Location, tenant string) Thing {
	thing := newThingImpl(id, "Passage", l, tenant)
	return &Passage{
		thingImpl: thing,
	}
}

func (c *Passage) Handle(m Value, onchange func(m Measurements) error) error {
	if !m.HasDigitalInput() {
		return nil
	}

	if c.State == *m.BoolValue {
		return nil
	}

	var err error

	if *m.BoolValue {
		c.Count++

		if m.Timestamp.YearDay() == time.Now().YearDay() {
			c.Today++
		} else {
			c.Today = 1
		}

		peopleCounter := NewPeopleCounter(c.ID(), m.ID, c.Today, c.Count, m.Timestamp)

		err = onchange(peopleCounter)
		if err != nil {
			return err
		}
	}

	c.State = *m.BoolValue

	door := NewDoor(c.ID(), m.ID, c.State, m.Timestamp)

	return onchange(door)
}

func (c *Passage) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
