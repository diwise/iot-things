package things

import (
	"encoding/json"
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
func (c *Passage) increasePassages() {
	c.CumulatedNumberOfPassages++

	if c.passages == nil {
		c.passages = make(map[int]int)
	}

	today := time.Now().Year() + time.Now().YearDay()
	c.passages[today]++

	c.PassagesToday = c.passages[today]
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
		c.increasePassages()

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
