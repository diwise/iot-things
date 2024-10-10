package things

import (
	"encoding/json"
	"errors"
)

func NewContainer(id string, l Location, tenant string) Thing {
	thing := newThing(id, "Container", l, tenant)
	return &Container{
		thing: thing,
	}
}
func NewWasteContainer(id string, l Location, tenant string) Thing {
	thing := newThing(id, "Container", l, tenant)
	subType := "WasteContainer"
	thing.SubType = &subType
	return &Container{
		thing: thing,
	}
}

func (c *Container) Handle(m Measurement, onchange func(m Measurement) error) error {
	if !m.HasDistance() {
		return nil
	}

	l, p := fillingLevel(c.ID(), *m.Value, c.MaxDistance, c.MaxLevel, c.Offset, c.Angle, m.Timestamp)

	c.FillingLevel = &l
	c.Percent = &p

	errs := []error{}

	errs = append(errs, onchange(l))
	errs = append(errs, onchange(p))

	return errors.Join(errs...)
}

func (c *Container) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}

func (c *PumpingStation) Handle(m Measurement, onchange func(m Measurement) error) error {
	return nil
}

func (c *PumpingStation) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}

func (c *Room) Handle(m Measurement, onchange func(m Measurement) error) error {
	return nil
}

func (c *Room) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}

func (c *Sewer) Handle(m Measurement, onchange func(m Measurement) error) error {
	return nil
}

func (c *Sewer) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}

func (c *Passage) Handle(m Measurement, onchange func(m Measurement) error) error {
	return nil
}

func (c *Passage) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}

func (c *Lifebuoy) Handle(m Measurement, onchange func(m Measurement) error) error {
	return nil
}

func (c *Lifebuoy) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}

func (c *WaterMeter) Handle(m Measurement, onchange func(m Measurement) error) error {
	return nil
}

func (c *WaterMeter) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
