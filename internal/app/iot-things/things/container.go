package things

import (
	"encoding/json"
	"errors"

	"github.com/diwise/iot-things/internal/app/iot-things/functions"
)

type Container struct {
	thingImpl
	CurrentLevel float64 `json:"current_level"`
	Percent      float64 `json:"percent"`

	MaxDistance *float64 `json:"max_distance,omitempty"`
	MaxLevel    *float64 `json:"max_level,omitempty"`
	MeanLevel   *float64 `json:"mean_level,omitempty"`
	Offset      *float64 `json:"offset,omitempty"`
	Angle       *float64 `json:"angle,omitempty"`
}

func NewContainer(id string, l Location, tenant string) Thing {
	thing := newThingImpl(id, "Container", l, tenant)
	return &Container{
		thingImpl: thing,
	}
}

func NewWasteContainer(id string, l Location, tenant string) Thing {
	thing := newThingImpl(id, "Container", l, tenant)

	subType := "WasteContainer"
	thing.SubType = &subType

	return &Container{
		thingImpl: thing,
	}
}

func (c *Container) Handle(m Measurement, onchange func(m Measurement) error) error {
	if !m.HasDistance() {
		return nil
	}

	level, err := functions.NewLevel(c.Angle, c.MaxDistance, c.MaxLevel, c.MeanLevel, c.Offset, c.CurrentLevel)
	if err != nil {
		return err
	}

	_, err = level.Calc(*m.Value, m.Timestamp)
	if err != nil {
		return err
	}

	c.CurrentLevel = level.Current()
	c.Percent = level.Percent()

	errs := []error{}

	errs = append(errs, onchange(newActualFillingLevel(c.ID(), m.ID, m.Timestamp, c.CurrentLevel)))
	errs = append(errs, onchange(newActualFillingPercentage(c.ID(), m.ID, m.Timestamp, c.Percent)))

	return errors.Join(errs...)
}

func (c *Container) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
