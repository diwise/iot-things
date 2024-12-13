package things

import (
	"encoding/json"
	"errors"

	"github.com/diwise/iot-things/internal/app/iot-things/functions"
)

type Container struct {
	thingImpl
	functions.LevelConfig

	CurrentLevel float64 `json:"currentLevel"`
	Percent      float64 `json:"percent"`
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

func (c *Container) Handle(m []Measurement, onchange func(m ValueProvider) error) error {
	errs := []error{}

	for _, v := range m {
		errs = append(errs, c.handle(v, onchange))
	}

	return errors.Join(errs...)
}

func (c *Container) handle(m Measurement, onchange func(m ValueProvider) error) error {
	if !hasDistance(&m) {
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

	fillingLevel := NewFillingLevel(c.ID(), m.ID, level.Percent(), level.Current(), m.Timestamp)

	d := *m.Value
	n := 1

	for _, ref := range c.RefDevices {
		if ref.DeviceID != m.ID {
			for _, h := range ref.Measurements {
				if hasDistance(&h) {
					d += *h.Value
					n++
				}
			}
		}
	}

	avg_distance := d / float64(n)
	avg_level, _ := functions.NewLevel(c.Angle, c.MaxDistance, c.MaxLevel, c.MeanLevel, c.Offset, c.CurrentLevel)
	avg_level.Calc(avg_distance, m.Timestamp)

	c.CurrentLevel = avg_level.Current()
	c.Percent = avg_level.Percent()

	return onchange(fillingLevel)
}

func (c *Container) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
