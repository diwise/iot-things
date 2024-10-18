package things

import (
	"encoding/json"

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

func (c *Container) Handle(v Measurement, onchange func(m ValueProvider) error) error {
	if !v.HasDistance() {
		return nil
	}

	level, err := functions.NewLevel(c.Angle, c.MaxDistance, c.MaxLevel, c.MeanLevel, c.Offset, c.CurrentLevel)
	if err != nil {
		return err
	}

	_, err = level.Calc(*v.Value, v.Timestamp)
	if err != nil {
		return err
	}

	fillingLevel := NewFillingLevel(c.ID(), v.ID, level.Percent(), level.Current(), v.Timestamp)

	d := *v.Value
	n := 1

	for _, ref := range c.RefDevices {
		if ref.DeviceID != v.ID {
			for _, h := range ref.Measurements {
				if h.HasDistance() {
					d += *h.Value
					n++
				}
			}
		}
	}

	avg_distance := d / float64(n)
	avg_level, _ := functions.NewLevel(c.Angle, c.MaxDistance, c.MaxLevel, c.MeanLevel, c.Offset, c.CurrentLevel)
	avg_level.Calc(avg_distance, v.Timestamp)

	c.CurrentLevel = avg_level.Current()
	c.Percent = avg_level.Percent()

	return onchange(fillingLevel)
}

func (c *Container) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
