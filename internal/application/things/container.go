package things

import (
	"context"
	"errors"

	"github.com/diwise/iot-things/internal/application/functions"
)

type Container struct {
	Base
	functions.LevelConfig

	CurrentLevel float64 `json:"currentLevel"`
	Percent      float64 `json:"percent"`
}

func NewContainer(id string, l Location, tenant string) Thing {
	thing := newBase(id, "Container", l, tenant)
	return &Container{
		Base: thing,
	}
}

func NewWasteContainer(id string, l Location, tenant string) Thing {
	thing := newBase(id, "Container", l, tenant)

	subType := "WasteContainer"
	thing.SubType = &subType

	return &Container{
		Base: thing,
	}
}

func (c *Container) Handle(ctx context.Context, m []Measurement, onchange func(m ValueProvider) error) error {
	errs := []error{}

	for _, v := range m {
		errs = append(errs, c.handle(ctx, v, onchange))
	}

	return errors.Join(errs...)
}

func (c *Container) handle(ctx context.Context, m Measurement, onchange func(m ValueProvider) error) error {
	if input, ok := resolveInput("container", m); ok {
		return c.Apply(ctx, input, m, onchange)
	}
	return nil
}

func (c *Container) Apply(ctx context.Context, input string, m Measurement, onchange func(m ValueProvider) error) error {
	if input != "distance" {
		return nil
	}
	if !hasDistance(&m) {
		return nil
	}
	return c.applyDistance(m, onchange)
}

func (c *Container) applyDistance(m Measurement, onchange func(m ValueProvider) error) error {
	level, err := functions.NewLevel(c.Angle, c.MaxDistance, c.MaxLevel, c.MeanLevel, c.Offset, c.CurrentLevel)
	if err != nil {
		return err
	}

	_, err = level.Calc(*m.Value, m.Timestamp)
	if err != nil {
		return err
	}

	fillingLevel := NewFillingLevel(c.ID(), m.ID, level.Percent(), level.Current(), m.Timestamp)

	avgDistance := avg(c, "distance", m, *m.Value)
	avg_level, _ := functions.NewLevel(c.Angle, c.MaxDistance, c.MaxLevel, c.MeanLevel, c.Offset, c.CurrentLevel)
	avg_level.Calc(avgDistance, m.Timestamp)

	c.CurrentLevel = avg_level.Current()
	c.Percent = avg_level.Percent()

	return onchange(fillingLevel)
}
