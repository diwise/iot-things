package things

import (
	"context"
	"errors"
)

type Building struct {
	Base
	Energy      float64     `json:"energy"`
	Power       float64     `json:"power"`
	Temperature Measurement `json:"temperature"`
}

func NewBuilding(id string, l Location, tenant string) Thing {
	thing := newBase(id, "Building", l, tenant)
	return &Building{
		Base: thing,
	}
}

func (building *Building) Handle(ctx context.Context, m []Measurement, onchange func(m ValueProvider) error) error {
	errs := []error{}

	for _, v := range m {
		errs = append(errs, building.handle(ctx, v, onchange))
	}

	return errors.Join(errs...)
}

func (building *Building) handle(ctx context.Context, m Measurement, onchange func(m ValueProvider) error) error {
	if input, ok := resolveInput("building", m); ok {
		return building.Apply(ctx, input, m, onchange)
	}
	return nil
}

func (building *Building) Apply(ctx context.Context, input string, m Measurement, onchange func(m ValueProvider) error) error {
	switch input {
	case "energy":
		return building.applyEnergy(m, onchange)
	case "power":
		return building.applyPower(m, onchange)
	case "temperature":
		return building.applyTemperature(m, onchange)
	}
	return nil
}

func (building *Building) applyEnergy(m Measurement, onchange func(m ValueProvider) error) error {
	if hasEnergy(&m) {
		previousValue := building.Energy
		value := *m.Value / 3600000.0 // convert from Joule to kWh

		if hasChanged(previousValue, value) {
			building.Energy = value
			energy := NewEnergy(building.ID(), m.ID, building.Energy, m.Timestamp)
			return onchange(energy)
		}
	}

	return nil
}

func (building *Building) applyPower(m Measurement, onchange func(m ValueProvider) error) error {
	if hasPower(&m) {
		previousValue := building.Power
		value := *m.Value / 1000.0 // convert from Watt to kW

		if hasChanged(previousValue, value) {
			building.Power = value
			power := NewPower(building.ID(), m.ID, building.Power, m.Timestamp)
			return onchange(power)
		}
	}

	return nil
}

func (building *Building) applyTemperature(m Measurement, onchange func(m ValueProvider) error) error {
	if !hasTemperature(&m) {
		return nil
	}

	// Jämför det nya aggregerade värdet mot det sparade medelvärdet.
	avgTemp := avg(building, m, *m.Value, hasTemperature)
	if !hasChanged(building.Temperature, avgTemp) {
		return nil
	}

	temp := newTemperature(building.ID(), m.ID, *m.Value, m.Timestamp)
	if err := onchange(temp); err != nil {
		return err
	}

	building.Temperature = Measurement{
		Value:     &avgTemp,
		Source:    m.Source,
		Timestamp: m.Timestamp,
	}

	return nil
}
