package things

import (
	"context"
	"encoding/json"
	"errors"
)

type Building struct {
	thingImpl
	Energy      float64     `json:"energy"`
	Power       float64     `json:"power"`
	Temperature Measurement `json:"temperature"`
}

func NewBuilding(id string, l Location, tenant string) Thing {
	thing := newThingImpl(id, "Building", l, tenant)
	return &Building{
		thingImpl: thing,
	}
}

func (building *Building) Handle(ctx context.Context, m []Measurement, onchange func(m ValueProvider) error) error {
	errs := []error{}

	for _, v := range m {
		errs = append(errs, building.handle(v, onchange))
	}

	return errors.Join(errs...)
}

func (building *Building) handle(m Measurement, onchange func(m ValueProvider) error) error {
	if hasEnergy(&m) {
		previousValue := building.Energy
		value := *m.Value / 3600000.0 // convert from Joule to kWh

		if hasChanged(previousValue, value) {
			building.Energy = value
			energy := NewEnergy(building.ID(), m.ID, building.Energy, m.Timestamp)
			return onchange(energy)
		}
	}

	if hasPower(&m) {
		previousValue := building.Power
		value := *m.Value / 1000.0 // convert from Watt to kW

		if hasChanged(previousValue, value) {
			building.Power = value
			power := NewPower(building.ID(), m.ID, building.Power, m.Timestamp)
			return onchange(power)
		}
	}

	if hasTemperature(&m) {
		if !hasChanged(building.Temperature, *m.Value) {
			return nil
		}

		temp := NewTemperature(building.ID(), m.ID, *m.Value, m.Timestamp)
		err := onchange(temp)
		if err != nil {
			return err
		}

		t := *m.Value
		n := 1

		for _, ref := range building.RefDevices {
			if ref.DeviceID != m.ID {
				for _, v := range ref.Measurements {
					if hasTemperature(&v) {
						t += *v.Value
						n++
					}
				}
			}
		}

		avgTemp := t / float64(n)

		building.Temperature = Measurement{
			Value:     &avgTemp,
			Source:    m.Source,
			Timestamp: m.Timestamp,
		}

		return nil
	}

	return nil

}

func (building *Building) Byte() []byte {
	b, _ := json.Marshal(building)
	return b
}
