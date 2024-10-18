package things

import "encoding/json"

type Building struct {
	thingImpl
	Energy      float64 `json:"energy"`
	Power       float64 `json:"power"`
	Temperature float64 `json:"temperature"`
}

func NewBuilding(id string, l Location, tenant string) Thing {
	thing := newThingImpl(id, "Building", l, tenant)
	return &Building{
		thingImpl: thing,
	}
}

func (b *Building) Handle(m Measurement, onchange func(m ValueProvider) error) error {
	if m.HasEnergy() {
		previousValue := b.Energy
		value := *m.Value / 3600000.0 // convert from Joule to kWh

		if hasChanged(previousValue, value) {
			b.Energy = value
			energy := NewEnergy(b.ID(), m.ID, b.Energy, m.Timestamp)
			return onchange(energy)
		}
	}

	if m.HasPower() {
		previousValue := b.Power
		value := *m.Value / 1000.0 // convert from Watt to kW

		if hasChanged(previousValue, value) {
			b.Power = value
			power := NewPower(b.ID(), m.ID, b.Power, m.Timestamp)
			return onchange(power)
		}
	}

	if m.HasTemperature() {
		if !hasChanged(b.Temperature, *m.Value) {
			return nil
		}

		temp := NewTemperature(b.ID(), m.ID, *m.Value, m.Timestamp)
		err := onchange(temp)
		if err != nil {
			return err
		}

		t := *m.Value
		n := 1

		for _, ref := range b.RefDevices {
			if ref.DeviceID != m.ID {
				for _, v := range ref.Measurements {
					if v.HasTemperature() {
						t += *v.Value
						n++
					}
				}
			}
		}

		b.Temperature = t / float64(n)

		return nil
	}

	return nil
}

func (c *Building) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
