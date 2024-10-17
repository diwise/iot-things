package things

type Building struct {
	thingImpl
	Energy float64 `json:"energy"`
	Power  float64 `json:"power"`
}

func NewBuilding(id string, l Location, tenant string) Thing {
	thing := newThingImpl(id, "Building", l, tenant)
	return &Building{
		thingImpl: thing,
	}
}

func (b *Building) Handle(m Value, onchange func(m Measurements) error) error {
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

	return nil
}
