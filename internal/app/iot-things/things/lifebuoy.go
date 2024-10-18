package things

import "encoding/json"

type Lifebuoy struct {
	thingImpl
	Presence bool `json:"presence"`
}

func (c *Lifebuoy) Handle(v Measurement, onchange func(m ValueProvider) error) error {
	if !(v.HasDigitalInput() || v.HasPresence()) {
		return nil
	}

	if !hasChanged(c.Presence, *v.BoolValue) {
		return nil
	}

	c.Presence = *v.BoolValue
	presence := NewPresence(c.ID(), v.ID, c.Presence, v.Timestamp)

	return onchange(presence)
}

func (c *Lifebuoy) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
