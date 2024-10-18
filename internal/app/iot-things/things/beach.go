package things

import "encoding/json"

type Beach struct {
	thingImpl
	Temperature float64 `json:"temperature"`
}

func NewBeach(id string, l Location, tenant string) Beach {
	return Beach{
		thingImpl: newThingImpl(id, "Beach", l, tenant),
	}
}

func (c *Beach) Handle(v Value, onchange func(m Measurements) error) error {
	if !v.HasTemperature() {
		return nil
	}

	if !hasChanged(c.Temperature, *v.Value) {
		return nil
	}

	temp := NewTemperature(c.ID(), v.ID, *v.Value, v.Timestamp)
	err := onchange(temp)
	if err != nil {
		return err
	}

	t := *v.Value
	n := 1

	for _, ref := range c.RefDevices {
		if ref.DeviceID != v.ID {
			for _, v := range ref.Values {
				if v.HasTemperature() {
					t += *v.Value
					n++
				}
			}
		}
	}

	c.Temperature = t / float64(n)

	return nil
}

func (c *Beach) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
