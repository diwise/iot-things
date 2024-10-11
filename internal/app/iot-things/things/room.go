package things

import "encoding/json"

type Room struct {
	thingImpl
	Temperature float64 `json:"temperature"`
}

func (c *Room) Handle(v Value, onchange func(m Measurements) error) error {
	if !v.HasTemperature() {
		return nil
	}

	if c.Temperature == *v.Value {
		return nil
	}

	temp := NewTemperature(c.ID(), v.ID, c.Temperature, v.Timestamp)
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

func (c *Room) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
