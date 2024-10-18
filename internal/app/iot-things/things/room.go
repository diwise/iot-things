package things

import "encoding/json"

type Room struct {
	thingImpl
	Temperature float64 `json:"temperature"`
}

func NewRoom(id string, l Location, tenant string) Room {
	return Room{
		thingImpl: newThingImpl(id, "Room", l, tenant),
	}
}

func (c *Room) Handle(v Measurement, onchange func(m ValueProvider) error) error {
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
			for _, v := range ref.Measurements {
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
