package things

import "encoding/json"

type PointOfInterest struct {
	thingImpl
	Temperature float64 `json:"temperature"`
}

func NewPointOfInterest(id string, l Location, tenant string) PointOfInterest {
	return PointOfInterest{
		thingImpl: newThingImpl(id, "PointOfInterest", l, tenant),
	}
}

func (c *PointOfInterest) Handle(v Measurement, onchange func(m ValueProvider) error) error {
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

func (c *PointOfInterest) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
