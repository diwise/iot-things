package things

import "encoding/json"

type WaterMeter struct {
	thingImpl
	CumulativeVolume Value `json:"cumulativeVolume"`
	Leakage          *bool `json:"leakage,omitempty"`
	Burst            *bool `json:"burst,omitempty"`
	Backflow         *bool `json:"backflow,omitempty"`
	Fraud            *bool `json:"fraud,omitempty"`
}

func (c *WaterMeter) Handle(v Value, onchange func(m Measurements) error) error {
	return nil
}

func (c *WaterMeter) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
