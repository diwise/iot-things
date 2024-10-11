package things

import "encoding/json"

type Sewer struct {
	thingImpl
	CurrentLevel float64 `json:"current_level"`
	Percent      float64 `json:"percent"`
}

func (c *Sewer) Handle(v Value, onchange func(m Measurements) error) error {
	return nil
}

func (c *Sewer) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
