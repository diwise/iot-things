package things

import "encoding/json"

type PumpingStation struct {
	thingImpl
}

func (c *PumpingStation) Handle(v Value, onchange func(m Measurements) error) error {
	return nil
}

func (c *PumpingStation) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
