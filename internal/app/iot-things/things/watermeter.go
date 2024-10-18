package things

import (
	"encoding/json"
	"strings"
)

const (
	CumulatedWaterVolumeSuffix string = "/1"
	LeakageSuffix              string = "/10"
	BackflowSuffix             string = "/11"
	FraudSuffix                string = "/13"
)

type Watermeter struct {
	thingImpl
	CumulativeVolume float64 `json:"cumulativeVolume"`
	Leakage          bool    `json:"leakage"`
	Burst            bool    `json:"burst"`
	Backflow         bool    `json:"backflow"`
	Fraud            bool    `json:"fraud"`
}

func (c *Watermeter) Handle(v Measurement, onchange func(m ValueProvider) error) error {
	if !v.HasWaterMeter() {
		return nil
	}

	changed := false

	if strings.HasSuffix(v.ID, CumulatedWaterVolumeSuffix) {
		changed = hasChanged(c.CumulativeVolume, *v.Value)
		c.CumulativeVolume = *v.Value
	}
	if strings.HasSuffix(v.ID, LeakageSuffix) {
		changed = hasChanged(c.Leakage, *v.BoolValue)
		c.Leakage = *v.BoolValue
	}
	if strings.HasSuffix(v.ID, BackflowSuffix) {
		changed = hasChanged(c.Backflow, *v.BoolValue)
		c.Backflow = *v.BoolValue
	}
	if strings.HasSuffix(v.ID, FraudSuffix) {
		changed = hasChanged(c.Fraud, *v.BoolValue)
		c.Fraud = *v.BoolValue
	}

	if changed {
		wm := NewWaterMeter(c.ID(), v.ID, c.CumulativeVolume, c.Leakage, c.Backflow, c.Fraud, v.Timestamp)
		return onchange(wm)
	}

	return nil
}

func (c *Watermeter) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
