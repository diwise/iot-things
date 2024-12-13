package things

import (
	"encoding/json"
	"errors"
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

func NewWatermeter(id string, l Location, tenant string) Thing {
	return &Watermeter{
		thingImpl: newThingImpl(id, "Room", l, tenant),
	}
}

func (wm *Watermeter) Handle(m []Measurement, onchange func(m ValueProvider) error) error {
	errs := []error{}

	for _, v := range m {
		errs = append(errs, wm.handle(v, onchange))
	}

	return errors.Join(errs...)
}

func (wm *Watermeter) handle(m Measurement, onchange func(m ValueProvider) error) error {
	if !hasWaterMeter(&m) {
		return nil
	}

	changed := false

	if strings.HasSuffix(m.ID, CumulatedWaterVolumeSuffix) {
		changed = hasChanged(wm.CumulativeVolume, *m.Value)
		wm.CumulativeVolume = *m.Value
	}

	if strings.HasSuffix(m.ID, LeakageSuffix) {
		changed = hasChanged(wm.Leakage, *m.BoolValue)
		wm.Leakage = *m.BoolValue
	}
	if strings.HasSuffix(m.ID, BackflowSuffix) {
		changed = hasChanged(wm.Backflow, *m.BoolValue)
		wm.Backflow = *m.BoolValue
	}
	if strings.HasSuffix(m.ID, FraudSuffix) {
		changed = hasChanged(wm.Fraud, *m.BoolValue)
		wm.Fraud = *m.BoolValue
	}

	if changed {
		wm := NewWaterMeter(wm.ID(), m.ID, wm.CumulativeVolume, wm.Leakage, wm.Backflow, wm.Fraud, m.Timestamp)
		return onchange(wm)
	}

	return nil
}

func (wm *Watermeter) Byte() []byte {
	b, _ := json.Marshal(wm)
	return b
}
