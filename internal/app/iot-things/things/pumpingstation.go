package things

import (
	"encoding/json"
	"time"

	"github.com/diwise/iot-things/internal/app/iot-things/functions"
)

type PumpingStation struct {
	thingImpl

	PumpingObserved       bool           `json:"pumpingObserved"`
	PumpingObservedAt     *time.Time     `json:"pumpingObservedAt"`
	PumpingDuration       *time.Duration `json:"pumpingDuration"`
	PumpingCumulativeTime time.Duration  `json:"pumpingCumulativeTime"`

	Sw *functions.Stopwatch `json:"stopwatch"`
}

func NewPumpingStation(id string, l Location, tenant string) Thing {
	thing := newThingImpl(id, "PumpingStation", l, tenant)
	return &PumpingStation{
		thingImpl: thing,
		Sw:        functions.NewStopwatch(),
	}
}

func (ps *PumpingStation) stopWatch() *functions.Stopwatch {
	if ps.Sw == nil {
		ps.Sw = functions.NewStopwatch()
	}
	return ps.Sw
}

func (ps *PumpingStation) Handle(v Value, onchange func(m Measurements) error) error {
	if !v.HasDigitalInput() {
		return nil
	}

	err := ps.stopWatch().Push(*v.BoolValue, v.Timestamp, func(sw functions.Stopwatch) error {
		ps.PumpingObserved = sw.State
		ps.PumpingObservedAt = sw.StartTime
		ps.PumpingDuration = sw.Duration
		ps.PumpingCumulativeTime = sw.CumulativeTime

		return nil
	})
	if err != nil {
		return err
	}

	stopwatch := NewStopwatch(ps.ID(), v.ID, ps.PumpingCumulativeTime.Seconds(), *v.BoolValue, v.Timestamp)

	return onchange(stopwatch)
}

func (ps *PumpingStation) Byte() []byte {
	b, _ := json.Marshal(ps)
	return b
}
