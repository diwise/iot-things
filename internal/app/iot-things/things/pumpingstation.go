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

func (ps *PumpingStation) Handle(v Measurement, onchange func(m ValueProvider) error) error {
	if !v.HasDigitalInput() {
		return nil
	}

	err := ps.stopWatch().Push(*v.BoolValue, v.Timestamp, func(sw functions.Stopwatch) error {
		ps.PumpingObserved = sw.State
		ps.PumpingObservedAt = sw.StartTime
		ps.PumpingDuration = sw.Duration

		switch sw.CurrentEvent {
		case functions.Started:
			stopwatch := NewStopwatch(ps.ID(), v.ID, 0, true, *ps.PumpingObservedAt)
			return onchange(stopwatch)
		case functions.Updated:
			stopwatch := NewStopwatch(ps.ID(), v.ID, ps.PumpingDuration.Seconds(), ps.PumpingObserved, v.Timestamp)
			return onchange(stopwatch)
		case functions.Stopped:
			stopwatch := NewStopwatch(ps.ID(), v.ID, ps.PumpingDuration.Seconds(), false, v.Timestamp)
			ps.PumpingCumulativeTime += *ps.PumpingDuration
			return onchange(stopwatch)
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (ps *PumpingStation) Byte() []byte {
	b, _ := json.Marshal(ps)
	return b
}
