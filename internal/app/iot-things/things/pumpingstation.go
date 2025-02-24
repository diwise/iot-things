package things

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/diwise/iot-things/internal/app/iot-things/functions"
)

type PumpingStation struct {
	thingImpl

	PumpingObserved       bool           `json:"pumpingObserved"`
	PumpingObservedAt     *time.Time     `json:"pumpingObservedAt"`
	PumpingDuration       *time.Duration `json:"pumpingDuration"`
	PumpingCumulativeTime time.Duration  `json:"pumpingCumulativeTime"`

	Sw *functions.Stopwatch `json:"_stopwatch"`
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

func (ps *PumpingStation) Handle(ctx context.Context, m []Measurement, onchange func(m ValueProvider) error) error {
	errs := []error{}

	for _, v := range m {
		errs = append(errs, ps.handle(v, onchange))
	}

	return errors.Join(errs...)
}

func (ps *PumpingStation) handle(m Measurement, onchange func(m ValueProvider) error) error {
	if !hasDigitalInput(&m) {
		return nil
	}

	err := ps.stopWatch().Push(*m.BoolValue, m.Timestamp, func(sw functions.Stopwatch) error {
		ps.PumpingObserved = sw.State
		ps.PumpingObservedAt = sw.StartTime
		ps.PumpingDuration = sw.Duration

		var z, sec float64

		z = 0.0
		if ps.PumpingDuration != nil {
			sec = ps.PumpingDuration.Seconds()
		}

		switch sw.CurrentEvent {
		case functions.Started:
			ps.PumpingObservedAt = &m.Timestamp
			stopwatch := NewStopwatch(ps.ID(), m.ID, &z, true, *ps.PumpingObservedAt)
			return onchange(stopwatch)
		case functions.Updated:
			ps.PumpingObservedAt = &m.Timestamp
			stopwatch := NewStopwatch(ps.ID(), m.ID, &sec, ps.PumpingObserved, *ps.PumpingObservedAt)
			return onchange(stopwatch)
		case functions.Stopped:
			ps.PumpingObservedAt = &m.Timestamp
			stopwatch := NewStopwatch(ps.ID(), m.ID, &sec, false, *ps.PumpingObservedAt)
			ps.PumpingCumulativeTime += *ps.PumpingDuration
			return onchange(stopwatch)
		case functions.InitialState:
			ps.PumpingObservedAt = &m.Timestamp
			stopwatch := NewStopwatch(ps.ID(), m.ID, &z, false, *ps.PumpingObservedAt)
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
