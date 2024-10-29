package things

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/diwise/iot-things/internal/app/iot-things/functions"
)

type Sewer struct {
	thingImpl
	functions.LevelConfig

	CurrentLevel float64 `json:"currentLevel"`
	Percent      float64 `json:"percent"`

	OverflowObserved       bool           `json:"overflowObserved"`
	OverflowObservedAt     *time.Time     `json:"overflowObservedAt"`
	OverflowDuration       *time.Duration `json:"overflowDuration"`
	OverflowCumulativeTime time.Duration  `json:"overflowCumulativeTime"`

	Sw *functions.Stopwatch `json:"stopwatch"`
}

func NewSewer(id string, l Location, tenant string) Thing {
	return &Sewer{
		thingImpl: newThingImpl(id, "Sewer", l, tenant),
		Sw:        functions.NewStopwatch(),
	}
}

func (s *Sewer) Handle(m []Measurement, onchange func(m ValueProvider) error) error {
	errs := []error{}

	for _, v := range m {
		errs = append(errs, s.handle(v, onchange))
	}

	return errors.Join(errs...)
}

func (s *Sewer) handle(v Measurement, onchange func(m ValueProvider) error) error {
	if v.HasDistance() {
		return s.handleDistance(v, onchange)
	}

	if v.HasDigitalInput() {
		return s.handleDigitalInput(v, onchange)
	}

	return nil
}

func (s *Sewer) handleDistance(v Measurement, onchange func(m ValueProvider) error) error {
	level, err := functions.NewLevel(s.Angle, s.MaxDistance, s.MaxLevel, s.MeanLevel, s.Offset, s.CurrentLevel)
	if err != nil {
		return err
	}

	_, err = level.Calc(*v.Value, v.Timestamp)
	if err != nil {
		return err
	}

	fillingLevel := NewFillingLevel(s.ID(), v.ID, level.Percent(), level.Current(), v.Timestamp)

	s.CurrentLevel = level.Current()
	s.Percent = level.Percent()

	return onchange(fillingLevel)
}

func (s *Sewer) stopWatch() *functions.Stopwatch {
	if s.Sw == nil {
		s.Sw = functions.NewStopwatch()
	}
	return s.Sw
}

func (s *Sewer) handleDigitalInput(v Measurement, onchange func(m ValueProvider) error) error {
	err := s.stopWatch().Push(*v.BoolValue, v.Timestamp, func(sw functions.Stopwatch) error {
		s.OverflowObserved = sw.State
		s.OverflowObservedAt = sw.StartTime
		s.OverflowDuration = sw.Duration

		switch sw.CurrentEvent {
		case functions.Started:
			stopwatch := NewStopwatch(s.ID(), v.ID, 0, true, *s.OverflowObservedAt)
			return onchange(stopwatch)
		case functions.Updated:
			stopwatch := NewStopwatch(s.ID(), v.ID, s.OverflowDuration.Seconds(), s.OverflowObserved, v.Timestamp)
			return onchange(stopwatch)
		case functions.Stopped:
			stopwatch := NewStopwatch(s.ID(), v.ID, s.OverflowDuration.Seconds(), false, v.Timestamp)
			s.OverflowCumulativeTime += *s.OverflowDuration
			return onchange(stopwatch)
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *Sewer) Byte() []byte {
	b, _ := json.Marshal(s)
	return b
}
