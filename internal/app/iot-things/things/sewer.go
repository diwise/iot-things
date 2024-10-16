package things

import (
	"encoding/json"
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

func (c *Sewer) Handle(v Value, onchange func(m Measurements) error) error {
	if v.HasDistance() {
		return c.handleDistance(v, onchange)
	}

	if v.HasDigitalInput() {
		return c.handleDigitalInput(v, onchange)
	}

	return nil
}

func (s *Sewer) handleDistance(v Value, onchange func(m Measurements) error) error {
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

func (s *Sewer) handleDigitalInput(v Value, onchange func(m Measurements) error) error {
	err := s.stopWatch().Push(*v.BoolValue, v.Timestamp, func(sw functions.Stopwatch) error {
		s.OverflowObserved = sw.State
		s.OverflowObservedAt = sw.StartTime
		s.OverflowDuration = sw.Duration
		s.OverflowCumulativeTime = sw.CumulativeTime

		return nil
	})
	if err != nil {
		return err
	}

	stopwatch := NewStopwatch(s.ID(), v.ID, s.OverflowCumulativeTime.Seconds(), *v.BoolValue, v.Timestamp)

	return onchange(stopwatch)
}

func (c *Sewer) Byte() []byte {
	b, _ := json.Marshal(c)
	return b
}
