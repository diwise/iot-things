package things

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/diwise/iot-things/internal/app/iot-things/functions"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

type Sewer struct {
	thingImpl
	functions.LevelConfig

	CurrentLevel float64   `json:"currentLevel"`
	Percent      float64   `json:"percent"`
	Measured     *Measured `json:"measured,omitempty"`

	OverflowObserved       bool           `json:"overflowObserved"`
	OverflowObservedAt     *time.Time     `json:"overflowObservedAt"`
	OverflowEndedAt        *time.Time     `json:"overflowEndedAt"`
	OverflowDuration       *time.Duration `json:"overflowDuration"`
	OverflowCumulativeTime time.Duration  `json:"overflowCumulativeTime"`

	LastAction string `json:"lastAction"`

	Sw *functions.Stopwatch `json:"_stopwatch"`
}

type Measured struct {
	Level      float64   `json:"level"`
	Percent    float64   `json:"percent"`
	ObservedAt time.Time `json:"observedAt"`
}

func NewSewer(id string, l Location, tenant string) Thing {
	return &Sewer{
		thingImpl: newThingImpl(id, "Sewer", l, tenant),
		Sw:        functions.NewStopwatch(),
	}
}

func (s *Sewer) Handle(ctx context.Context, m []Measurement, onchange func(m ValueProvider) error) error {
	errs := []error{}

	for _, v := range m {
		errs = append(errs, s.handle(ctx, v, onchange))
	}

	return errors.Join(errs...)
}

func (s *Sewer) handle(ctx context.Context, m Measurement, onchange func(m ValueProvider) error) error {
	if hasDistance(&m) {
		return s.handleDistance(ctx, m, onchange)
	}

	if hasDigitalInput(&m) {
		return s.handleDigitalInput(ctx, m, onchange)
	}

	return nil
}

func (s *Sewer) handleDistance(ctx context.Context, v Measurement, onchange func(m ValueProvider) error) error {
	log := logging.GetFromContext(ctx)

	level, err := functions.NewLevel(s.Angle, s.MaxDistance, s.MaxLevel, s.MeanLevel, s.Offset, s.CurrentLevel)
	if err != nil {
		return err
	}

	_, err = level.Calc(*v.Value, v.Timestamp)
	if err != nil {
		return err
	}

	if level.Current() < 0 {
		log.Warn("negative level value", slog.String("id", v.ID), slog.Float64("level", level.Current()), slog.Time("timestamp", v.Timestamp))
		// CHANGE: okt 28 2025, do not ignore negative values
		// return nil
	}

	pcnt := level.Percent()
	if pcnt < 0 || pcnt > 100 {
		log.Info("invalid percent value", slog.String("id", v.ID), slog.Float64("percent", pcnt), slog.Time("timestamp", v.Timestamp))
		if pcnt < 0 {
			pcnt = 0
		}
		if pcnt > 100 {
			pcnt = 100
		}
	}

	fillingLevel := NewFillingLevel(s.ID(), v.ID, pcnt, level.Current(), v.Timestamp)

	s.Measured = &Measured{
		Level:      level.Current(),
		Percent:    pcnt,
		ObservedAt: v.Timestamp,
	}

	if v.Timestamp.After(s.ObservedAt) {
		s.ObservedAt = v.Timestamp
		s.CurrentLevel = level.Current()
		s.Percent = pcnt
	}

	return onchange(fillingLevel)
}

func (s *Sewer) stopWatch() *functions.Stopwatch {
	if s.Sw == nil {
		s.Sw = functions.NewStopwatch()
	}
	return s.Sw
}

func (s *Sewer) handleDigitalInput(ctx context.Context, v Measurement, onchange func(m ValueProvider) error) error {
	log := logging.GetFromContext(ctx).With(slog.String("id", v.ID), slog.Time("timestamp", v.Timestamp))

	err := s.stopWatch().Push(*v.BoolValue, v.Timestamp, func(sw functions.Stopwatch) error {
		s.OverflowObserved = sw.State
		s.OverflowObservedAt = sw.StartTime
		s.OverflowDuration = sw.Duration

		var logOverflowObservedAt time.Time = time.Time{}
		var logOverflowDuration time.Duration = time.Duration(0)

		if s.OverflowObservedAt != nil {
			logOverflowObservedAt = *s.OverflowObservedAt
		}

		if s.OverflowDuration != nil {
			logOverflowDuration = *s.OverflowDuration
		}

		log.Debug("overflow observed", slog.Bool("state", s.OverflowObserved), slog.Time("start_time", logOverflowObservedAt), slog.Duration("duration", logOverflowDuration), slog.Int("event", int(sw.CurrentEvent)))

		var z, sec float64

		z = 0.0
		if s.OverflowDuration != nil {
			sec = s.OverflowDuration.Seconds()
		}

		switch sw.CurrentEvent {
		case functions.Started:
			log.Debug("overflow started", slog.String("sewer_id", s.ID()), slog.String("measurement_id", v.ID), slog.Float64("cumulative_time", z), slog.Bool("on_off", true), slog.Time("ts", *s.OverflowObservedAt))

			stopwatch := NewStopwatch(s.ID(), v.ID, &z, true, *s.OverflowObservedAt)

			s.LastAction = "overflow started"
			s.OverflowEndedAt = nil

			return onchange(stopwatch)
		case functions.Updated:
			log.Debug("overflow updated", slog.String("sewer_id", s.ID()), slog.String("measurement_id", v.ID), slog.Float64("cumulative_time", sec), slog.Bool("on_off", s.OverflowObserved), slog.Time("ts", v.Timestamp))

			stopwatch := NewStopwatch(s.ID(), v.ID, &sec, s.OverflowObserved, v.Timestamp)

			s.LastAction = "overflow updated"
			s.OverflowEndedAt = nil

			return onchange(stopwatch)
		case functions.Stopped:
			log.Debug("overflow stopped", slog.String("sewer_id", s.ID()), slog.String("measurement_id", v.ID), slog.Float64("cumulative_time", sec), slog.Bool("on_off", false), slog.Time("ts", v.Timestamp))

			stopwatch := NewStopwatch(s.ID(), v.ID, &sec, false, v.Timestamp)

			s.LastAction = "overflow stopped"
			s.OverflowCumulativeTime += *s.OverflowDuration
			s.OverflowEndedAt = &v.Timestamp

			return onchange(stopwatch)
		default:
			log.Debug("overflow default", slog.String("sewer_id", s.ID()), slog.String("measurement_id", v.ID), slog.Float64("cumulative_time", -1), slog.Bool("on_off", sw.State), slog.Time("ts", v.Timestamp), slog.Time("now", time.Now()))

			stopwatch := NewStopwatch(s.ID(), v.ID, nil, sw.State, v.Timestamp)

			s.LastAction = "overflow unknown"

			return onchange(stopwatch)
		}
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
