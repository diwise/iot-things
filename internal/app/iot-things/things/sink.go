package things

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/diwise/iot-things/internal/app/iot-things/functions"
)

type Sink struct {
	thingImpl
	functions.LevelConfig

	AutoCfg *bool `json:"_autocfg"`

	On             bool           `json:"on"`
	OnAt           *time.Time     `json:"onAt"`
	Duration       *time.Duration `json:"duration"`
	CumulativeTime time.Duration  `json:"cumulativeTime"`

	Sw *functions.Stopwatch `json:"_stopwatch"`
}

func NewSink(id string, l Location, tenant string) Thing {
	thing := newThingImpl(id, "Sink", l, tenant)
	return &Sink{
		thingImpl: thing,
		Sw:        functions.NewStopwatch(),
	}
}

func (s *Sink) stopWatch() *functions.Stopwatch {
	if s.Sw == nil {
		s.Sw = functions.NewStopwatch()
	}
	return s.Sw
}

func (d *Sink) Handle(ctx context.Context, m []Measurement, onchange func(m ValueProvider) error) error {
	errs := []error{}

	for _, v := range m {
		errs = append(errs, d.handle(v, onchange))
	}

	return errors.Join(errs...)
}

func (s *Sink) handle(m Measurement, onchange func(m ValueProvider) error) error {
	var errs []error

	if _, err := handleTemperature(s, m, onchange); err != nil {
		errs = append(errs, err)
	}

	if _, err := handlePresence(s, m, onchange); err != nil {
		errs = append(errs, err)
	}

	if _, err := handlePower(s, m, onchange); err != nil {
		errs = append(errs, err)
	}

	if _, err := handleEnergy(s, m, onchange); err != nil {
		errs = append(errs, err)
	}

	if _, err := handleDistance(s, m, onchange); err != nil {
		errs = append(errs, err)
	}

	if _, err := handleDigitalInput(s, m, onchange); err != nil {
		errs = append(errs, err)
	}

	if _, err := handleIlluminance(s, m, onchange); err != nil {
		errs = append(errs, err)
	}

	if _, err := handleHumidity(s, m, onchange); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func (l *Sink) Byte() []byte {
	b, _ := json.Marshal(l)
	return b
}

func handleHumidity(s *Sink, m Measurement, onchange func(m ValueProvider) error) (*float64, error) {
	if !(hasHumidity(&m)) {
		return nil, nil
	}

	err := onchange(NewHumidity(s.ID(), m.ID, *m.Value, m.Timestamp))
	if err != nil {
		return nil, err
	}

	return m.Value, nil
}

func handleIlluminance(s *Sink, m Measurement, onchange func(m ValueProvider) error) (*float64, error) {
	if !(hasIlluminance(&m)) {
		return nil, nil
	}

	err := onchange(NewIlluminance(s.ID(), m.ID, *m.Value, m.Timestamp))
	if err != nil {
		return nil, err
	}

	return m.Value, nil
}

func handleDigitalInput(s *Sink, m Measurement, onchange func(m ValueProvider) error) (*bool, error) {
	if !(hasDigitalInput(&m)) {
		return nil, nil
	}

	err := s.stopWatch().Push(*m.BoolValue, m.Timestamp, func(sw functions.Stopwatch) error {
		s.On = sw.State
		s.OnAt = sw.StartTime
		s.Duration = sw.Duration

		var zero, sec float64

		zero = 0.0
		if s.Duration != nil {
			sec = s.Duration.Seconds()
		}

		switch sw.CurrentEvent {
		case functions.Started:
			s.OnAt = &m.Timestamp
			stopwatch := NewStopwatch(s.ID(), m.ID, &zero, true, *s.OnAt)
			return onchange(stopwatch)
		case functions.Updated:
			s.OnAt = &m.Timestamp
			stopwatch := NewStopwatch(s.ID(), m.ID, &sec, s.On, *s.OnAt)
			return onchange(stopwatch)
		case functions.Stopped:
			s.OnAt = &m.Timestamp
			stopwatch := NewStopwatch(s.ID(), m.ID, &sec, false, *s.OnAt)
			s.CumulativeTime += *s.Duration
			return onchange(stopwatch)
		case functions.InitialState:
			s.OnAt = &m.Timestamp
			stopwatch := NewStopwatch(s.ID(), m.ID, &zero, false, *s.OnAt)
			return onchange(stopwatch)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return m.BoolValue, nil
}

func handleDistance(s *Sink, m Measurement, onchange func(m ValueProvider) error) (*float64, error) {
	if !(hasDistance(&m)) {
		return nil, nil
	}

	if s.AutoCfg == nil || *s.AutoCfg {
		if s.MaxDistance == nil || m.Value != nil || *s.MaxDistance == 0 || *s.MaxDistance < *m.Value {
			s.MaxDistance = m.Value
			t := true
			s.AutoCfg = &t
		}

		if s.MaxLevel == nil || m.Value != nil || *s.MaxLevel == 0 || *s.MaxLevel < *m.Value {
			s.MaxLevel = m.Value
			t := true
			s.AutoCfg = &t
		}
	}

	level, err := functions.NewLevel(s.Angle, s.MaxDistance, s.MaxLevel, s.MeanLevel, s.Offset, 0.0)
	if err != nil {
		return nil, err
	}

	_, err = level.Calc(*m.Value, m.Timestamp)
	if err != nil {
		return nil, err
	}

	fillingLevel := NewFillingLevel(s.ID(), m.ID, level.Percent(), level.Current(), m.Timestamp)
	err = onchange(fillingLevel)
	if err != nil {
		return nil, err
	}

	return fillingLevel.Level.Value, nil
}

func handleEnergy(s *Sink, m Measurement, onchange func(m ValueProvider) error) (*float64, error) {
	if !(hasEnergy(&m)) {
		return nil, nil
	}

	energy := *m.Value / 3600000.0 // convert from Joule to kWh
	err := onchange(NewEnergy(s.ID(), m.ID, energy, m.Timestamp))
	if err != nil {
		return nil, err
	}

	return &energy, nil
}

func handlePower(s *Sink, m Measurement, onchange func(m ValueProvider) error) (*float64, error) {
	if !(hasPower(&m)) {
		return nil, nil
	}

	power := *m.Value / 1000.0 // convert from Watt to kW
	err := onchange(NewPower(s.ID(), m.ID, power, m.Timestamp))
	if err != nil {
		return nil, err
	}

	return &power, nil
}

func handlePresence(s *Sink, m Measurement, onchange func(m ValueProvider) error) (*bool, error) {
	if !(hasPresence(&m)) {
		return nil, nil
	}

	presence := NewPresence(s.ID(), m.ID, *m.BoolValue, m.Timestamp)
	err := onchange(presence)
	if err != nil {
		return nil, err
	}

	return m.BoolValue, nil
}

func handleTemperature(s *Sink, m Measurement, onchange func(m ValueProvider) error) (*float64, error) {
	if !(hasTemperature(&m)) {
		return nil, nil
	}

	temp := NewTemperature(s.ID(), m.ID, *m.Value, m.Timestamp)
	err := onchange(temp)
	if err != nil {
		return nil, err
	}

	return m.Value, nil
}
