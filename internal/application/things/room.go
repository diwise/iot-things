package things

import (
	"context"
	"errors"
	"strings"
)

type Room struct {
	Base
	Temperature Measurement `json:"temperature"`
	Humidity    float64     `json:"humidity"`
	Illuminance float64     `json:"illuminance"`
	CO2         float64     `json:"co2"`
}

func NewRoom(id string, l Location, tenant string) Thing {
	return &Room{
		Base: newBase(id, "Room", l, tenant),
	}
}

func (r *Room) Handle(ctx context.Context, m []Measurement, onchange func(m ValueProvider) error) error {
	errs := []error{}

	for _, v := range m {
		errs = append(errs, r.handle(v, onchange))
	}

	return errors.Join(errs...)
}

func (r *Room) handle(m Measurement, onchange func(m ValueProvider) error) error {
	if hasTemperature(&m) {
		return r.handleTemperature(m, onchange)
	}

	if hasHumidity(&m) {
		return r.handleHumidity(m, onchange)
	}

	if hasIlluminance(&m) {
		return r.handleIlluminance(m, onchange)
	}

	if hasAirQuality(&m) {
		return r.handleAirQuality(m, onchange)
	}

	return nil
}

func (r *Room) handleAirQuality(m Measurement, onchange func(m ValueProvider) error) error {

	const CO2 = "/17"

	if !(strings.HasSuffix(m.ID, CO2)) {
		return nil
	}

	if !hasChanged(r.CO2, *m.Value) {
		return nil
	}

	air := NewAirQuality(r.ID(), m.ID, *m.Value, m.Timestamp)
	err := onchange(air)
	if err != nil {
		return err
	}

	r.CO2 = avg(r, m, *m.Value, hasAirQuality)

	return nil
}

func (r *Room) handleIlluminance(m Measurement, onchange func(m ValueProvider) error) error {

	const SensorValue = "/5700"

	if !(strings.HasSuffix(m.ID, SensorValue)) {
		return nil
	}

	if !hasChanged(r.Illuminance, *m.Value) {
		return nil
	}

	ill := NewIlluminance(r.ID(), m.ID, *m.Value, m.Timestamp)
	err := onchange(ill)
	if err != nil {
		return err
	}

	r.Illuminance = avg(r, m, *m.Value, hasIlluminance)

	return nil
}

func (r *Room) handleHumidity(m Measurement, onchange func(m ValueProvider) error) error {

	const SensorValue = "/5700"

	if !(strings.HasSuffix(m.ID, SensorValue)) {
		return nil
	}

	if !hasChanged(r.Humidity, *m.Value) {
		return nil
	}

	hum := NewHumidity(r.ID(), m.ID, *m.Value, m.Timestamp)
	err := onchange(hum)
	if err != nil {
		return err
	}

	r.Humidity = avg(r, m, *m.Value, hasHumidity)

	return nil
}

func (r *Room) handleTemperature(m Measurement, onchange func(m ValueProvider) error) error {

	const SensorValue = "/5700"

	if !(strings.HasSuffix(m.ID, SensorValue)) {
		return nil
	}

	if !hasChanged(r.Temperature, *m.Value) {
		return nil
	}

	temp := newTemperatureFromMeasurement(r.ID(), m)
	err := onchange(temp)
	if err != nil {
		return err
	}

	avgTemp := avg(r, m, *m.Value, hasTemperature)

	r.Temperature = Measurement{
		Value:     &avgTemp,
		Source:    m.Source,
		Timestamp: m.Timestamp,
	}

	return nil
}
