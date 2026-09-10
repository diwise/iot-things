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

	// Jämför det nya aggregerade värdet mot det sparade, inte mot en enskild
	// sensors värde. Endast CO2-resursen (17) deltar, inte andra resurser i
	// samma AirQuality-objekt.
	newCO2 := avg(r, m, *m.Value, hasCO2)
	if !hasChanged(r.CO2, newCO2) {
		return nil
	}

	air := NewAirQuality(r.ID(), m.ID, *m.Value, m.Timestamp)
	if err := onchange(air); err != nil {
		return err
	}

	r.CO2 = newCO2

	return nil
}

func (r *Room) handleIlluminance(m Measurement, onchange func(m ValueProvider) error) error {

	const SensorValue = "/5700"

	if !(strings.HasSuffix(m.ID, SensorValue)) {
		return nil
	}

	newIlluminance := avg(r, m, *m.Value, hasIlluminance)
	if !hasChanged(r.Illuminance, newIlluminance) {
		return nil
	}

	ill := NewIlluminance(r.ID(), m.ID, *m.Value, m.Timestamp)
	if err := onchange(ill); err != nil {
		return err
	}

	r.Illuminance = newIlluminance

	return nil
}

func (r *Room) handleHumidity(m Measurement, onchange func(m ValueProvider) error) error {

	const SensorValue = "/5700"

	if !(strings.HasSuffix(m.ID, SensorValue)) {
		return nil
	}

	newHumidity := avg(r, m, *m.Value, hasHumidity)
	if !hasChanged(r.Humidity, newHumidity) {
		return nil
	}

	hum := NewHumidity(r.ID(), m.ID, *m.Value, m.Timestamp)
	if err := onchange(hum); err != nil {
		return err
	}

	r.Humidity = newHumidity

	return nil
}

func (r *Room) handleTemperature(m Measurement, onchange func(m ValueProvider) error) error {

	const SensorValue = "/5700"

	if !(strings.HasSuffix(m.ID, SensorValue)) {
		return nil
	}

	// Beräkna det nya aggregerade värdet först och jämför med det sparade
	// medelvärdet. Att jämföra med en enskild sensors värde gav falskt
	// "oförändrat" när sensorns värde råkade sammanfalla med medelvärdet.
	avgTemp := avg(r, m, *m.Value, hasTemperature)
	if !hasChanged(r.Temperature, avgTemp) {
		return nil
	}

	temp := newTemperatureFromMeasurement(r.ID(), m)
	if err := onchange(temp); err != nil {
		return err
	}

	r.Temperature = Measurement{
		Value:     &avgTemp,
		Source:    m.Source,
		Timestamp: m.Timestamp,
	}

	return nil
}
