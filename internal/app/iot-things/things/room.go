package things

import (
	"encoding/json"
	"errors"
	"strings"
)

type Room struct {
	thingImpl
	Temperature float64 `json:"temperature"`
	Humidity    float64 `json:"humidity"`
	Illuminance float64 `json:"illuminance"`
	CO2         float64 `json:"co2"`
	//Presence    bool    `json:"presence"`
}

func NewRoom(id string, l Location, tenant string) Thing {
	return &Room{
		thingImpl: newThingImpl(id, "Room", l, tenant),
	}
}

func (r *Room) Handle(m []Measurement, onchange func(m ValueProvider) error) error {
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

	//if hasPresence(&m) {
	//	return r.handlePresence(m, onchange)
	//}

	return nil
}

/*
func (r *Room) handlePresence(m Measurement, onchange func(m ValueProvider) error) error {

		const Presence = "/5500"

		if !(strings.HasSuffix(m.ID, Presence)) {
			return nil
		}

		if !hasChanged(r.Presence, *m.BoolValue) {
			return nil
		}

		pres := NewPresence(r.ID(), m.ID, *m.BoolValue, m.Timestamp)
		err := onchange(pres)
		if err != nil {
			return err
		}

		r.Presence = *m.BoolValue

		return nil
	}
*/
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

	r.CO2 = avg(r, m.ID, *m.Value, hasAirQuality)

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

	r.Illuminance = avg(r, m.ID, *m.Value, hasIlluminance)

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

	r.Humidity = avg(r, m.ID, *m.Value, hasHumidity)

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

	temp := NewTemperature(r.ID(), m.ID, *m.Value, m.Timestamp)
	err := onchange(temp)
	if err != nil {
		return err
	}

	r.Temperature = avg(r, m.ID, *m.Value, hasTemperature)

	return nil
}

func (r *Room) Byte() []byte {
	b, _ := json.Marshal(r)
	return b
}
