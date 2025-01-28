package things

import (
	"fmt"
	"math"
	"time"

	"github.com/diwise/senml"
)

const (
	lwm2mPrefix string = "urn:oma:lwm2m:ext:"

	AirQualityURN    string = lwm2mPrefix + "3428"
	ConductivityURN  string = lwm2mPrefix + "3327"
	DigitalInputURN  string = lwm2mPrefix + "3200"
	DistanceURN      string = lwm2mPrefix + "3330"
	DoorURN          string = "urn:oma:lwm2m:x:10351"
	EnergyURN        string = lwm2mPrefix + "3331"
	FillingLevelURN  string = lwm2mPrefix + "3435"
	HumidityURN      string = lwm2mPrefix + "3304"
	IlluminanceURN   string = lwm2mPrefix + "3301"
	PeopleCounterURN string = lwm2mPrefix + "3334"
	PowerURN         string = lwm2mPrefix + "3328"
	PresenceURN      string = lwm2mPrefix + "3302"
	PressureURN      string = lwm2mPrefix + "3323"
	StopwatchURN     string = lwm2mPrefix + "3350"
	TemperatureURN   string = lwm2mPrefix + "3303"
	WaterMeterURN    string = lwm2mPrefix + "3424"
)

var (
	BuildingURNs        = []string{EnergyURN, PowerURN, TemperatureURN}
	ContainerURNs       = []string{DistanceURN}
	LifebuoyURNs        = []string{DigitalInputURN, PresenceURN}
	PassageURNs         = []string{DigitalInputURN}
	PointOfInterestURNs = []string{TemperatureURN}
	PumpingStationURNs  = []string{DigitalInputURN}
	RoomURNs            = []string{TemperatureURN, HumidityURN, IlluminanceURN, AirQualityURN, PresenceURN}
	SewerURNs           = []string{DistanceURN, DigitalInputURN}
	WaterMeterURNs      = []string{WaterMeterURN}
	DeskURNs            = []string{DigitalInputURN, PresenceURN}
)

func hasChanged(a, b any) bool {
	switch a.(type) {
	case float64:
		return isNotZero(a.(float64) - b.(float64))
	case bool:
		return a.(bool) != b.(bool)
	case string:
		return a.(string) != b.(string)
	default:
		return true
	}
}

func isNotZero(v float64) bool {
	return (math.Abs(v) >= 0.001)
}

/* --------------------- Filling Level --------------------- */

type FillingLevel struct {
	Percentage Value
	Level      Value
}

func NewFillingLevel(id, ref string, percentage, level float64, ts time.Time) FillingLevel {
	return FillingLevel{
		Percentage: newActualFillingPercentage(id, ref, ts, percentage),
		Level:      newActualFillingLevel(id, ref, ts, level),
	}
}

func (f FillingLevel) Values() []Value {
	return []Value{f.Percentage, f.Level}
}

func newActualFillingPercentage(id, ref string, ts time.Time, value float64) Value {
	id = fmt.Sprintf("%s/%s/%s", id, "3435", "2")
	return newValue(id, FillingLevelURN, ref, "%", ts, value)
}

func newActualFillingLevel(id, ref string, ts time.Time, value float64) Value {
	id = fmt.Sprintf("%s/%s/%s", id, "3435", "3")
	return newValue(id, FillingLevelURN, ref, "m", ts, value)
}

/* --------------------- People Counter --------------------- */

type PeopleCounter struct {
	DailyNumberOfPassages     Value
	CumulatedNumberOfPassages Value
}

func NewPeopleCounter(id, ref string, daily int, cumulated int64, ts time.Time) PeopleCounter {
	return PeopleCounter{
		DailyNumberOfPassages:     newDailyNumberOfPassages(id, ref, ts, daily),
		CumulatedNumberOfPassages: newCumulatedNumberOfPassages(id, ref, ts, cumulated),
	}
}

func (p PeopleCounter) Values() []Value {
	return []Value{p.DailyNumberOfPassages, p.CumulatedNumberOfPassages}
}

func newDailyNumberOfPassages(id, ref string, ts time.Time, value int) Value {
	id = fmt.Sprintf("%s/%s/%s", id, "3434", "5")
	return newValue(id, PeopleCounterURN, ref, "", ts, float64(value))
}

func newCumulatedNumberOfPassages(id, ref string, ts time.Time, value int64) Value {
	id = fmt.Sprintf("%s/%s/%s", id, "3434", "6")
	return newValue(id, PeopleCounterURN, ref, "", ts, float64(value))
}

/* --------------------- Door --------------------- */

type Door struct {
	Status Value
}

func NewDoor(id, ref string, state bool, ts time.Time) Door {
	id = fmt.Sprintf("%s/%s/%s", id, "10351", "50")
	return Door{
		Status: newBoolValue(id, DoorURN, ref, "", ts, state),
	}
}

func (d Door) Values() []Value {
	return []Value{d.Status}
}

/* --------------------- Temperature --------------------- */

type Temperature struct {
	Value Value
}

func NewTemperature(id, ref string, value float64, ts time.Time) Temperature {
	id = fmt.Sprintf("%s/%s/%s", id, "3303", "5700")
	return Temperature{
		Value: newValue(id, TemperatureURN, ref, "Cel", ts, value),
	}
}

func (t Temperature) Values() []Value {
	return []Value{t.Value}
}

/* --------------------- Humidity --------------------- */

type Humidity struct {
	Value Value
}

func NewHumidity(id, ref string, value float64, ts time.Time) Humidity {
	id = fmt.Sprintf("%s/%s/%s", id, "3304", "5700")
	return Humidity{
		Value: newValue(id, HumidityURN, ref, "%", ts, value),
	}
}
func (h Humidity) Values() []Value {
	return []Value{h.Value}
}

/* --------------------- Illuminance --------------------- */
type Illuminance struct {
	Value Value
}

func NewIlluminance(id, ref string, value float64, ts time.Time) Illuminance {
	id = fmt.Sprintf("%s/%s/%s", id, "3301", "5700")
	return Illuminance{
		Value: newValue(id, IlluminanceURN, ref, "lux", ts, value),
	}
}
func (i Illuminance) Values() []Value {
	return []Value{i.Value}
}

/* --------------------- Air Quality --------------------- */
type AirQuality struct {
	CO2 Value
}

func NewAirQuality(id, ref string, value float64, ts time.Time) AirQuality {
	id = fmt.Sprintf("%s/%s/%s", id, "3428", "17")
	return AirQuality{
		CO2: newValue(id, AirQualityURN, ref, "ppm", ts, value),
	}
}
func (a AirQuality) Values() []Value {
	return []Value{a.CO2}
}

/* --------------------- Presence --------------------- */

type Presence struct {
	Value Value
}

func NewPresence(id, ref string, value bool, ts time.Time) Presence {
	id = fmt.Sprintf("%s/%s/%s", id, "3302", "5500")
	return Presence{
		Value: newBoolValue(id, PresenceURN, ref, "", ts, value),
	}
}

func (d Presence) Values() []Value {
	return []Value{d.Value}
}

/* --------------------- Stopwatch --------------------- */

type Stopwatch struct {
	CumulativeTime *Value
	OnOff          *Value
}

func NewStopwatch(id, ref string, cumulativeTime *float64, onOff bool, ts time.Time) Stopwatch {
	var ct, oo *Value
	if cumulativeTime != nil {
		c := newValue(fmt.Sprintf("%s/%s/%s", id, "3350", "5544"), StopwatchURN, ref, "s", ts, *cumulativeTime)
		ct = &c
	}
	o := newBoolValue(fmt.Sprintf("%s/%s/%s", id, "3350", "5850"), StopwatchURN, ref, "", ts, onOff)
	oo = &o

	return Stopwatch{
		CumulativeTime: ct,
		OnOff:          oo,
	}
}

func (sw Stopwatch) Values() []Value {
	values := []Value{}
	if sw.CumulativeTime != nil {
		values = append(values, *sw.CumulativeTime)
	}
	values = append(values, *sw.OnOff)

	return values
}

/* --------------------- Energy --------------------- */

type Energy struct {
	Value Value
}

func NewEnergy(id, ref string, v float64, ts time.Time) Energy {
	energy := newValue(fmt.Sprintf("%s/%s/%s", id, "3331", "5700"), EnergyURN, ref, "kWh", ts, v)

	return Energy{
		Value: energy,
	}
}

func (e Energy) Values() []Value {
	return []Value{
		e.Value,
	}
}

/* --------------------- Power --------------------- */

type Power struct {
	Value Value
}

func NewPower(id, ref string, v float64, ts time.Time) Power {
	pwr := newValue(fmt.Sprintf("%s/%s/%s", id, "3328", "5700"), PowerURN, ref, "kW", ts, v)

	return Power{
		Value: pwr,
	}
}

func (p Power) Values() []Value {
	return []Value{
		p.Value,
	}
}

/* --------------------- WaterMeter --------------------- */

type WaterMeter struct {
	CumulatedWaterVolume Value
	LeakDetected         Value
	BackFlowDetected     Value
	FraudDetected        Value
}

func NewWaterMeter(id, ref string, v float64, l, b, f bool, ts time.Time) WaterMeter {
	vol := newValue(fmt.Sprintf("%s/%s/%s", id, "3424", "1"), WaterMeterURN, ref, senml.UnitCubicMeter, ts, v)
	leak := newBoolValue(fmt.Sprintf("%s/%s/%s", id, "3424", "10"), WaterMeterURN, ref, "", ts, l)
	backflow := newBoolValue(fmt.Sprintf("%s/%s/%s", id, "3424", "11"), WaterMeterURN, ref, "", ts, b)
	fraud := newBoolValue(fmt.Sprintf("%s/%s/%s", id, "3424", "13"), WaterMeterURN, ref, "", ts, f)

	return WaterMeter{
		CumulatedWaterVolume: vol,
		LeakDetected:         leak,
		BackFlowDetected:     backflow,
		FraudDetected:        fraud,
	}
}

func (p WaterMeter) Values() []Value {
	return []Value{
		p.CumulatedWaterVolume,
		p.LeakDetected,
		p.BackFlowDetected,
		p.FraudDetected,
	}
}
