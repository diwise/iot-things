package things

import (
	"fmt"
	"time"
)

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
	return newValue(id, "urn:oma:lwm2m:ext:3435", ref, "%", ts, value)
}

func newActualFillingLevel(id, ref string, ts time.Time, value float64) Value {
	id = fmt.Sprintf("%s/%s/%s", id, "3435", "3")
	return newValue(id, "urn:oma:lwm2m:ext:3435", ref, "m", ts, value)
}

/* --------------------- People Counter --------------------- */

type PeopleCounter struct {
	DailyNumberOfPassages     Value
	CumulatedNumberOfPassages Value
}

func NewPeopleCounter(id, ref string, daily, cumulated int64, ts time.Time) PeopleCounter {
	return PeopleCounter{
		DailyNumberOfPassages:     newDailyNumberOfPassages(id, ref, ts, daily),
		CumulatedNumberOfPassages: newCumulatedNumberOfPassages(id, ref, ts, cumulated),
	}
}

func (p PeopleCounter) Values() []Value {
	return []Value{p.DailyNumberOfPassages, p.CumulatedNumberOfPassages}
}

func newDailyNumberOfPassages(id, ref string, ts time.Time, value int64) Value {
	id = fmt.Sprintf("%s/%s/%s", id, "3434", "5")
	return newValue(id, "urn:oma:lwm2m:ext:3434", ref, "", ts, float64(value))
}

func newCumulatedNumberOfPassages(id, ref string, ts time.Time, value int64) Value {
	id = fmt.Sprintf("%s/%s/%s", id, "3434", "6")
	return newValue(id, "urn:oma:lwm2m:ext:3434", ref, "", ts, float64(value))
}

/* --------------------- Door --------------------- */

type Door struct {
	Status Value
}

func NewDoor(id, ref string, state bool, ts time.Time) Door {
	id = fmt.Sprintf("%s/%s/%s", id, "10351", "50")
	return Door{
		Status: newBoolValue(id, "urn:oma:lwm2m:x:10351", ref, "", ts, state),
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
		Value: newValue(id, "urn:oma:lwm2m:ext:3303", ref, "Cel", ts, value),
	}
}

func (t Temperature) Values() []Value {
	return []Value{t.Value}
}

/* --------------------- Presence --------------------- */

type Presence struct {
	Value Value 
}

func NewPresence(id, ref string, value bool, ts time.Time) Presence {
	id = fmt.Sprintf("%s/%s/%s", id, "3302", "5500")
	return Presence{
		Value: newBoolValue(id, "urn:oma:lwm2m:ext:3302", ref, "", ts, value),
	}
}

func (d Presence) Values() []Value {
	return []Value{d.Value}
}

/* --------------------- Stopwatch --------------------- */

type Stopwatch struct {
	CumulativeTime Value
	OnOff          Value
}

func NewStopwatch(id, ref string, cumulativeTime float64, onOff bool, ts time.Time) Stopwatch {
	ct := newValue(fmt.Sprintf("%s/%s/%s", id, "3350", "5544"), "urn:oma:lwm2m:ext:3350", ref, "s", ts, cumulativeTime)
	oo := newBoolValue(fmt.Sprintf("%s/%s/%s", id, "3350", "5850"), "urn:oma:lwm2m:ext:3350", ref, "", ts, onOff)

	return Stopwatch{
		CumulativeTime: ct,
		OnOff:          oo,
	}
}

func (sw Stopwatch) Values() []Value {
	return []Value{
		sw.CumulativeTime,
		sw.OnOff,
	}
}
