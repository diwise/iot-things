package functions

import (
	"errors"
	"fmt"
	"math"
	"time"
)

type Level interface {
	Calc(distance float64, ts time.Time) (bool, error)
	Current() float64
	Offset() float64
	Percent() float64
}

type LevelConfig struct {
	MaxDistance *float64 `json:"maxd,omitempty"`
	MaxLevel    *float64 `json:"maxl,omitempty"`
	MeanLevel   *float64 `json:"meanl,omitempty"`
	Offset      *float64 `json:"offset,omitempty"`
	Angle       *float64 `json:"angle,omitempty"`
}

type level struct {
	cosAlpha    float64
	maxDistance float64
	maxLevel    float64
	meanLevel   float64
	offsetLevel float64

	Current_ float64  `json:"current"`
	Percent_ *float64 `json:"percent,omitempty"`
	Offset_  *float64 `json:"offset,omitempty"`
}

func NewLevel(angle, maxDistance, maxLevel, meanLevel, offset *float64, current float64) (Level, error) {
	lvl := &level{
		cosAlpha: 1.0,
	}

	if angle != nil && (*angle < 0 || *angle >= 90.0) {
		return nil, fmt.Errorf("level angle %f not within allowed [0, 90) range", *angle)
	}
	if angle != nil {
		lvl.cosAlpha = math.Cos(*angle * math.Pi / 180.0)
	}

	f := func(value *float64, v float64) float64 {
		if value != nil {
			return *value
		}
		return v
	}

	lvl.maxDistance = f(maxDistance, current)
	lvl.maxLevel = f(maxLevel, current)
	lvl.meanLevel = f(meanLevel, 0)
	lvl.offsetLevel = f(offset, 0)

	lvl.Current_ = current

	if isNotZero(lvl.maxLevel) {
		pct := math.Min((lvl.Current_*100.0)/lvl.maxLevel, 100.0)
		if pct < 0 {
			pct = 0
		}
		lvl.Percent_ = &pct
	}

	if isNotZero(lvl.meanLevel) {
		offset := lvl.Current_ - lvl.meanLevel
		lvl.Offset_ = &offset
	}

	return lvl, nil
}

func (l *level) Calc(distance float64, ts time.Time) (bool, error) {
	rnd := func(v float64) float64 {
		return math.Round(v*1e5) / 1e5
	}

	var errs []error

	previousLevel := l.Current_

	currentLevel := rnd((l.maxDistance - distance) * l.cosAlpha)

	if l.offsetLevel != 0 && currentLevel < l.offsetLevel {
		currentLevel = l.offsetLevel
	}

	l.Current_ = currentLevel

	if !hasChanged(previousLevel, l.Current_) {
		return false, nil
	}

	if isNotZero(l.maxLevel) {
		pct := math.Min((l.Current_*100.0)/l.maxLevel, 100.0)
		l.Percent_ = &pct
	}

	if isNotZero(l.meanLevel) {
		offset := l.Current_ - l.meanLevel
		l.Offset_ = &offset
	}

	return true, errors.Join(errs...)

}

func (l *level) Current() float64 {
	return l.Current_
}

func (l *level) Offset() float64 {
	if l.Offset_ != nil {
		return *l.Offset_
	}

	return 0.0
}

func (l *level) Percent() float64 {
	if l.Percent_ != nil {
		return *l.Percent_
	}

	return 0.0
}

func hasChanged(prev, new float64) bool {
	return isNotZero(new - prev)
}

func isNotZero(value float64) bool {
	return (math.Abs(value) >= 0.0001)
}
