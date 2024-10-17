package functions

import (
	"time"
)

type Stopwatch struct {
	StartTime      *time.Time     `json:"startTime"`
	StopTime       *time.Time     `json:"stopTime"`
	State          bool           `json:"state"`
	Duration       *time.Duration `json:"duration"`
	CumulativeTime time.Duration  `json:"cumulativeTime"`
}

func NewStopwatch() *Stopwatch {
	return &Stopwatch{}
}

func (sw *Stopwatch) Push(state bool, ts time.Time, onchange func(sw Stopwatch) error) error {
	currentState := sw.State

	// On
	if state {
		// Off -> On = Start new stopwatch
		if !currentState {
			utc := ts.UTC()
			sw.StartTime = &utc
			sw.State = true
			sw.StopTime = nil // setting end time and duration to nil values to ensure we don't send out the wrong ones later
			sw.Duration = nil

			onchange(*sw)
		}

		// On -> On = Update duration
		if currentState {
			duration := ts.Sub(*sw.StartTime)
			sw.Duration = &duration
		}
	}

	// Off
	if !state {
		// On -> Off = Stop stopwatch
		if currentState {
			sw.StopTime = &ts
			sw.State = false
			duration := ts.Sub(*sw.StartTime)
			sw.Duration = &duration
			sw.CumulativeTime += *sw.Duration

			onchange(*sw)

			sw.StartTime = nil
			sw.Duration = nil
		}

		// Off -> Off = Do nothing
		if !currentState {
			return nil
		}
	}

	sw.State = state

	return nil
}
