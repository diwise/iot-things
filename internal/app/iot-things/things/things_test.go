package things

import (
	"testing"
	"time"

	"github.com/matryer/is"
)

func TestContainer(t *testing.T) {
	is := is.New(t)

	thing := NewContainer("id", Location{Latitude: 62, Longitude: 17}, "default")
	container := thing.(*Container)

	maxd := 0.94
	maxl := 0.79
	container.MaxDistance = &maxd
	container.MaxLevel = &maxl

	v := 0.54
	distance := Measurement{
		ID:        "device/3330/5700",
		Urn:       "urn:oma:lwm2m:ext:3330",
		Value:     &v,
		Timestamp: time.Now(),
	}
	container.Handle([]Measurement{distance}, func(m ValueProvider) error {
		return nil
	})

	is.Equal(container.CurrentLevel, 0.4)
	is.Equal(int(container.Percent), 50)
}

func TestPassage(t *testing.T) {
	is := is.New(t)

	thing := NewPassage("id", Location{Latitude: 62, Longitude: 17}, "default")
	passage := thing.(*Passage)

	on := true
	digitalInputOn := Measurement{
		ID:        "device/3200/5500",
		Urn:       "urn:oma:lwm2m:ext:3200",
		BoolValue: &on,
		Timestamp: time.Now(),
	}

	off := false
	digitalInputOff := Measurement{
		ID:        "device/3200/5500",
		Urn:       "urn:oma:lwm2m:ext:3200",
		BoolValue: &off,
		Timestamp: time.Now(),
	}

	passage.Handle([]Measurement{digitalInputOn}, func(m ValueProvider) error {
		return nil
	})
	is.Equal(passage.CurrentState, true)
	passage.Handle([]Measurement{digitalInputOff}, func(m ValueProvider) error {
		return nil
	})
	is.Equal(passage.CurrentState, false)

	passage.Handle([]Measurement{digitalInputOn}, func(m ValueProvider) error {
		return nil
	})
	is.Equal(passage.CurrentState, true)
	passage.Handle([]Measurement{digitalInputOff}, func(m ValueProvider) error {
		return nil
	})
	is.Equal(passage.CurrentState, false)

	digitalInputOnYesterday := Measurement{
		ID:        "device/3200/5500",
		Urn:       "urn:oma:lwm2m:ext:3200",
		BoolValue: &on,
		Timestamp: time.Now().Add(-24 * time.Hour),
	}
	digitalInputOffYesterday := Measurement{
		ID:        "device/3200/5500",
		Urn:       "urn:oma:lwm2m:ext:3200",
		BoolValue: &off,
		Timestamp: time.Now().Add(-24 * time.Hour),
	}

	passage.Handle([]Measurement{digitalInputOnYesterday}, func(m ValueProvider) error {
		return nil
	})
	passage.Handle([]Measurement{digitalInputOffYesterday}, func(m ValueProvider) error {
		return nil
	})

	is.Equal(passage.CurrentState, false)
	is.Equal(passage.CumulatedNumberOfPassages, int64(3))
	is.Equal(passage.PassagesToday, 2)
}

func TestSewer(t *testing.T) {
	is := is.New(t)

	thing := NewSewer("id", Location{Latitude: 62, Longitude: 17}, "default")
	sewer := thing.(*Sewer)

	maxd := 0.94
	maxl := 0.79
	sewer.MaxDistance = &maxd
	sewer.MaxLevel = &maxl

	v := 0.54
	distance := Measurement{
		ID:        "device/3330/5700",
		Urn:       "urn:oma:lwm2m:ext:3330",
		Value:     &v,
		Timestamp: time.Now(),
	}
	sewer.Handle([]Measurement{distance}, func(m ValueProvider) error {
		return nil
	})

	is.Equal(sewer.CurrentLevel, 0.4)
	is.Equal(int(sewer.Percent), 50)

	now := time.Now()

	vb := true
	digitalInputOn := Measurement{
		ID:        "device/3200/5500",
		Urn:       "urn:oma:lwm2m:ext:3200",
		BoolValue: &vb,
		Timestamp: now.Add(-1 * time.Hour),
	}

	sewer.Handle([]Measurement{digitalInputOn}, func(m ValueProvider) error {
		return nil
	})

	vb = false
	digitalInputOff := Measurement{
		ID:        "device/3200/5500",
		Urn:       "urn:oma:lwm2m:ext:3200",
		BoolValue: &vb,
		Timestamp: now.Add(1 * time.Hour),
	}

	sewer.Handle([]Measurement{digitalInputOff}, func(m ValueProvider) error {
		return nil
	})

	is.Equal(sewer.OverflowObserved, false)
	is.Equal(sewer.OverflowCumulativeTime, 2*time.Hour)
}
