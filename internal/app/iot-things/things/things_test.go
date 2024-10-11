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
	distance := Value{
		ID:        "device/3330/5700",
		Urn:       "urn:oma:lwm2m:ext:3330",
		Value:     &v,
		Timestamp: time.Now(),
	}
	container.Handle(distance, func(m Measurements) error {
		return nil
	})

	is.Equal(container.CurrentLevel, 0.4)
	is.Equal(int(container.Percent), 50)
}

func TestPassage(t *testing.T){
	is := is.New(t)

	thing := NewPassage("id", Location{Latitude: 62, Longitude: 17}, "default")
	passage := thing.(*Passage)

	v := true
	digitalInput := Value{
		ID:        "device/3200/5500",
		Urn:       "urn:oma:lwm2m:ext:3200",
		BoolValue: &v,
		Timestamp: time.Now(),
	}

	passage.Handle(digitalInput, func(m Measurements) error {
		return nil
	})

	is.Equal(passage.State, true)
}