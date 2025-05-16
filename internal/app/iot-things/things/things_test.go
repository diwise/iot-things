package things

import (
	"context"
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
	container.Handle(context.Background(), []Measurement{distance}, func(m ValueProvider) error {
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

	passage.Handle(context.Background(), []Measurement{digitalInputOn}, func(m ValueProvider) error {
		return nil
	})
	is.Equal(passage.CurrentState, true)
	passage.Handle(context.Background(), []Measurement{digitalInputOff}, func(m ValueProvider) error {
		return nil
	})
	is.Equal(passage.CurrentState, false)

	passage.Handle(context.Background(), []Measurement{digitalInputOn}, func(m ValueProvider) error {
		return nil
	})
	is.Equal(passage.CurrentState, true)
	passage.Handle(context.Background(), []Measurement{digitalInputOff}, func(m ValueProvider) error {
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

	passage.Handle(context.Background(), []Measurement{digitalInputOnYesterday}, func(m ValueProvider) error {
		return nil
	})
	passage.Handle(context.Background(), []Measurement{digitalInputOffYesterday}, func(m ValueProvider) error {
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
	sewer.Handle(context.Background(), []Measurement{distance}, func(m ValueProvider) error {
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

	sewer.Handle(context.Background(), []Measurement{digitalInputOn}, func(m ValueProvider) error {
		return nil
	})

	vb = false
	digitalInputOff := Measurement{
		ID:        "device/3200/5500",
		Urn:       "urn:oma:lwm2m:ext:3200",
		BoolValue: &vb,
		Timestamp: now.Add(1 * time.Hour),
	}

	sewer.Handle(context.Background(), []Measurement{digitalInputOff}, func(m ValueProvider) error {
		return nil
	})

	is.Equal(sewer.OverflowObserved, false)
	is.Equal(sewer.OverflowCumulativeTime, 2*time.Hour)
}

func TestSewerDigitalInput(t *testing.T) {
	is := is.New(t)

	thing := NewSewer("id", Location{Latitude: 62, Longitude: 17}, "default")
	sewer := thing.(*Sewer)

	now := time.Now()

	vb := false
	digitalInputOff := Measurement{
		ID:        "device/3200/5500",
		Urn:       "urn:oma:lwm2m:ext:3200",
		BoolValue: &vb,
		Timestamp: now.Add(-2 * time.Hour),
	}

	values := make([]Value, 0)
	filter := func(m ValueProvider) []Value {
		v := make([]Value, 0)
		for _, x := range m.Values() {
			if x.BoolValue != nil {
				v = append(v, x)
			}
		}
		return v
	}

	sewer.Handle(context.Background(), []Measurement{digitalInputOff}, func(m ValueProvider) error {
		values = append(values, filter(m)...)
		return nil
	})

	vb = true
	digitalInputOn := Measurement{
		ID:        "device/3200/5500",
		Urn:       "urn:oma:lwm2m:ext:3200",
		BoolValue: &vb,
		Timestamp: now.Add(-1 * time.Hour),
	}

	sewer.Handle(context.Background(), []Measurement{digitalInputOn}, func(m ValueProvider) error {
		values = append(values, filter(m)...)
		return nil
	})

	vb = false
	digitalInputOff = Measurement{
		ID:        "device/3200/5500",
		Urn:       "urn:oma:lwm2m:ext:3200",
		BoolValue: &vb,
		Timestamp: now.Add(1 * time.Hour),
	}

	sewer.Handle(context.Background(), []Measurement{digitalInputOff}, func(m ValueProvider) error {
		values = append(values, filter(m)...)
		return nil
	})

	vb = false
	digitalInputOff = Measurement{
		ID:        "device/3200/5500",
		Urn:       "urn:oma:lwm2m:ext:3200",
		BoolValue: &vb,
		Timestamp: now.Add(2 * time.Hour),
	}

	sewer.Handle(context.Background(), []Measurement{digitalInputOff}, func(m ValueProvider) error {
		values = append(values, filter(m)...)
		return nil
	})

	is.Equal(sewer.OverflowObserved, false)
	is.Equal(sewer.OverflowCumulativeTime, 2*time.Hour)
	is.Equal(len(values), 4)
}

func TestPumpingStation(t *testing.T) {
	is := is.New(t)

	thing := NewPumpingStation("id", Location{Latitude: 62, Longitude: 17}, "default")
	pumpingstation := thing.(*PumpingStation)

	now := time.Now()

	vb := true
	err := pumpingstation.Handle(context.Background(), []Measurement{
		{
			ID:        "device/3200/5500",
			Urn:       "urn:oma:lwm2m:ext:3200",
			BoolValue: &vb,
			Timestamp: now.Add(-1 * time.Hour),
		}}, func(m ValueProvider) error {
		return nil
	})

	is.NoErr(err)
}

func TestPumpingStationFalse(t *testing.T) {
	is := is.New(t)

	thing := NewPumpingStation("id", Location{Latitude: 62, Longitude: 17}, "default")
	pumpingstation := thing.(*PumpingStation)

	now := time.Now()

	vb := false
	err := pumpingstation.Handle(context.Background(), []Measurement{
		{
			ID:        "device/3200/5500",
			Urn:       "urn:oma:lwm2m:ext:3200",
			BoolValue: &vb,
			Timestamp: now.Add(-1 * time.Hour),
		}}, func(m ValueProvider) error {
		return nil
	})

	is.NoErr(err)
}
func TestRoom(t *testing.T) {
	is := is.New(t)

	thing := NewRoom("id", Location{Latitude: 62, Longitude: 17}, "default")
	room := thing.(*Room)

	//temperature
	v := 20.0
	temperature := Measurement{
		ID:        "device/3303/5700",
		Urn:       "urn:oma:lwm2m:ext:3303",
		Value:     &v,
		Timestamp: time.Now(),
	}
	room.Handle(context.Background(), []Measurement{temperature}, func(m ValueProvider) error {
		return nil
	})
	is.Equal(room.Temperature, 20.0)

	//humidity
	v = 50.0
	humidity := Measurement{
		ID:        "device/3304/5700",
		Urn:       "urn:oma:lwm2m:ext:3304",
		Value:     &v,
		Timestamp: time.Now(),
	}
	room.Handle(context.Background(), []Measurement{humidity}, func(m ValueProvider) error {
		return nil
	})
	is.Equal(room.Humidity, 50.0)

	//illuminance
	v = 1000.0
	illuminance := Measurement{
		ID:        "device/3301/5700",
		Urn:       "urn:oma:lwm2m:ext:3301",
		Value:     &v,
		Timestamp: time.Now(),
	}
	room.Handle(context.Background(), []Measurement{illuminance}, func(m ValueProvider) error {
		return nil
	})
	is.Equal(room.Illuminance, 1000.0)

	//air quality
	v = 0.5
	airQuality := Measurement{
		ID:        "device/3428/17",
		Urn:       "urn:oma:lwm2m:ext:3428",
		Value:     &v,
		Timestamp: time.Now(),
	}
	room.Handle(context.Background(), []Measurement{airQuality}, func(m ValueProvider) error {
		return nil
	})

	is.Equal(room.CO2, 0.5)
}

func TestPointOfInterest(t *testing.T){
	is := is.New(t)
	ctx := context.Background()

	thing := NewPointOfInterest("id", Location{Latitude: 62, Longitude: 17}, "default")
	poi := thing.(*PointOfInterest)

	v := 20.0
	src := "www.example.com"
	temperature := Measurement{
		ID:        "device/3303/5700",
		Urn:       "urn:oma:lwm2m:ext:3303",
		Value:     &v,
		Timestamp: time.Now(),
		Source: &src,
	}

	err := poi.Handle(ctx, []Measurement{temperature}, func(m ValueProvider) error {
		return nil
	})

	is.NoErr(err)

	is.Equal(20.0, *poi.Temperature.Value)
	is.Equal("www.example.com", *poi.Temperature.Source)
}