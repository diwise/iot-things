package iotthings

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/diwise/iot-things/internal/app/iot-things/things"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

func TestRoomTemperature(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	r := things.NewRoom("room-001", things.DefaultLocation, "default")
	r.AddDevice("c5a2ae17c239")

	NewMeasurementsHandler(appMock(r, nil), msgCtxMock())(ctx, msgMock(temperatureMsg), slog.Default())

	is.Equal(r.(*things.Room).Temperature, 21.0)
}

func TestContainerDistance(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	c := things.NewContainer("container-001", things.DefaultLocation, "default")
	c.AddDevice("9fb5801ebafc")

	maxd := 3.0
	maxl := 2.8
	c.(*things.Container).MaxDistance = &maxd
	c.(*things.Container).MaxLevel = &maxl

	NewMeasurementsHandler(appMock(c, nil), msgCtxMock())(ctx, msgMock(distanceMsg), slog.Default())

	is.Equal(c.(*things.Container).CurrentLevel, 0.49) //3.0 - 2.51
	is.Equal(c.(*things.Container).Percent, 17.5)
}

func TestPassageDigitalInput(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	p := things.NewPassage("passage-001", things.DefaultLocation, "default")
	p.AddDevice("ce3acc09ab62")
	p.(*things.Passage).ValidURN = things.PassageURNs

	off := func(ts time.Time) *messaging.IncomingTopicMessageMock {
		return msgMock(fmt.Sprintf(digitalInputMsg, ts.Unix(), "false"))
	}
	on := func(ts time.Time) *messaging.IncomingTopicMessageMock {
		return msgMock(fmt.Sprintf(digitalInputMsg, ts.Unix(), "true"))
	}

	v := map[string][]things.Value{}
	a := appMock(p, v)
	m := msgCtxMock()

	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	tomorrow := now.AddDate(0, 0, 1)

	messages := []*messaging.IncomingTopicMessageMock{
		off(yesterday),
		on(yesterday),
		off(yesterday),
		on(now),
		off(now),
		on(now),
		off(now),
		on(tomorrow),
		off(tomorrow),
	}

	h := NewMeasurementsHandler(a, m)

	for _, msg := range messages {
		h(ctx, msg, slog.Default())
	}

	is.Equal(p.(*things.Passage).CumulatedNumberOfPassages, int64(4))
	is.Equal(p.(*things.Passage).PassagesToday, 2)
	is.Equal(p.(*things.Passage).ObservedAt.Unix(), tomorrow.Unix())
}

func appMock(t things.Thing, v map[string][]things.Value) *ThingsAppMock {
	return &ThingsAppMock{
		GetConnectedThingsFunc: func(ctx context.Context, deviceID string) ([]things.Thing, error) {
			return []things.Thing{t}, nil
		},
		AddValueFunc: func(ctx context.Context, t things.Thing, m things.Value) error {
			if v == nil {
				v = map[string][]things.Value{}
			}
			v[t.ID()] = append(v[t.ID()], m)
			return nil
		},
		SaveThingFunc: func(ctx context.Context, t things.Thing) error {
			return nil
		},
	}
}

func msgCtxMock() *messaging.MsgContextMock {
	return &messaging.MsgContextMock{
		PublishOnTopicFunc: func(ctx context.Context, message messaging.TopicMessage) error {
			return nil
		},
	}
}

func msgMock(body string) *messaging.IncomingTopicMessageMock {
	return &messaging.IncomingTopicMessageMock{
		BodyFunc: func() []byte {
			return []byte(body)
		},
		TopicNameFunc: func() string {
			return "message.accepted"
		},
		ContentTypeFunc: func() string {
			return "application/json"
		},
	}
}

var (
	temperatureMsg  = `{"pack":[{"bn":"c5a2ae17c239/3303/","bt":1730124834,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},{"n":"5700","u":"Cel","v":21},{"u":"lat","v":0},{"u":"lon","v":0},{"n":"tenant","vs":"default"}],"timestamp":"2024-10-28T14:13:54.532480028Z"}`
	distanceMsg     = `{"pack":[{"bn":"9fb5801ebafc/3330/","bt":1730124849,"n":"0","vs":"urn:oma:lwm2m:ext:3330"},{"n":"5700","u":"m","v":2.51},{"n":"5701","vs":"metre"},{"u":"lat","v":62},{"u":"lon","v":17},{"n":"tenant","vs":"default"}],"timestamp":"2024-10-28T14:14:09.424249918Z"}`
	digitalInputMsg = `{"pack":[{"bn":"ce3acc09ab62/3200/","bt":%d,"n":"0","vs":"urn:oma:lwm2m:ext:3200"},{"n":"5500","vb":%s},{"n":"5501","v":5},{"u":"lat","v":0},{"u":"lon","v":0},{"n":"tenant","vs":"default"}],"timestamp":"2024-10-29T01:40:34.003076718Z"}`
)
