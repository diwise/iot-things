package application

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/diwise/iot-things/internal/application/things"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

func TestSeed(t *testing.T) {
	ctx := context.Background()

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			return QueryResult{
				Data: [][]byte{},
			}, nil
		},
	}
	w := &ThingsWriterMock{
		AddThingFunc: func(ctx context.Context, t things.Thing) error {
			return nil
		},
	}

	app := New(ctx, r, w, msgCtxMock())
	err := app.Seed(ctx, strings.NewReader(csvData))
	if err != nil {
		t.Fatalf("Seed returned error: %v", err)
	}
}

func TestSeedUpdate(t *testing.T) {
	ctx := context.Background()

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			l := things.Location{
				Latitude:  62.39095613,
				Longitude: 17.31727909,
			}
			if query.ID != nil && *query.ID == "5" {
				wc := things.NewWasteContainer("5", l, "default")

				return QueryResult{
					Data: [][]byte{
						wc.Byte(),
					},
				}, nil

			}
			return QueryResult{
				Data: [][]byte{},
			}, nil
		},
	}
	w := &ThingsWriterMock{
		AddThingFunc: func(ctx context.Context, t things.Thing) error {
			return nil
		},
		UpdateThingFunc: func(ctx context.Context, t things.Thing) error {
			return nil
		},
	}

	app := New(ctx, r, w, msgCtxMock())
	err := app.Seed(ctx, strings.NewReader(csvData))
	if err != nil {
		t.Fatalf("Seed returned error: %v", err)
	}
}

func TestSeedRejectsMalformedCSVRow(t *testing.T) {
	ctx := context.Background()

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			return QueryResult{Data: [][]byte{}}, nil
		},
	}
	w := &ThingsWriterMock{}

	app := New(ctx, r, w, msgCtxMock())
	err := app.Seed(ctx, strings.NewReader("id;type;subType;name;decsription;location;tenant;tags;refDevices;args\nshort;row\n"))
	if err == nil {
		t.Fatal("expected Seed to reject malformed CSV row")
	}

	if !strings.Contains(err.Error(), "failed to read csv row 2") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadConfig(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			return QueryResult{
				Data: [][]byte{},
			}, nil
		},
	}
	w := &ThingsWriterMock{
		AddThingFunc: func(ctx context.Context, t things.Thing) error {
			return nil
		},
	}

	yamlConfig := `
types:
  - type: "exampleType1"
    subTypes:
      - "subType1A"
      - "subType1B"
  - type: "exampleType2"
    subTypes:
      - "subType2A"
      - "subType2B"
      - "subType2C"
`

	app := New(ctx, r, w, msgCtxMock())
	err := app.LoadConfig(ctx, strings.NewReader(yamlConfig))
	is.NoErr(err)
}

func TestHandleMeasurementsPublishesWithContextValuesAfterCancellation(t *testing.T) {
	type contextKey string

	const traceKey contextKey = "trace-id"
	const traceValue = "trace-123"

	appCtx := t.Context()

	ingressCtx, cancelIngress := context.WithCancel(context.WithValue(context.Background(), traceKey, traceValue))

	room := things.NewRoom("room-001", things.DefaultLocation, "default")
	room.AddDevice("device-1")

	currentThing := room
	allowPublishQuery := make(chan struct{})
	published := make(chan struct{}, 1)

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, query ThingQuery) (QueryResult, error) {
			if query.RefDeviceID != nil {
				return QueryResult{Data: [][]byte{currentThing.Byte()}}, nil
			}

			if query.ID != nil && *query.ID == currentThing.ID() {
				<-allowPublishQuery

				if got := ctx.Value(traceKey); got != traceValue {
					t.Fatalf("expected trace value %q in publisher query context, got %v", traceValue, got)
				}
				if err := ctx.Err(); err != nil {
					return QueryResult{}, err
				}

				return QueryResult{Data: [][]byte{currentThing.Byte()}}, nil
			}

			return QueryResult{Data: [][]byte{}}, nil
		},
	}
	w := &ThingsWriterMock{
		AddValueFunc: func(ctx context.Context, t things.Thing, m things.Value) error {
			return nil
		},
		UpdateThingFunc: func(ctx context.Context, t things.Thing) error {
			currentThing = t
			return nil
		},
	}
	m := &messaging.MsgContextMock{
		PublishOnTopicFunc: func(ctx context.Context, message messaging.TopicMessage) error {
			if got := ctx.Value(traceKey); got != traceValue {
				t.Fatalf("expected trace value %q in publish context, got %v", traceValue, got)
			}
			if err := ctx.Err(); err != nil {
				t.Fatalf("expected detached publish context, got err %v", err)
			}

			published <- struct{}{}
			return nil
		},
	}

	a := New(appCtx, r, w, m)

	value := 21.0
	a.HandleMeasurements(ingressCtx, []things.Measurement{{
		ID:        "device-1/3303/5700",
		Urn:       things.TemperatureURN,
		Value:     &value,
		Timestamp: time.Now().UTC(),
	}})

	cancelIngress()
	close(allowPublishQuery)

	select {
	case <-published:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for thing.updated publish")
	}
}

const csvData string = `id;type;subType;name;decsription;location;tenant;tags;refDevices;args
forradet-bpn;Sewer;CombinedSewageOverflow;Förrådet BPN;Förrådet BPN;62.4008,17.4135;msva;braddmatare;d4f3e2f1-d430-467b-85ec-7cd977b0335f;
5;Container;WasteContainer;namn;beskrivning;62.39095613,17.31727909;default;soptunna,linje 1;d4f3e2f1-d430-467b-85ec-7cd977b0335f,527090f3-7f85-49f8-889b-99a50530dede;{'max_distance':0.94,'max_level':0.79}
`
