package iotthings

import (
	"context"
	"strings"
	"testing"

	"github.com/diwise/iot-things/internal/app/iot-things/things"
	"github.com/matryer/is"
)

func TestSeed(t *testing.T) {
	ctx := context.Background()

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, conditions ...ConditionFunc) (QueryResult, error) {
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
	app.Seed(ctx, strings.NewReader(csvData))
}

func TestSeedUpdate(t *testing.T) {
	ctx := context.Background()

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, conditions ...ConditionFunc) (QueryResult, error) {
			cond := newConditions(conditions...)
			id, ok := cond["id"]
			l := things.Location{
				Latitude:  62.39095613,
				Longitude: 17.31727909,
			}
			if ok && id == "5" {
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

	app := New(ctx,r, w, msgCtxMock())
	app.Seed(ctx, strings.NewReader(csvData))
}

func TestLoadConfig(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, conditions ...ConditionFunc) (QueryResult, error) {
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

	app := New(ctx,r, w, msgCtxMock())
	err := app.LoadConfig(ctx, strings.NewReader(yamlConfig))
	is.NoErr(err)
}

func newConditions(conditions ...ConditionFunc) map[string]any {
	m := make(map[string]any)

	for _, f := range conditions {
		m = f(m)
	}

	if _, ok := m["limit"]; !ok {
		m["limit"] = 100
	}

	if _, ok := m["offset"]; !ok {
		m["offset"] = 0
	}

	return m
}

const csvData string = `id;type;subType;name;decsription;location;tenant;tags;refDevices;args
forradet-bpn;Sewer;CombinedSewageOverflow;Förrådet BPN;Förrådet BPN;62.4008,17.4135;msva;braddmatare;d4f3e2f1-d430-467b-85ec-7cd977b0335f;
5;Container;WasteContainer;namn;beskrivning;62.39095613,17.31727909;default;soptunna,linje 1;d4f3e2f1-d430-467b-85ec-7cd977b0335f,527090f3-7f85-49f8-889b-99a50530dede;{'max_distance':0.94,'max_level':0.79}
`
