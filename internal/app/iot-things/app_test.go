package iotthings

import (
	"context"
	"strings"
	"testing"

	"github.com/diwise/iot-things/internal/app/iot-things/things"
)

func TestSeed(t *testing.T) {
	ctx := context.Background()

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, conditions ...ConditionFunc) (QueryResult, error) {
			return QueryResult{
				Things: [][]byte{},
			}, nil
		},
	}
	w := &ThingsWriterMock{
		AddThingFunc: func(ctx context.Context, t things.Thing) error {
			return nil
		},
	}

	app := New(r, w)
	app.Seed(ctx, strings.NewReader(csvData))
}

func TestSeedUpdate(t *testing.T) {
	ctx := context.Background()

	r := &ThingsReaderMock{
		QueryThingsFunc: func(ctx context.Context, conditions ...ConditionFunc) (QueryResult, error) {
			cond := newConditions(conditions...)
			id, ok := cond["id"]
			if ok && id == "5" {
				wc := things.NewWasteContainer("5", things.Location{62.39095613, 17.31727909}, "default")

				return QueryResult{
					Things: [][]byte{
						wc.Byte(),
					},
				}, nil

			}
			return QueryResult{
				Things: [][]byte{},
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

	app := New(r, w)
	app.Seed(ctx, strings.NewReader(csvData))
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
