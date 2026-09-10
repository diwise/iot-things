package things

// T3.0 kontraktslåsning: serialiserad JSON per saktyp är kontrakt mot API
// och storage. Golden-filerna i testdata fångar fältnamn och utdata så att
// T3 (borttaget Byte, tunnare kontrakt, en struct per sak) inte ändrar dem.
//
// Uppdatera golden med: go test ./internal/application/things/ -run TestThingJSONContract -update

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/matryer/is"
)

var update = flag.Bool("update", false, "update golden files")

var contractInputs = map[string]string{
	"building":        `{"id":"building-1","type":"Building","tenant":"default","location":{"latitude":62,"longitude":17},"energy":1.5,"power":2.5,"temperature":{"v":21.5,"timestamp":"2024-01-01T00:00:00Z"}}`,
	"container":       `{"id":"container-1","type":"Container","subType":"WasteContainer","tenant":"default","location":{"latitude":62,"longitude":17},"currentLevel":0.49,"percent":17.5}`,
	"lifebuoy":        `{"id":"lifebuoy-1","type":"Lifebuoy","tenant":"default","presence":true}`,
	"passage":         `{"id":"passage-1","type":"Passage","tenant":"default","cumulatedNumberOfPassages":5,"passagesToday":2,"currentState":true}`,
	"pointofinterest": `{"id":"poi-1","type":"PointOfInterest","subType":"Beach","tenant":"default","temperature":{"v":21,"timestamp":"2024-01-01T00:00:00Z"},"current":{"v":1,"timestamp":"2024-01-01T00:00:00Z"}}`,
	"pumpingstation":  `{"id":"pump-1","type":"PumpingStation","tenant":"default","pumpingObserved":true,"pumpingCumulativeTime":0}`,
	"room":            `{"id":"room-1","type":"Room","tenant":"default","temperature":{"v":21,"timestamp":"2024-01-01T00:00:00Z"},"humidity":55,"illuminance":300,"co2":700}`,
	"sewer":           `{"id":"sewer-1","type":"Sewer","tenant":"default","currentLevel":1.2,"percent":30,"lastAction":"overflow unknown"}`,
	"watermeter":      `{"id":"watermeter-1","type":"Watermeter","tenant":"default","cumulativeVolume":10.5,"leakage":false,"burst":false,"backflow":false,"fraud":false}`,
	"desk":            `{"id":"desk-1","type":"Desk","tenant":"default","presence":false}`,
	"sink":            `{"id":"sink-1","type":"Sink","tenant":"default","on":false,"cumulativeTime":0}`,
}

// TestThingJSONContract serialiserar varje saktyp precis som storage och
// thing.updated gör (json.Marshal på det konkreta värdet) och jämför mot
// golden. Provet använder json.Marshal direkt, inte Byte(), så det är
// stabilt genom T3 där Byte tas bort.
func TestThingJSONContract(t *testing.T) {
	is := is.New(t)

	for name, in := range contractInputs {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)

			thing, err := ConvToThing([]byte(in))
			is.NoErr(err)

			got, err := json.Marshal(thing)
			is.NoErr(err)

			path := filepath.Join("testdata", "contract-"+name+".json")
			if *update {
				is.NoErr(os.WriteFile(path, got, 0o644))
				return
			}

			want, err := os.ReadFile(path)
			is.NoErr(err)
			is.Equal(string(got), string(want))
		})
	}
}
