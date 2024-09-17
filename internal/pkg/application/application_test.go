package application

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/diwise/iot-things/internal/pkg/presentation/auth"
	"github.com/diwise/iot-things/internal/pkg/storage"
	"github.com/matryer/is"
)

func TestSeed(t *testing.T) {
	is := is.New(t)

	reader := &ThingReaderMock{
		RetrieveThingFunc: func(ctx context.Context, conditions ...storage.ConditionFunc) ([]byte, string, error) {
			thing := Thing{
				ThingID: "urn:diwise:WasteContainer:52e0a125-01f6-4300-ac97-37bd911c2b28",
				Type:    "WasteContainer",
				Location: Location{
					Latitude:  62.390715,
					Longitude: 17.306868,
				},
				Id:     "52e0a125-01f6-4300-ac97-37bd911c2b28",
				Tenant: "default",
				Tags:   []string{"tag1", "tag2"},
			}

			b, _ := json.Marshal(thing)

			return b, "", nil
		},
	}

	writer := &ThingWriterMock{
		CreateThingFunc: func(ctx context.Context, v []byte) error {
			return nil
		},
		UpdateThingFunc: func(ctx context.Context, v []byte) error {
			return nil
		},
		AddRelatedThingFunc: func(ctx context.Context, v []byte, conditions ...storage.ConditionFunc) error {
			return nil
		},
	}

	app := New(reader, writer)
	ctx := context.Background()

	err := app.Seed(ctx, bytes.NewReader([]byte(csvData)))
	is.NoErr(err)
}

func TestSeedIntegrationTest(t *testing.T) {
	is := is.New(t)

	db, ctx, cancel, err := setupIntegrationTest()
	defer cancel()

	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}

	app := New(db, db)
	err = app.Seed(ctx, bytes.NewReader([]byte(things)))
	is.NoErr(err)

	b, err := app.RetrieveThing(ctx, "urn:diwise:WasteContainer:35", map[string][]string{})
	is.NoErr(err)

	m := make(map[string]any)
	err = json.Unmarshal(b, &m)
	is.NoErr(err)

	cap, ok := m["capacity"]
	is.True(ok)
	is.Equal(float64(160), cap.(float64))
}

func setupIntegrationTest() (storage.Db, context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	ctx = auth.WithAllowedTenants(ctx, []string{"default"})

	db, err := storage.New(ctx, storage.NewConfig(
		"localhost",
		"postgres",
		"password",
		"5432",
		"postgres",
		"disable",
	))

	return db, ctx, cancel, err
}

const csvData = `thingId;thingType;location;props;relatedId;relatedType;location;tenant;tags
52e0a125-01f6-4300-ac97-37bd911c2b28;WasteContainer;62.390715,,17.306868;int=1,str='str',float=3.0;a2c1821b-03b0-42cf-bcf2-3f9c0a38d227;Function;62.390715,,17.306868;default;tag1,tag2
c91149a8-256b-4d65-8ca8-fc00074485c8;WasteContainer;62.390715,,17.306868;;ebc1747e-c20e-426d-b1d3-24a01ac85428;Function;;default;
d74a9652-afe6-46ea-8bf1-f4e11d5e02c6;WasteContainer;62.390715,,17.306868;;;;;default;`

const things = `thingID;thingType;location;props;relatedId;relatedType;location;tenant;tags
35;WasteContainer;62.39040978,17.30775705;capacity=160;level:35;level;62.39040978,17.30775705;default;soptunna,miljörum
36;WasteContainer;62.39080222,17.31036154;capacity=160;level:36;level;62.39080222,17.31036154;default;hundlatrin
49;WasteContainer;62.27082193,17.37153332;capacity=60;level:49;level;62.27082193,17.37153332;default;
49;WasteContainer;62.27082193,17.37153332;capacity=60;level:49;level;62.27082193,17.37153332;default;test`
