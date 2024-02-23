package application

import (
	"bytes"
	"context"
	"testing"

	"github.com/matryer/is"
)

func TestSeed(t *testing.T) {
	is := is.New(t)

	reader := &ThingReaderMock{
		RetrieveThingFunc: func(ctx context.Context, thingId string) ([]byte, string, error) {
			return nil, "", nil
		},
	}

	writer := &ThingWriterMock{
		CreateThingFunc: func(ctx context.Context, v []byte) error {
			return nil
		},
		UpdateThingFunc: func(ctx context.Context, v []byte) error {
			return nil
		},
		AddRelatedThingFunc: func(ctx context.Context, thingId string, v []byte) error {
			return nil
		},
	}

	app := New(reader, writer)
	ctx := context.Background()

	err := app.Seed(ctx, bytes.NewReader([]byte(csvData)))
	is.NoErr(err)
}

const csvData = `thingId;thingType;location;relatedId;relatedType;location;tenant;
52e0a125-01f6-4300-ac97-37bd911c2b28;WasteContainer;62.390715,17.306868;a2c1821b-03b0-42cf-bcf2-3f9c0a38d227;Function;0.0,0.0;default;
c91149a8-256b-4d65-8ca8-fc00074485c8;WasteContainer;62.390715,17.306868;ebc1747e-c20e-426d-b1d3-24a01ac85428;Function;;default;
d74a9652-afe6-46ea-8bf1-f4e11d5e02c6;WasteContainer;62.390715,17.306868;;;;default;`
