package application

import (
	"context"
	"testing"

	"github.com/matryer/is"
)

func TestSeed(t *testing.T) {
	is := is.New(t)

	storage := &StorageMock{
		RetrieveEntityFunc: func(ctx context.Context, entityId string) ([]byte, string, error) {
			return nil, "", nil
		},
		CreateEntityFunc: func(ctx context.Context, v []byte) error {
			return nil
		},
		UpdateEntityFunc: func(ctx context.Context, v []byte) error {
			return nil
		},
		AddRelatedEntityFunc: func(ctx context.Context, entityId string, v []byte) error {
			return nil
		},
	}

	app := New(storage)
	ctx := context.Background()

	err := app.Seed(ctx, []byte(csvData))
	is.NoErr(err)
}

const csvData = `entityId;entityType;location;deviceId;location;
52e0a125-01f6-4300-ac97-37bd911c2b28;WasteContainer;62.390715,17.306868;a2c1821b-03b0-42cf-bcf2-3f9c0a38d227;0.0,0.0;`
