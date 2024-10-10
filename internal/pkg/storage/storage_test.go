package storage

import (
	"context"
	"testing"
	"time"

	"github.com/diwise/iot-things/internal/app"
	"github.com/diwise/iot-things/internal/pkg/presentation/auth"
	"github.com/google/uuid"
)

func TestAddThing(t *testing.T) {
	db, ctx, cancel, err := new()
	defer cancel()

	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}
	uuid := uuid.NewString()
	thing := app.NewWasteContainer(uuid, app.Location{Latitude: 17.2, Longitude: 64.3}, "default")

	err = db.AddThing(ctx, thing)
	if err != nil {
		t.Error(err)
	}
}

func TestAddThingWithDevices(t *testing.T) {
	db, ctx, cancel, err := new()
	defer cancel()

	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}
	thingID := uuid.NewString()
	thing := app.NewWasteContainer(thingID, app.Location{Latitude: 17.2, Longitude: 64.3}, "default")

	thing.AddDevice(uuid.NewString())

	err = db.AddThing(ctx, thing)
	if err != nil {
		t.Error(err)
	}
}

func TestQueryThings(t *testing.T) {
	db, ctx, cancel, err := new()
	defer cancel()

	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}

	thingID := uuid.NewString()
	thing := app.NewWasteContainer(thingID, app.Location{Latitude: 17.2, Longitude: 64.3}, "default")

	deviceID := uuid.NewString()
	thing.AddDevice(deviceID)
	thing.AddTag("tag1")
	thing.AddTag("tag2")

	err = db.AddThing(ctx, thing)
	if err != nil {
		t.Error(err)
	}

	result, err := db.QueryThings(ctx, app.WithRefDevice(deviceID), app.WithTenants([]string{"default"}))
	if err != nil {
		t.Error(err)
	}
	if result.TotalCount != 1 {
		t.Errorf("no thing, or too many things, found")
	}

	result, err = db.QueryThings(ctx, app.WithID(thingID))
	if err != nil {
		t.Error(err)
	}
	if result.TotalCount != 1 {
		t.Errorf("no thing, or too many things, found")
	}

	result, err = db.QueryThings(ctx, app.WithTypes([]string{"Container"}))
	if err != nil {
		t.Error(err)
	}
	if result.TotalCount == 0 {
		t.Errorf("no thing, or too many things, found")
	}

	result, err = db.QueryThings(ctx, app.WithSubType("WasteContainer"))
	if err != nil {
		t.Error(err)
	}
	if result.TotalCount == 0 {
		t.Errorf("no thing, or too many things, found")
	}

	result, err = db.QueryThings(ctx, app.WithTags([]string{"tag1", "tag2"}), app.WithTenants([]string{"default"}))
	if err != nil {
		t.Error(err)
	}
	if result.TotalCount == 0 {
		t.Errorf("no thing, or too many things, found")
	}
}

func new() (Storage, context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	ctx = auth.WithAllowedTenants(ctx, []string{"default"})

	db, err := New(ctx, Config{
		host:     "localhost",
		user:     "postgres",
		password: "password",
		port:     "5432",
		dbname:   "postgres",
		sslmode:  "disable",
	})

	return db, ctx, cancel, err
}
