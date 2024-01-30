package storage

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matryer/is"
)

func new() (Db, context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

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

func TestConnectAndInitialize(t *testing.T) {
	_, _, cancel, err := new()
	defer cancel()

	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}
}

func TestCreateEntity(t *testing.T) {
	is := is.New(t)

	db, ctx, cancel, err := new()
	defer cancel()

	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}

	id := getUuid()

	err = db.CreateEntity(ctx, createEnity(id, "WasteContainer"))
	is.NoErr(err)
}

func TestUpdateEntity(t *testing.T) {
	is := is.New(t)

	db, ctx, cancel, err := new()
	defer cancel()

	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}

	id := getUuid()

	err = db.CreateEntity(ctx, createEnity(id, "WasteContainer"))
	is.NoErr(err)

	err = db.UpdateEntity(ctx, createEnity(id, "WasteContainer"))
	is.NoErr(err)
}

func TestQueryEntities(t *testing.T) {
	is := is.New(t)

	db, ctx, cancel, err := new()
	defer cancel()

	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}

	id := getUuid()
	wasteContainer := createEnity(id, "WasteContainer")

	err = db.CreateEntity(ctx, wasteContainer)
	is.NoErr(err)

	q := make([]ConditionFunc, 0)
	q = append(q, EntityID(id))
	q = append(q, EntityType("WasteContainer"))

	e, err := db.QueryEntities(ctx, q...)
	is.NoErr(err)

	entities := make([]entity, 0)
	json.Unmarshal(e, &entities)

	is.Equal(1, len(entities))
	is.Equal(id, entities[0].Id)
	is.Equal(float64(17.2), entities[0].Location.Latitude)
	is.Equal(float64(64.3), entities[0].Location.Longitude)
}

func TestRetrieveEntity(t *testing.T) {
	is := is.New(t)

	db, ctx, cancel, err := new()
	defer cancel()

	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}

	id := getUuid()

	err = db.CreateEntity(ctx, createEnity(id, "WasteContainer"))
	is.NoErr(err)

	b, et, err := db.RetrieveEntity(ctx, id)
	is.NoErr(err)
	is.Equal("WasteContainer", et)
	var e entity
	json.Unmarshal(b, &e)
	is.Equal(id, e.Id)
}

func TestAddRelatedEntity(t *testing.T) {
	is := is.New(t)
	db, ctx, cancel, err := new()
	defer cancel()

	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}

	wasteContainerId := getUuid()

	err = db.CreateEntity(ctx, createEnity(wasteContainerId, "WasteContainer"))
	is.NoErr(err)

	deviceId := getUuid()

	err = db.AddRelatedEntity(ctx, wasteContainerId, createEnity(deviceId, "Device"))
	is.NoErr(err)
}

func createEnity(args ...string) []byte {
	type_ := "WasteContainer"
	if len(args) > 1 {
		type_ = args[1]
	}

	e := entity{
		Id:   args[0],
		Type: type_,
		Location: location{
			Latitude:  17.2,
			Longitude: 64.3,
		},
	}

	b, _ := json.Marshal(e)

	return b
}

func getUuid() string {
	id, _ := uuid.NewUUID()
	return id.String()
}
