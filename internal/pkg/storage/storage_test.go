package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/diwise/iot-things/internal/pkg/presentation/auth"
	"github.com/google/uuid"
	"github.com/matryer/is"
)

func new() (Db, context.Context, context.CancelFunc, error) {
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

func TestConnectAndInitialize(t *testing.T) {
	_, _, cancel, err := new()
	defer cancel()

	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}
}

func TestCreateThing(t *testing.T) {
	is := is.New(t)

	db, ctx, cancel, err := new()
	defer cancel()

	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}

	id := getUuid()

	err = db.CreateThing(ctx, createEnity(id, "WasteContainer"))
	is.NoErr(err)
}

func TestUpdateThing(t *testing.T) {
	is := is.New(t)

	db, ctx, cancel, err := new()
	defer cancel()

	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}

	id := getUuid()

	err = db.CreateThing(ctx, createEnity(id, "WasteContainer"))
	is.NoErr(err)

	err = db.UpdateThing(ctx, createEnity(id, "WasteContainer"))
	is.NoErr(err)
}

func TestQueryThings(t *testing.T) {
	is := is.New(t)

	db, ctx, cancel, err := new()
	defer cancel()

	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}

	id := getUuid()
	wasteContainer := createEnity(id, "WasteContainer")

	err = db.CreateThing(ctx, wasteContainer)
	is.NoErr(err)

	q := make([]ConditionFunc, 0)
	q = append(q, WithThingID(fmt.Sprintf("urn:diwise:%s:%s", "WasteContainer", id)))

	result, err := db.QueryThings(ctx, q...)
	is.NoErr(err)

	things := make([]thing, 0)
	json.Unmarshal(result.Things, &things)

	is.Equal(1, len(things))
	is.Equal(id, things[0].ID)
	is.Equal(float64(17.2), things[0].Location.Latitude)
	is.Equal(float64(64.3), things[0].Location.Longitude)
}

func TestQueryThingsIDAndType(t *testing.T) {
	is := is.New(t)

	db, ctx, cancel, err := new()
	defer cancel()

	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}

	id := getUuid()
	wasteContainer := createEnity(id, "WasteContainer")

	err = db.CreateThing(ctx, wasteContainer)
	is.NoErr(err)

	q := make([]ConditionFunc, 0)
	q = append(q, WithID(id))
	q = append(q, WithType([]string{"WasteContainer"}))

	result, err := db.QueryThings(ctx, q...)
	is.NoErr(err)

	things := make([]thing, 0)
	json.Unmarshal(result.Things, &things)

	is.Equal(1, len(things))
	is.Equal(id, things[0].ID)
	is.Equal(float64(17.2), things[0].Location.Latitude)
	is.Equal(float64(64.3), things[0].Location.Longitude)
}

func TestRetrieveThing(t *testing.T) {
	is := is.New(t)

	db, ctx, cancel, err := new()
	defer cancel()

	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}

	id := getUuid()

	err = db.CreateThing(ctx, createEnity(id, "WasteContainer"))
	is.NoErr(err)

	b, et, err := db.RetrieveThing(ctx, WithThingID(fmt.Sprintf("urn:diwise:%s:%s", "WasteContainer", id)))
	is.NoErr(err)
	is.Equal("wastecontainer", et)
	var e thing
	json.Unmarshal(b, &e)
	is.Equal(id, e.ID)
}

func TestAddRelatedThing(t *testing.T) {
	is := is.New(t)
	db, ctx, cancel, err := new()
	defer cancel()

	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}

	wasteContainerId := getUuid()

	err = db.CreateThing(ctx, createEnity(wasteContainerId, "WasteContainer"))
	is.NoErr(err)

	deviceId := getUuid()

	thingID := fmt.Sprintf("urn:diwise:%s:%s", "WasteContainer", wasteContainerId)

	err = db.AddRelatedThing(ctx, createEnity(deviceId, "Device"), WithThingID(thingID))
	is.NoErr(err)
}

func TestWhere(t *testing.T) {
	is := is.New(t)

	c := &Condition{}
	WithThingID("id")(c)
	WithType([]string{"type"})(c)
	WithTenants([]string{"default","test"})(c)

	w := c.Where()
	args := c.NamedArgs()

	is.Equal("where thing_id=@thing_id and type=@type and tenant=any(@tenant)", strings.Trim(w, " "))
	is.Equal("type", args["type"].(string))
	is.Equal("default", args["tenant"].([]string)[0])
	is.Equal("test", args["tenant"].([]string)[1])
}

func createEnity(args ...string) []byte {
	type_ := "WasteContainer"
	if len(args) > 1 {
		type_ = args[1]
	}

	e := thing{
		ID:   args[0],
		Type: type_,
		Location: location{
			Latitude:  17.2,
			Longitude: 64.3,
		},
		Tenant: "default",
	}

	b, _ := json.Marshal(e)

	return b
}

func getUuid() string {
	id, _ := uuid.NewUUID()
	return id.String()
}
