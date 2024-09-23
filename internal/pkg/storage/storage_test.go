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

func TestDeleteRelatedThing(t *testing.T) {
	is := is.New(t)
	db, ctx, cancel, err := new()
	defer cancel()

	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}

	wasteContainerId := getUuid()
	wasteContainer := createEnity(wasteContainerId, "WasteContainer")
	err = db.CreateThing(ctx, wasteContainer)
	is.NoErr(err)

	thingID := fmt.Sprintf("urn:diwise:%s:%s", "WasteContainer", wasteContainerId)

	deviceId := getUuid()
	device := createEnity(deviceId, "Device")
	err = db.AddRelatedThing(ctx, device, WithThingID(thingID))
	is.NoErr(err)

	relatedID := fmt.Sprintf("urn:diwise:%s:%s", "Device", deviceId)

	err = db.DeleteRelatedThing(ctx, thingID, relatedID, WithTenants([]string{"default"}))
	is.NoErr(err)
}

func TestWhere(t *testing.T) {
	is := is.New(t)

	c := &Condition{}
	WithThingID("id")(c)
	WithType([]string{"type"})(c)
	WithTenants([]string{"default", "test"})(c)

	w := c.Where()
	args := c.NamedArgs()

	is.Equal("where thing_id=@thing_id and type=@type and tenant=any(@tenant)", strings.Trim(w, " "))
	is.Equal("type", args["type"].(string))
	is.Equal("default", args["tenant"].([]string)[0])
	is.Equal("test", args["tenant"].([]string)[1])
}

func TestWhereTags(t *testing.T) {
	is := is.New(t)

	c := &Condition{}
	WithThingID("id")(c)
	WithType([]string{"type"})(c)
	WithTenants([]string{"default", "test"})(c)
	WithTags([]string{"tag1", "tag2"})(c)

	w := c.Where()
	args := c.NamedArgs()

	is.Equal("where thing_id=@thing_id and type=@type and tenant=any(@tenant) and data ? 'tags' and data->'tags' @> (@tags)", strings.Trim(w, " "))
	is.Equal("type", args["type"].(string))
	is.Equal("default", args["tenant"].([]string)[0])
	is.Equal("test", args["tenant"].([]string)[1])
}

func TestQueryWithTags(t *testing.T) {
	is := is.New(t)
	db, ctx, cancel, err := new()
	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}
	defer cancel()

	id := uuid.NewString()
	e := thing{
		ID:      id,
		Type:    "WasteContainer",
		Tenant:  "tenant",
		ThingID: fmt.Sprintf("urn:diwise:WasteContainer:%s", id),
		Location: location{
			Latitude:  17.2,
			Longitude: 64.3,
		},
		Tags: []string{"tag3", "tag1", "tag2", id},
	}

	b, _ := json.Marshal(e)
	err = db.CreateThing(ctx, b)
	is.NoErr(err)

	res, err := db.QueryThings(ctx, WithTags([]string{id}), WithTenants([]string{"tenant"}))
	is.NoErr(err)

	is.Equal(int64(1), res.TotalCount)

	res, err = db.QueryThings(ctx, WithTags([]string{"tag1", id}), WithTenants([]string{"tenant"}))
	is.NoErr(err)

	is.Equal(int64(1), res.TotalCount)

	res, err = db.QueryThings(ctx, WithTags([]string{"tag1"}), WithTenants([]string{"tenant"}))
	is.NoErr(err)

	is.True(res.TotalCount > 1)
}

func TestTags(t *testing.T) {
	is := is.New(t)
	db, ctx, cancel, err := new()
	if err != nil {
		t.Log("could not connect to database or create tables, will skip test")
		t.SkipNow()
	}
	defer cancel()
	id := uuid.NewString()
	e := thing{
		ID:      id,
		Type:    "WasteContainer",
		Tenant:  "tenant",
		ThingID: fmt.Sprintf("urn:diwise:WasteContainer:%s", id),
		Location: location{
			Latitude:  17.2,
			Longitude: 64.3,
		},
		Tags: []string{"tag3", "tag1", "tag2"},
	}

	b, _ := json.Marshal(e)
	err = db.CreateThing(ctx, b)
	is.NoErr(err)

	_, err = db.GetTags(ctx, []string{"tenant"})
	is.NoErr(err)

	// is.Equal("tag1,tag2,tag3", strings.Join(tags, ","))
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
