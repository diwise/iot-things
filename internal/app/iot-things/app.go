package iotthings

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/diwise/iot-things/internal/app/iot-things/things"
)

//go:generate moq -rm -out app_mock.go . ThingsApp
type ThingsApp interface {
	AddThing(ctx context.Context, b []byte) error
	SaveThing(ctx context.Context, t things.Thing) error
	UpdateThing(ctx context.Context, b []byte, tenants []string) error
	MergeThing(ctx context.Context, thingID string, b []byte, tenants []string) error
	GetConnectedThings(ctx context.Context, deviceID string) ([]things.Thing, error)
	QueryThings(ctx context.Context, params map[string][]string) (QueryResult, error)
	GetTags(ctx context.Context, tenants []string) ([]string, error)
	GetTypes(ctx context.Context, tenants []string) ([]string, error)
	Seed(ctx context.Context, r io.Reader) error

	AddMeasurement(ctx context.Context, t things.Thing, m things.Measurement) error
}

//go:generate moq -rm -out reader_mock.go . ThingsReader
type ThingsReader interface {
	QueryThings(ctx context.Context, conditions ...ConditionFunc) (QueryResult, error)
	GetTags(ctx context.Context, tenants []string) ([]string, error)
}

//go:generate moq -rm -out writer_mock.go . ThingsWriter
type ThingsWriter interface {
	AddThing(ctx context.Context, t things.Thing) error
	UpdateThing(ctx context.Context, t things.Thing) error
	AddMeasurement(ctx context.Context, t things.Thing, m things.Measurement) error
}

var ErrThingNotFound = errors.New("thing not found")
var ErrAlreadyExists = errors.New("thing already exists")

type app struct {
	reader ThingsReader
	writer ThingsWriter
}

func New(r ThingsReader, w ThingsWriter) ThingsApp {
	return &app{
		reader: r,
		writer: w,
	}
}

func (a *app) AddThing(ctx context.Context, b []byte) error {
	t, err := convToThing(b)
	if err != nil {
		return err
	}

	if t.ID() == "" {
		return errors.New("thing ID must be provided")
	}
	if t.Tenant() == "" {
		return errors.New("tenant must be provided")
	}
	if t.Type() == "" {
		return errors.New("thing type must be provided")
	}

	err = a.writer.AddThing(ctx, t)
	if err != nil {
		return err
	}

	return nil
}

func (a *app) UpdateThing(ctx context.Context, b []byte, tenants []string) error {
	if len(tenants) == 0 {
		return errors.New("tenants must be provided")
	}

	t, err := convToThing(b)
	if err != nil {
		return err
	}

	if t.ID() == "" {
		return errors.New("thing ID must be provided")
	}
	if t.Tenant() == "" {
		return errors.New("tenant must be provided")
	}
	if t.Type() == "" {
		return errors.New("thing type must be provided")
	}

	result, err := a.reader.QueryThings(ctx, WithID(t.ID()), WithTenants(tenants))
	if err != nil {
		return err
	}
	if len(result.Things) != 1 {
		return ErrThingNotFound
	}

	err = a.writer.UpdateThing(ctx, t)
	if err != nil {
		return err
	}

	return nil
}

func (a *app) SaveThing(ctx context.Context, t things.Thing) error {
	if t.ID() == "" {
		return errors.New("thing ID must be provided")
	}
	if t.Tenant() == "" {
		return errors.New("tenant must be provided")
	}
	if t.Type() == "" {
		return errors.New("thing type must be provided")
	}

	err := a.writer.UpdateThing(ctx, t)
	if err != nil {
		return err
	}

	return nil
}

func (a *app) MergeThing(ctx context.Context, thingID string, b []byte, tenants []string) error {
	if len(tenants) == 0 {
		return errors.New("tenants must be provided")
	}

	patch := make(map[string]any)
	err := json.Unmarshal(b, &patch)
	if err != nil {
		return err
	}

	result, err := a.reader.QueryThings(ctx, WithID(thingID), WithTenants(tenants))
	if err != nil {
		return err
	}
	if len(result.Things) != 1 {
		return ErrThingNotFound
	}

	current := make(map[string]any)
	err = json.Unmarshal(result.Things[0], &current)
	if err != nil {
		return err
	}

	for k, v := range patch {
		if slices.Contains([]string{"id", "type", "tenant"}, k) {
			continue
		}
		current[k] = v
	}

	v, err := json.Marshal(current)
	if err != nil {
		return err
	}

	patchedThing, err := convToThing(v)
	if err != nil {
		return err
	}

	err = a.writer.UpdateThing(ctx, patchedThing)
	if err != nil {
		return err
	}

	return nil
}

func (a *app) QueryThings(ctx context.Context, params map[string][]string) (QueryResult, error) {
	result, err := a.reader.QueryThings(ctx, WithParams(params)...)
	if err != nil {
		return QueryResult{}, err
	}
	return result, nil
}

func (a *app) getThingByID(ctx context.Context, thingID string) things.Thing {
	result, err := a.reader.QueryThings(ctx, WithID(thingID))
	if err != nil {
		return nil
	}
	if len(result.Things) != 1 {
		return nil
	}

	t, err := convToThing(result.Things[0])
	if err != nil {
		return nil
	}

	return t
}

func (a *app) GetConnectedThings(ctx context.Context, deviceID string) ([]things.Thing, error) {
	result, err := a.reader.QueryThings(ctx, WithRefDevice(deviceID))
	if err != nil {
		return nil, err
	}

	things := make([]things.Thing, 0)

	for _, b := range result.Things {
		t, err := convToThing(b)
		if err != nil {
			return nil, err
		}

		things = append(things, t)
	}

	return things, nil
}

func (a *app) GetTags(ctx context.Context, tenants []string) ([]string, error) {
	return a.reader.GetTags(ctx, tenants)
}

func (a *app) GetTypes(ctx context.Context, tenants []string) ([]string, error) {
	return []string{
		"Container",
		"PumpingStation",
		"Room",
		"Sewer",
		"Passage",
		"Lifebuoy",
		"WaterMeter",
	}, nil
}

func (a *app) AddMeasurement(ctx context.Context, t things.Thing, m things.Measurement) error {
	if m.ID == "" {
		return errors.New("measurement ID must be provided")
	}
	if m.Timestamp.IsZero() {
		return errors.New("timestamp must be provided")
	}
	if m.Value == nil && m.StringValue == nil && m.BoolValue == nil {
		return errors.New("value must be provided")
	}
	if m.Urn == "" {
		return errors.New("URN must be provided")
	}

	return a.writer.AddMeasurement(ctx, t, m)
}

func (a *app) Seed(ctx context.Context, r io.Reader) error {
	f := csv.NewReader(r)
	f.Comma = ';'
	rowNum := 0

	location := func(s string) things.Location {
		parts := strings.Split(s, ",")
		if len(parts) != 2 {
			return things.Location{}
		}

		parse := func(s string) float64 {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return 0.0
			}
			return f
		}

		return things.Location{
			Latitude:  parse(parts[0]),
			Longitude: parse(parts[1]),
		}
	}

	tags := func(t string) []string {
		if t == "" {
			return []string{}
		}
		if !strings.Contains(t, ",") {
			return []string{t}
		}
		tags := strings.Split(t, ",")
		return tags
	}

	refDevices := func(t string) []things.Device {
		if t == "" {
			return nil
		}
		if !strings.Contains(t, ",") {
			return []things.Device{{DeviceID: t}}
		}
		devices := []things.Device{}
		for _, s := range strings.Split(t, ",") {
			devices = append(devices, things.Device{DeviceID: s})
		}
		return devices
	}

	args := func(t string) map[string]any {
		m := make(map[string]any)
		if t == "" {
			return nil
		}
		t = strings.ReplaceAll(t, "'", "\"")
		err := json.Unmarshal([]byte(t), &m)
		if err != nil {
			return nil
		}
		return m
	}

	tenants := []string{"default"}

	for {
		record, err := f.Read()
		if err == io.EOF {
			break
		}

		if rowNum == 0 {
			rowNum++
			continue
		}

		//  0	 1      2      3         4           5      6        7      8          9
		// id, type, subType, name, decsription, location, tenant, tags, refDevices, args

		id_ := record[0]
		type_ := record[1]
		subType_ := record[2]
		name_ := record[3]
		description_ := record[4]
		location_ := location(record[5])
		tenant_ := record[6]
		tags_ := tags(record[7])
		refDevices_ := refDevices(record[8])

		m := make(map[string]any)

		current := a.getThingByID(ctx, id_)
		if current != nil {
			err := json.Unmarshal(current.Byte(), &m)
			if err != nil {
				return err
			}
		} else {
			m["id"] = id_
			m["type"] = type_
		}

		if subType_ != "" {
			m["sub_type"] = subType_
		} else {
			delete(m, "sub_type")
		}

		m["name"] = name_
		m["description"] = description_
		m["location"] = location_
		m["tenant"] = tenant_
		
		if len(tags_) > 0 {
			m["tags"] = tags_
		} else {
			delete(m, "tags")
		}
		
		if len(refDevices_) > 0 {
			m["ref_devices"] = refDevices_
		} else {
			delete(m, "ref_devices")
		}

		for k, v := range args(record[9]) {
			m[k] = v
		}

		b, err := json.Marshal(m)
		if err != nil {
			return err
		}

		if !slices.Contains(tenants, tenant_) {
			tenants = append(tenants, tenant_)
		}

		err = a.UpdateThing(ctx, b, tenants)
		if err != nil {
			return err
		}
	}

	return nil
}

func convToThing(b []byte) (things.Thing, error) {
	t := struct {
		Type string `json:"type"`
	}{}
	err := json.Unmarshal(b, &t)
	if err != nil {
		return nil, err
	}

	switch t.Type {
	case "Container":
		c, err := unmarshal[things.Container](b)
		return &c, err
	case "PumpingStation":
		ps, err := unmarshal[things.PumpingStation](b)
		return &ps, err
	case "Room":
		r, err := unmarshal[things.Room](b)
		return &r, err
	case "Sewer":
		s, err := unmarshal[things.Sewer](b)
		return &s, err
	case "Passage":
		p, err := unmarshal[things.Passage](b)
		return &p, err
	case "Lifebuoy":
		l, err := unmarshal[things.Lifebuoy](b)
		return &l, err
	case "WaterMeter":
		l, err := unmarshal[things.WaterMeter](b)
		return &l, err
	default:
		return nil, errors.New("unknown thing type")
	}
}

func unmarshal[T any](b []byte) (T, error) {
	var m T
	err := json.Unmarshal(b, &m)
	if err != nil {
		return m, err
	}
	return m, nil
}
