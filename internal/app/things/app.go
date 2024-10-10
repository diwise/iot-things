package things

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"
)

//go:generate moq -rm -out app_mock.go . ThingsApp
type ThingsApp interface {
	AddThing(ctx context.Context, b []byte) error
	UpdateThing(ctx context.Context, b []byte, tenants []string) error
	MergeThing(ctx context.Context, thingID string, b []byte, tenants []string) error
	GetConnectedThings(ctx context.Context, deviceID string) ([]Thing, error)
	QueryThings(ctx context.Context, params map[string][]string) (QueryResult, error)
	GetTags(ctx context.Context, tenants []string) ([]string, error)
	GetTypes(ctx context.Context, tenants []string) ([]string, error)
	Seed(ctx context.Context, r io.Reader) error

	AddMeasurement(ctx context.Context, t Thing, m Measurement) error
}

//go:generate moq -rm -out reader_mock.go . ThingsReader
type ThingsReader interface {
	QueryThings(ctx context.Context, conditions ...ConditionFunc) (QueryResult, error)
	GetTags(ctx context.Context, tenants []string) ([]string, error)
}

//go:generate moq -rm -out writer_mock.go . ThingsWriter
type ThingsWriter interface {
	AddThing(ctx context.Context, t Thing) error
	UpdateThing(ctx context.Context, t Thing) error
	AddMeasurement(ctx context.Context, t Thing, m Measurement) error
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

func (a *app) getThingByID(ctx context.Context, thingID string) Thing {
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

func (a *app) GetConnectedThings(ctx context.Context, deviceID string) ([]Thing, error) {
	result, err := a.reader.QueryThings(ctx, WithRefDevice(deviceID))
	if err != nil {
		return nil, err
	}

	things := make([]Thing, 0)

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

func (a *app) AddMeasurement(ctx context.Context, t Thing, m Measurement) error {
	return a.writer.AddMeasurement(ctx, t, m)
}

func (a *app) Seed(ctx context.Context, r io.Reader) error {
	f := csv.NewReader(r)
	f.Comma = ';'
	rowNum := 0

	location := func(s string) Location {
		parts := strings.Split(s, ",")
		if len(parts) != 2 {
			return Location{}
		}

		parse := func(s string) float64 {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return 0.0
			}
			return f
		}

		return Location{
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

	refDevices := func(t string) []Device {
		if t == "" {
			return nil
		}
		if !strings.Contains(t, ",") {
			return []Device{{DeviceID: t}}
		}
		devices := []Device{}
		for _, s := range strings.Split(t, ",") {
			devices = append(devices, Device{DeviceID: s})
		}
		return devices
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

		//  0	 1      2      3         4           5      6        7      8
		// id, type, subType, name, decsription, location, tenant, tags, refDevices

		id_ := record[0]
		type_ := record[1]
		subType_ := record[2]
		name_ := record[3]
		description_ := record[4]
		location_ := location(record[5])
		tenant_ := record[6]
		tags_ := tags(record[7])
		refDevices_ := refDevices(record[8])

		current := a.getThingByID(ctx, id_)
		if current != nil {
			m := make(map[string]any)
			err := json.Unmarshal(current.Byte(), &m)
			if err != nil {
				return err
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
			m["tags"] = tags_
			m["ref_devices"] = refDevices_

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

			continue
		}

		t := &thing{
			ID_:      id_,
			Type_:    type_,
			Location: location_,
			Tenant_:  tenant_,
		}
		t.Name = name_
		t.Description = description_
		if subType_ != "" {
			t.SubType = &subType_
		}
		t.Tags = tags_
		t.RefDevices = refDevices_

		err = a.writer.AddThing(ctx, t)
		if err != nil {
			return err
		}
	}

	return nil
}

func convToThing(b []byte) (Thing, error) {
	t := struct {
		Type string `json:"type"`
	}{}
	err := json.Unmarshal(b, &t)
	if err != nil {
		return nil, err
	}

	switch t.Type {
	case "Container":
		c, err := unmarshal[Container](b)
		return &c, err
	case "PumpingStation":
		ps, err := unmarshal[PumpingStation](b)
		return &ps, err
	case "Room":
		r, err := unmarshal[Room](b)
		return &r, err
	case "Sewer":
		s, err := unmarshal[Sewer](b)
		return &s, err
	case "Passage":
		p, err := unmarshal[Passage](b)
		return &p, err
	case "Lifebuoy":
		l, err := unmarshal[Lifebuoy](b)
		return &l, err
	case "WaterMeter":
		l, err := unmarshal[WaterMeter](b)
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
