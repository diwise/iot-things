package iotthings

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/diwise/iot-things/internal/app/iot-things/things"
	"gopkg.in/yaml.v2"
)

//go:generate moq -rm -out app_mock.go . ThingsApp
type ThingsApp interface {
	AddThing(ctx context.Context, b []byte) error
	SaveThing(ctx context.Context, t things.Thing) error
	UpdateThing(ctx context.Context, b []byte, tenants []string) error
	MergeThing(ctx context.Context, thingID string, b []byte, tenants []string) error
	DeleteThing(ctx context.Context, thingID string, tenants []string) error
	GetConnectedThings(ctx context.Context, deviceID string) ([]things.Thing, error)
	QueryThings(ctx context.Context, params map[string][]string) (QueryResult, error)
	GetTags(ctx context.Context, tenants []string) ([]string, error)
	GetTypes(ctx context.Context, tenants []string) ([]things.ThingType, error)
	Seed(ctx context.Context, r io.Reader) error

	LoadConfig(ctx context.Context, r io.Reader) error

	AddValue(ctx context.Context, t things.Thing, m things.Value) error
	QueryValues(ctx context.Context, params map[string][]string) (QueryResult, error)
}

//go:generate moq -rm -out reader_mock.go . ThingsReader
type ThingsReader interface {
	QueryThings(ctx context.Context, conditions ...ConditionFunc) (QueryResult, error)
	QueryValues(ctx context.Context, conditions ...ConditionFunc) (QueryResult, error)
	GetTags(ctx context.Context, tenants []string) ([]string, error)
}

//go:generate moq -rm -out writer_mock.go . ThingsWriter
type ThingsWriter interface {
	AddThing(ctx context.Context, t things.Thing) error
	UpdateThing(ctx context.Context, t things.Thing) error
	DeleteThing(ctx context.Context, thingID string) error
	AddValue(ctx context.Context, t things.Thing, m things.Value) error
}

var ErrThingNotFound = errors.New("thing not found")
var ErrAlreadyExists = errors.New("thing already exists")

type app struct {
	reader ThingsReader
	writer ThingsWriter
	cfg    *config
}

type config struct {
	Types []typeConfig `json:"types" yaml:"types"`
}

type typeConfig struct {
	Type     string   `json:"type" yaml:"type"`
	SubTypes []string `json:"subTypes" yaml:"subTypes"`
}

func New(r ThingsReader, w ThingsWriter) ThingsApp {
	return &app{
		reader: r,
		writer: w,
	}
}

func (a *app) LoadConfig(ctx context.Context, r io.Reader) error {
	c := config{}
	err := yaml.NewDecoder(r).Decode(&c)
	if err != nil {
		return err
	}

	a.cfg = &c

	return nil
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
	if len(result.Data) != 1 {
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
	if len(result.Data) != 1 {
		return ErrThingNotFound
	}

	current := make(map[string]any)
	err = json.Unmarshal(result.Data[0], &current)
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

func (a *app) DeleteThing(ctx context.Context, thingID string, tenants []string) error {
	if len(tenants) == 0 {
		return errors.New("tenants must be provided")
	}

	result, err := a.reader.QueryThings(ctx, WithID(thingID), WithTenants(tenants))
	if err != nil {
		return err
	}
	if len(result.Data) != 1 {
		return ErrThingNotFound
	}

	err = a.writer.DeleteThing(ctx, thingID)
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

func (a *app) QueryValues(ctx context.Context, params map[string][]string) (QueryResult, error) {
	result, err := a.reader.QueryValues(ctx, WithParams(params)...)
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
	if len(result.Data) != 1 {
		return nil
	}

	t, err := convToThing(result.Data[0])
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

	for _, b := range result.Data {
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

func (a *app) AddValue(ctx context.Context, t things.Thing, m things.Value) error {
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

	return a.writer.AddValue(ctx, t, m)
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

		//  0	 1      2      3         4           5       6      7       8         9
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
			m["subType"] = subType_
		} else {
			delete(m, "subType")
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
			m["refDevices"] = refDevices_
		} else {
			delete(m, "refDevices")
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

		if current == nil {
			err = a.AddThing(ctx, b)
			if err != nil {
				return err
			}
		} else {
			err = a.UpdateThing(ctx, b, tenants)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *app) GetTypes(ctx context.Context, tenants []string) ([]things.ThingType, error) {
	types := make([]things.ThingType, 0)

	for _, t := range a.cfg.Types {
		types = append(types, things.ThingType{
			Type: t.Type,
			Name: t.Type,
		})

		for _, s := range t.SubTypes {
			types = append(types, things.ThingType{
				Type:    t.Type,
				SubType: s,
				Name:    fmt.Sprintf("%s:%s", t.Type, s),
			})
		}
	}

	return types, nil
}

func convToThing(b []byte) (things.Thing, error) {
	t := struct {
		Type string `json:"type"`
	}{}
	err := json.Unmarshal(b, &t)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(t.Type) {
	case "building":
		building, err := unmarshal[things.Building](b)
		building.ValidURN = things.BuildingURNs
		return &building, err
	case "container":
		c, err := unmarshal[things.Container](b)
		c.ValidURN = things.ContainerURNs
		return &c, err
	case "lifebuoy":
		l, err := unmarshal[things.Lifebuoy](b)
		l.ValidURN = things.LifebuoyURNs
		return &l, err
	case "passage":
		p, err := unmarshal[things.Passage](b)
		p.ValidURN = things.PassageURNs
		return &p, err
	case "pointofinterest":
		poi, err := unmarshal[things.PointOfInterest](b)
		poi.ValidURN = things.PointOfInterestURNs
		return &poi, err
	case "pumpingstation":
		ps, err := unmarshal[things.PumpingStation](b)
		ps.ValidURN = things.PumpingStationURNs
		return &ps, err
	case "room":
		r, err := unmarshal[things.Room](b)
		r.ValidURN = things.RoomURNs
		return &r, err
	case "sewer":
		s, err := unmarshal[things.Sewer](b)
		s.ValidURN = things.SewerURNs
		return &s, err
	case "watermeter":
		l, err := unmarshal[things.Watermeter](b)
		l.ValidURN = things.WaterMeterURNs
		return &l, err
	default:
		return nil, errors.New("unknown thing type [" + t.Type + "]")
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
