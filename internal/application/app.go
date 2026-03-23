package application

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/diwise/iot-things/internal/application/things"
	"github.com/diwise/iot-things/pkg/types"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"

	"gopkg.in/yaml.v2"
)

//go:generate moq -rm -out app_mock.go . ThingsApp
type ThingsApp interface {
	HandleMeasurements(ctx context.Context, measurements []things.Measurement)

	Add(ctx context.Context, b []byte) error
	Delete(ctx context.Context, thingID string, tenants []string) error
	Merge(ctx context.Context, thingID string, b []byte, tenants []string) error
	Query(ctx context.Context, query ThingQuery) (QueryResult, error)
	Update(ctx context.Context, b []byte, tenants []string) error

	AddValue(ctx context.Context, t things.Thing, m things.Value) error
	Values(ctx context.Context, query ValueQuery) (QueryResult, error)

	Tags(ctx context.Context, tenants []string) ([]string, error)
	Types(ctx context.Context, tenants []string) ([]things.ThingType, error)

	LoadConfig(ctx context.Context, r io.Reader) error
	Seed(ctx context.Context, r io.Reader) error
}

//go:generate moq -rm -out reader_mock.go . ThingsReader
type ThingsReader interface {
	QueryThings(ctx context.Context, query ThingQuery) (QueryResult, error)
	QueryValues(ctx context.Context, query ValueQuery) (QueryResult, error)
	GetTags(ctx context.Context, tenants []string) ([]string, error)
}

//go:generate moq -rm -out writer_mock.go . ThingsWriter
type ThingsWriter interface {
	AddThing(ctx context.Context, t things.Thing) error
	UpdateThing(ctx context.Context, t things.Thing) error
	DeleteThing(ctx context.Context, thingID string) error
	AddValue(ctx context.Context, t things.Thing, m things.Value) error
}

var (
	ErrThingNotFound      = errors.New("thing not found")
	ErrAlreadyExists      = errors.New("thing already exists")
	ErrMissingThingID     = errors.New("thing ID must be provided")
	ErrMissingThingTenant = errors.New("tenant must be provided")
	ErrMissingThingType   = errors.New("thing type must be provided")
)

type app struct {
	reader ThingsReader
	writer ThingsWriter
	cfg    *config

	pub chan string
	mu  sync.Mutex
}

type config struct {
	Types []typeConfig `json:"types" yaml:"types"`
}

type typeConfig struct {
	Type     string   `json:"type" yaml:"type"`
	SubTypes []string `json:"subTypes" yaml:"subTypes"`
}

func New(ctx context.Context, r ThingsReader, w ThingsWriter, msgCtx messaging.MsgContext) ThingsApp {
	a := &app{
		reader: r,
		writer: w,

		pub: make(chan string),
	}

	go publisher(ctx, a.reader, msgCtx, a.pub)

	return a
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

func (a *app) HandleMeasurements(ctx context.Context, measurements []things.Measurement) {
	a.mu.Lock()
	defer a.mu.Unlock()

	changedThings := []string{}

	for _, m := range measurements {
		changedThings = append(changedThings, a.handle(ctx, m)...)
	}

	if len(changedThings) > 0 {
		for _, thingID := range unique(changedThings) {
			a.pub <- thingID
		}
	}
}

func (a *app) handle(ctx context.Context, m things.Measurement) []string {
	log := logging.GetFromContext(ctx)

	connectedThings, err := a.getConnectedThings(ctx, m.DeviceID())
	if err != nil {
		log.Error("could not get connected things", "err", err.Error())
		return []string{}
	}

	if len(connectedThings) == 0 {
		log.Debug("no connected things found", "device_id", m.DeviceID())
		return []string{}
	}

	changedThings := []string{}

	for _, t := range connectedThings {
		measurements := []things.Measurement{m}

		ctx = logging.NewContextWithLogger(ctx, log, slog.String("device_id", m.DeviceID()), slog.String("thing_id", t.ID()))

		err := t.Handle(ctx, measurements, func(valueProvider things.ValueProvider) error {
			var errs []error

			values := valueProvider.Values()

			for _, v := range values {
				// add value to storage. A value is a measurement with the thingID instead of the deviceID
				errs = append(errs, a.AddValue(ctx, t, v))
			}

			return errors.Join(errs...)
		})
		if err != nil {
			log.Error("could not handle measurement", "err", err.Error())
			continue
		}

		// adds the current measurement to its (ref)device and ObservedAt if the timestamp is newer
		t.SetLastObserved(measurements)

		err = a.saveThing(ctx, t)
		if err != nil {
			log.Error("could not save thing", "err", err.Error())
			continue
		}

		changedThings = append(changedThings, t.ID())
	}

	if len(changedThings) == 0 {
		log.Debug("no changed things found", "device_id", m.DeviceID())
		return []string{}
	}

	return changedThings
}

func publisher(ctx context.Context, r ThingsReader, msgCtx messaging.MsgContext, inbox chan string) {
	log := logging.GetFromContext(ctx)

	for {
		select {
		case <-ctx.Done():
			return

		case thingID := <-inbox:
			result, err := r.QueryThings(ctx, ThingByIDQuery(thingID, nil))
			if err != nil {
				log.Error("could not query thing", "err", err.Error())
				continue
			}

			if len(result.Data) != 1 {
				log.Debug("thing not found", "thingID", thingID, slog.Int("count", len(result.Data)))
				continue
			}

			t, err := things.ConvToThing(result.Data[0])
			if err != nil {
				log.Error("could not convert thing", "err", err.Error())
				continue
			}

			msg := &types.ThingUpdated{ // for each updated connected thing, publish thing.updated
				ID:        t.ID(),
				Type:      t.Type(),
				Thing:     removeInternalState(t),
				Tenant:    t.Tenant(),
				Timestamp: time.Now().UTC(),
			}

			log.Debug("publish message", "content_type", msg.ContentType(), "thing_id", t.ID(), "tenant", t.Tenant(), "type", t.Type())

			err = msgCtx.PublishOnTopic(ctx, msg)
			if err != nil {
				log.Error("could not publish message", "err", err.Error())
				continue
			}
		}
	}
}

func (a *app) Add(ctx context.Context, b []byte) error {
	t, err := things.ConvToThing(b)
	if err != nil {
		return err
	}

	if t.ID() == "" {
		return ErrMissingThingID
	}
	if t.Tenant() == "" {
		return ErrMissingThingTenant
	}
	if t.Type() == "" {
		return ErrMissingThingType
	}

	err = a.writer.AddThing(ctx, t)
	if err != nil {
		return err
	}

	return nil
}

func (a *app) Update(ctx context.Context, b []byte, tenants []string) error {
	if len(tenants) == 0 {
		return errors.New("tenants must be provided")
	}

	t, err := things.ConvToThing(b)
	if err != nil {
		return err
	}

	if t.ID() == "" {
		return ErrMissingThingID
	}
	if t.Tenant() == "" {
		return ErrMissingThingTenant
	}
	if t.Type() == "" {
		return ErrMissingThingType
	}

	thingID := t.ID()
	result, err := a.reader.QueryThings(ctx, ThingByIDQuery(thingID, tenants))
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

func (a *app) saveThing(ctx context.Context, t things.Thing) error {
	if t.ID() == "" {
		return ErrMissingThingID
	}
	if t.Tenant() == "" {
		return ErrMissingThingTenant
	}
	if t.Type() == "" {
		return ErrMissingThingType
	}

	err := a.writer.UpdateThing(ctx, t)
	if err != nil {
		return err
	}

	return nil
}

func (a *app) Merge(ctx context.Context, thingID string, b []byte, tenants []string) error {
	if len(tenants) == 0 {
		return ErrMissingThingTenant
	}

	patch := make(map[string]any)
	err := json.Unmarshal(b, &patch)
	if err != nil {
		return err
	}

	result, err := a.reader.QueryThings(ctx, ThingByIDQuery(thingID, tenants))
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
		if slices.Contains([]string{"id", "type"}, k) {
			continue
		}
		current[k] = v
	}

	v, err := json.Marshal(current)
	if err != nil {
		return err
	}

	patchedThing, err := things.ConvToThing(v)
	if err != nil {
		return err
	}

	err = a.writer.UpdateThing(ctx, patchedThing)
	if err != nil {
		return err
	}

	return nil
}

func (a *app) Delete(ctx context.Context, thingID string, tenants []string) error {
	if len(tenants) == 0 {
		return ErrMissingThingTenant
	}

	result, err := a.reader.QueryThings(ctx, ThingByIDQuery(thingID, tenants))
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

func (a *app) Query(ctx context.Context, query ThingQuery) (QueryResult, error) {
	result, err := a.reader.QueryThings(ctx, query)
	if err != nil {
		return QueryResult{}, err
	}
	return result, nil
}

func (a *app) Values(ctx context.Context, query ValueQuery) (QueryResult, error) {
	result, err := a.reader.QueryValues(ctx, query)
	if err != nil {
		return QueryResult{}, err
	}
	return result, nil
}

func (a *app) getThingByID(ctx context.Context, thingID string) things.Thing {
	result, err := a.reader.QueryThings(ctx, ThingByIDQuery(thingID, nil))
	if err != nil {
		return nil
	}
	if len(result.Data) != 1 {
		return nil
	}

	t, err := things.ConvToThing(result.Data[0])
	if err != nil {
		return nil
	}

	return t
}

func (a *app) getConnectedThings(ctx context.Context, deviceID string) ([]things.Thing, error) {
	result, err := a.reader.QueryThings(ctx, ThingsByRefDeviceQuery(deviceID))
	if err != nil {
		return nil, err
	}

	tt := make([]things.Thing, 0)

	for _, b := range result.Data {
		t, err := things.ConvToThing(b)
		if err != nil {
			return nil, err
		}

		tt = append(tt, t)
	}

	return tt, nil
}

func (a *app) Tags(ctx context.Context, tenants []string) ([]string, error) {
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
		for s := range strings.SplitSeq(t, ",") {
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

		currentRow := rowNum + 1
		if err != nil {
			return fmt.Errorf("failed to read csv row %d: %w", currentRow, err)
		}

		rowNum = currentRow

		if rowNum == 1 {
			continue
		}

		if len(record) != 10 {
			return fmt.Errorf("invalid csv row %d: expected 10 columns, got %d", rowNum, len(record))
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

		maps.Copy(m, args(record[9]))

		b, err := json.Marshal(m)
		if err != nil {
			return err
		}

		if !slices.Contains(tenants, tenant_) {
			tenants = append(tenants, tenant_)
		}

		if current == nil {
			err = a.Add(ctx, b)
			if err != nil {
				return err
			}
		} else {
			err = a.Update(ctx, b, tenants)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *app) Types(ctx context.Context, tenants []string) ([]things.ThingType, error) {
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
				Name:    fmt.Sprintf("%s-%s", t.Type, s),
			})
		}
	}

	return types, nil
}
