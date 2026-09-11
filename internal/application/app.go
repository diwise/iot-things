package application

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"gopkg.in/yaml.v2"
)

type ThingsApp interface {
	HandleMeasurements(ctx context.Context, tenant string, messageID string, measurements []things.Measurement) error

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

	MigrateBindings(ctx context.Context) (int, error)
	HasUnmigratedThings(ctx context.Context) (bool, error)
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
	// ErrInvalidThingID skyddar värdeägarskapet: historik matchas på
	// thingID + "/", så ett thing-ID med "/" skulle kunna överlappa ett annat.
	ErrInvalidThingID = errors.New("thing ID must not contain '/'")
	// ErrInvalidBinding: bindningen måste ha en enhet och en ingång som
	// saktypen känner igen.
	ErrInvalidBinding = errors.New("invalid binding")
)

func validateBindings(t things.Thing) error {
	for _, b := range t.Bindings() {
		if b.DeviceID == "" {
			return fmt.Errorf("%w: missing deviceID", ErrInvalidBinding)
		}
		if !things.BindingValidForType(t.Type(), b) {
			return fmt.Errorf("%w: %s/%s/%s is not a valid signal for input %q on type %q",
				ErrInvalidBinding, b.DeviceID, b.Object, b.Resource, b.Input, t.Type())
		}
	}
	return nil
}

type app struct {
	reader ThingsReader
	writer ThingsWriter
	msgCtx messaging.MsgContext
	cfg    *config

	mu sync.Mutex
}

// thingsUpdatedCounter is created lazily once. It was previously created
// by the publisher goroutine; with direct publication there is no such
// goroutine.
var (
	thingsUpdatedCounter     metric.Int64Counter
	thingsUpdatedCounterOnce sync.Once
)

func updatedCounter() metric.Int64Counter {
	thingsUpdatedCounterOnce.Do(func() {
		c, err := otel.Meter("iot-things/measurements").Int64Counter(
			"diwise.things.updated",
			metric.WithUnit("1"),
			metric.WithDescription("Total number of updated things"),
		)
		if err == nil {
			thingsUpdatedCounter = c
		}
	})

	return thingsUpdatedCounter
}

type config struct {
	Types []typeConfig `json:"types" yaml:"types"`
}

type typeConfig struct {
	Type     string   `json:"type" yaml:"type"`
	SubTypes []string `json:"subTypes" yaml:"subTypes"`
}

func New(r ThingsReader, w ThingsWriter, msgCtx messaging.MsgContext) ThingsApp {
	return &app{
		reader: r,
		writer: w,
		msgCtx: msgCtx,
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

// HandleMeasurements behandlar alla mätningar i en rapport och publicerar
// thing.updated en gång per berörd sak. Saken hämtas en gång per enhet,
// applicerar hela rapportens mätningar på samma objekt, sparas och publiceras
// med sitt ackumulerade tillstånd. Ingen väntan mellan rapporter och ingen
// omläsning från lagring före publicering.
// HandleMeasurements behandlar hela rapporten per enhet. En enskild saks fel
// hindrar inte övriga saker, men felet propageras så att meddelandet kan
// återlevereras. Behandlingen är idempotent: historikvärden skrivs med
// ON CONFLICT DO NOTHING och sakens tillstånd är en upsert, så en retry
// konvergerar.
func (a *app) HandleMeasurements(ctx context.Context, tenant string, messageID string, measurements []things.Measurement) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(measurements) == 0 {
		return nil
	}

	baseLog := logging.GetFromContext(ctx)
	var errs []error

	// Gruppera per enhet. Ett pack är en enhet, men grupperingen gör
	// blandade batcher korrekta och håller sak-objektet återanvänt.
	groups := make(map[string][]things.Measurement)
	order := make([]string, 0)
	for _, m := range measurements {
		d := m.DeviceID()
		if _, ok := groups[d]; !ok {
			order = append(order, d)
		}
		groups[d] = append(groups[d], m)
	}

	for _, deviceID := range order {
		ms := groups[deviceID]

		// Tenant-isolering: saker hämtas och matchas bara inom rapportens
		// tenant. En koppling till en enhet i en annan tenant får aldrig
		// uppdatera saken.
		connectedThings, err := a.getConnectedThings(ctx, deviceID, []string{tenant})
		if err != nil {
			baseLog.Error("could not get connected things", "device_id", deviceID, "tenant", tenant, "err", err.Error())
			errs = append(errs, err)
			continue
		}

		for _, thing := range connectedThings {
			if thing.Tenant() != tenant {
				baseLog.Warn("skipping thing with mismatching tenant", "device_id", deviceID, "thing_id", thing.ID(), "thing_tenant", thing.Tenant(), "report_tenant", tenant)
				continue
			}

			thingCtx := logging.NewContextWithLogger(ctx, baseLog, "thing_id", thing.ID())
			log := logging.GetFromContext(thingCtx)

			// Deduplicering: en redan behandlad rapport får inte köra
			// tillståndsmaskinen igen (t.ex. Passage-räknare). Publicera bara
			// om eventet behöver säkerställas. Behandlingen är at-least-once.
			if messageID != "" && thing.LastMessageID() == messageID {
				if err := a.publishThingUpdated(thingCtx, thing); err != nil {
					errs = append(errs, err)
				}
				continue
			}

			type pending struct {
				input string
				m     things.Measurement
			}

			// Endast mätningar som är bundna till en ingång på saken, och som
			// är nyare än cachat värde, får ändra aktuellt tillstånd.
			var work []pending
			for _, m := range ms {
				inputs := things.MatchInputs(thing, m)
				if len(inputs) == 0 {
					continue
				}
				if !things.ShouldApply(thing, m) {
					continue
				}
				for _, input := range inputs {
					work = append(work, pending{input: input, m: m})
				}
			}
			if len(work) == 0 {
				continue
			}

			// Uppdatera cachen med rapportens värden före Apply så att
			// aggregeringen ser en sammanhängande bild av rapportens aktuella
			// signalvärden (flera kanaler i samma pack).
			fresh := make([]things.Measurement, 0, len(work))
			for _, w := range work {
				fresh = append(fresh, w.m)
			}
			thing.SetLastObserved(fresh)

			var applyErrs []error
			for _, w := range work {
				err := thing.Apply(thingCtx, w.input, w.m, func(valueProvider things.ValueProvider) error {
					var errs []error

					values := valueProvider.Values()

					for _, v := range values {
						// add value to storage. A value is a measurement with the thingID instead of the deviceID
						errs = append(errs, a.AddValue(thingCtx, thing, v))
					}

					return errors.Join(errs...)
				})
				if err != nil {
					log.Error("could not handle measurement", "input", w.input, "err", err.Error())
					applyErrs = append(applyErrs, err)
				}
			}
			if len(applyErrs) > 0 {
				errs = append(errs, errors.Join(applyErrs...))
				continue
			}

			thing.SetLastMessageID(messageID)

			err = a.saveThing(thingCtx, thing)
			if err != nil {
				log.Error("could not save thing", "err", err.Error())
				errs = append(errs, err)
				continue
			}

			if err := a.publishThingUpdated(thingCtx, thing); err != nil {
				errs = append(errs, err)
			}
		}
	}

	return errors.Join(errs...)
}

func (a *app) publishThingUpdated(ctx context.Context, thing things.Thing) error {
	log := logging.GetFromContext(ctx).With("thing_id", thing.ID())

	msg := &types.ThingUpdated{
		ID:        thing.ID(),
		Type:      thing.Type(),
		Thing:     removeInternalState(thing),
		Tenant:    thing.Tenant(),
		Timestamp: time.Now().UTC(),
	}

	log.Debug("publish message", "content_type", msg.ContentType(), "tenant", thing.Tenant(), "type", thing.Type())

	if err := a.msgCtx.PublishOnTopic(ctx, msg); err != nil {
		log.Error("could not publish message", "err", err.Error())
		return err
	}

	if c := updatedCounter(); c != nil {
		c.Add(ctx, 1)
	}

	return nil
}

// bindingsVersion markerar att en post konverterats till bindningar.
const bindingsVersion = 1

// MigrateBindings konverterar gamla poster (refDevices utan bindningar) till
// bindningar. Idempotent: poster med _bindingsVersion lämnas orörda.
func (a *app) MigrateBindings(ctx context.Context) (int, error) {
	result, err := a.reader.QueryThings(ctx, ThingQuery{Page: Pagination{Export: true}})
	if err != nil {
		return 0, err
	}

	log := logging.GetFromContext(ctx)
	migrated := 0

	for _, raw := range result.Data {
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			return migrated, fmt.Errorf("could not unmarshal thing for migration: %w", err)
		}

		if v, ok := m["_bindingsVersion"].(float64); ok && int(v) >= bindingsVersion {
			continue
		}

		thingType, _ := m["type"].(string)

		var bindings []map[string]any
		if refDevices, ok := m["refDevices"].([]any); ok {
			for _, rd := range refDevices {
				rdm, ok := rd.(map[string]any)
				if !ok {
					continue
				}
				deviceID, _ := rdm["deviceID"].(string)
				if deviceID == "" {
					continue
				}
				for _, in := range things.InputsFor(thingType) {
					bindings = append(bindings, map[string]any{
						"deviceID": deviceID,
						"object":   in.Object,
						"resource": in.Resource,
						"input":    in.Name,
					})
				}
			}
		}

		delete(m, "refDevices")
		if len(bindings) > 0 {
			m["bindings"] = bindings
		}
		m["_bindingsVersion"] = bindingsVersion

		b, err := json.Marshal(m)
		if err != nil {
			return migrated, fmt.Errorf("could not marshal migrated thing: %w", err)
		}

		t, err := things.ConvToThing(b)
		if err != nil {
			return migrated, fmt.Errorf("could not convert migrated thing: %w", err)
		}

		if err := a.writer.UpdateThing(ctx, t); err != nil {
			return migrated, fmt.Errorf("could not save migrated thing: %w", err)
		}

		migrated++
	}

	if migrated > 0 {
		log.Info("migrated things to bindings", "count", migrated)
	}

	return migrated, nil
}

// HasUnmigratedThings rapporterar om någon post saknar bindningsmarkör.
func (a *app) HasUnmigratedThings(ctx context.Context) (bool, error) {
	result, err := a.reader.QueryThings(ctx, ThingQuery{Page: Pagination{Export: true}})
	if err != nil {
		return false, err
	}
	for _, raw := range result.Data {
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			continue
		}
		if _, ok := m["_bindingsVersion"].(float64); ok {
			continue
		}
		// Poster som redan har bindningar (skapade efter T7) är migrerade.
		if _, ok := m["bindings"]; ok {
			continue
		}
		return true, nil
	}
	return false, nil
}

func (a *app) Add(ctx context.Context, b []byte) error {
	b, err := things.NormalizeInputBindings(b)
	if err != nil {
		return err
	}

	t, err := things.ConvToThing(b)
	if err != nil {
		return err
	}

	if t.ID() == "" {
		return ErrMissingThingID
	}
	if strings.Contains(t.ID(), "/") {
		return ErrInvalidThingID
	}
	if t.Tenant() == "" {
		return ErrMissingThingTenant
	}
	if t.Type() == "" {
		return ErrMissingThingType
	}
	if err := validateBindings(t); err != nil {
		return err
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

	b, err := things.NormalizeInputBindings(b)
	if err != nil {
		return err
	}

	t, err := things.ConvToThing(b)
	if err != nil {
		return err
	}

	if t.ID() == "" {
		return ErrMissingThingID
	}
	if strings.Contains(t.ID(), "/") {
		return ErrInvalidThingID
	}
	if t.Tenant() == "" {
		return ErrMissingThingTenant
	}
	if t.Type() == "" {
		return ErrMissingThingType
	}
	if err := validateBindings(t); err != nil {
		return err
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

	// Bakåtkompatibilitet: en patch som anger refDevices expanderas till
	// bindningar för saktypen.
	if refs, ok := patch["refDevices"].([]any); ok {
		thingType, _ := current["type"].(string)
		patch["bindings"] = things.BindingsFromRefDevices(refs, thingType)
		delete(patch, "refDevices")
	}

	for k, v := range patch {
		if slices.Contains([]string{"id", "type"}, k) {
			continue
		}

		if k == "tenant" {
			s, ok := v.(string)
			if !ok {
				return errors.New("invalid tenant value")
			}
			if s != "" && !slices.Contains(tenants, s) {
				return errors.New("you are not allowed to update the tenant of this thing")
			}
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
	if err := validateBindings(patchedThing); err != nil {
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

func (a *app) getConnectedThings(ctx context.Context, deviceID string, tenants []string) ([]things.Thing, error) {
	result, err := a.reader.QueryThings(ctx, ThingsByRefDeviceQuery(deviceID, tenants))
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

	// bindings parsar kolumnen som tidigare innehöll refDevices.
	// Format: device|channel|object|resource|input separerade med komma.
	// En post utan "|" tolkas som ett enhets-id och expanderas till alla
	// ingångar för saktypen (bakåtkompatibelt med äldre things.csv).
	bindings := func(t string, thingType string) []things.Binding {
		if t == "" {
			return nil
		}
		result := []things.Binding{}
		for _, seg := range strings.Split(t, ",") {
			seg = strings.TrimSpace(seg)
			if seg == "" {
				continue
			}
			if !strings.Contains(seg, "|") {
				for _, in := range things.InputsFor(thingType) {
					result = append(result, things.Binding{
						DeviceID: seg,
						Object:   in.Object,
						Resource: in.Resource,
						Input:    in.Name,
					})
				}
				continue
			}
			parts := strings.Split(seg, "|")
			if len(parts) != 5 {
				continue
			}
			result = append(result, things.Binding{
				DeviceID: parts[0],
				Channel:  parts[1],
				Object:   parts[2],
				Resource: parts[3],
				Input:    parts[4],
			})
		}
		return result
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
		// id, type, subType, name, decsription, location, tenant, tags, bindings, args

		id_ := record[0]
		type_ := record[1]
		subType_ := record[2]
		name_ := record[3]
		description_ := record[4]
		location_ := location(record[5])
		tenant_ := record[6]
		tags_ := tags(record[7])
		bindings_ := bindings(record[8], type_)

		m := make(map[string]any)

		current := a.getThingByID(ctx, id_)
		if current != nil {
			b, err := json.Marshal(current)
			if err != nil {
				return err
			}
			err = json.Unmarshal(b, &m)
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

		if len(bindings_) > 0 {
			m["bindings"] = bindings_
		} else {
			delete(m, "bindings")
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
		inputs := inputsFor(t.Type)

		types = append(types, things.ThingType{
			Type:   t.Type,
			Name:   t.Type,
			Inputs: inputs,
		})

		for _, s := range t.SubTypes {
			types = append(types, things.ThingType{
				Type:    t.Type,
				SubType: s,
				Name:    fmt.Sprintf("%s-%s", t.Type, s),
				Inputs:  inputs,
			})
		}
	}

	return types, nil
}

// inputsFor returnerar de unika ingångsnamnen för en saktyp.
func inputsFor(thingType string) []string {
	specs := things.InputsFor(thingType)
	names := make([]string, 0, len(specs))
	seen := make(map[string]struct{}, len(specs))
	for _, s := range specs {
		if _, ok := seen[s.Name]; ok {
			continue
		}
		seen[s.Name] = struct{}{}
		names = append(names, s.Name)
	}
	return names
}
