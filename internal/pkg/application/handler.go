package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/senml"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("iot-things")

type message struct {
	Pack      senml.Pack `json:"pack"`
	Timestamp time.Time  `json:"timestamp"`
}
type resource struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

func NewTopicMessageHandler(app App) messaging.TopicMessageHandler {
	return func(ctx context.Context, d messaging.IncomingTopicMessage, logger *slog.Logger) {
		var err error

		if d.TopicName() != "message.accepted" && d.TopicName() != "message.transformed" {
			return
		}

		ctx, span := tracer.Start(ctx, d.TopicName())
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, log := o11y.AddTraceIDToLoggerAndStoreInContext(span, logger, ctx)

		var m message
		err = json.Unmarshal(d.Body(), &m)
		if err != nil {
			log.Error("could not unmarshal message", "err", err.Error())
			return
		}

		if m.Pack.Validate() != nil {
			log.Error("message contains an invaid package")
			return
		}

		id, ok := getDeviceID(m.Pack)
		if !ok {
			log.Debug("no deviceID found in package")
			return
		}

		thingID := fmt.Sprintf("urn:diwise:device:%s", id)

		b, err := app.RetrieveRelatedThings(ctx, thingID)
		if err != nil {
			log.Error("no releated thing found for device (1)", "err", err.Error(), "device_id", id)
			return
		}

		var included []resource
		err = json.Unmarshal(b, &included)
		if err != nil {
			log.Error("no releated thing found for device (2)", "err", err.Error(), "device_id", id)
			return
		}

		if len(included) == 0 {
			log.Error("no releated thing found for device (3)", "err", err.Error(), "device_id", id)
			return
		}

		

	}
}

func getMeasurements(ctx context.Context, pack senml.Pack)([]Measurement,error) {
	log := logging.GetFromContext(ctx)

	header, ok := pack.GetRecord(senml.FindByName("0"))
	if !ok {
		return nil,fmt.Errorf("could not find header record (0)")
	}

	tenant, ok := pack.GetStringValue(senml.FindByName("tenant"))
	if !ok {
		return nil,fmt.Errorf("could not find tenant record")
	}

	measurements := make([]Measurement,0)

	deviceID := strings.Split(header.Name, "/")[0]
	urn := header.StringValue
	lat, lon, _ := pack.GetLatLon()

	var errs []error

	for _, r := range pack {
		n, err := strconv.Atoi(r.Name)
		if err != nil || n == 0 {
			continue
		}

		rec, ok := pack.GetRecord(senml.FindByName(r.Name))
		if !ok {
			log.Error("could not find record", "name", r.Name)
			continue
		}

		if rec.Value == nil && rec.BoolValue == nil {
			continue
		}

		id := rec.Name
		name := strconv.Itoa(n)
		ts, _ := rec.GetTime()

		measurement := NewMeasurement(ts, id, deviceID, name, urn, tenant)
		measurement.BoolValue = rec.BoolValue
		measurement.Value = rec.Value
		measurement.StringValue = rec.StringValue
		measurement.Lat = lat
		measurement.Lon = lon
		measurement.Unit = rec.Unit

		measurements = append(measurements, measurement)
	}

	return measurements, errors.Join(errs...)
}

func NewMeasurement(ts time.Time, id, deviceID, name, urn, tenant string) Measurement {
	return Measurement{
		DeviceID:  deviceID,
		ID:        id,
		Name:      name,
		Tenant:    tenant,
		Timestamp: ts,
		Urn:       urn,
	}
}

type Measurement struct {
	DeviceID    string    `json:"deviceID"`
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Tenant      string    `json:"tenant"`
	Timestamp   time.Time `json:"timestamp"`
	Urn         string    `json:"urn"`
	BoolValue   *bool     `json:"vb,omitempty"`
	Lat         float64   `json:"lat"`
	Lon         float64   `json:"lon"`
	StringValue string    `json:"vs,omitempty"`
	Unit        string    `json:"unit,omitempty"`
	Value       *float64  `json:"v,omitempty"`
}

func getDeviceID(m senml.Pack) (string, bool) {
	r, ok := m.GetRecord(senml.FindByName("0"))
	if !ok {
		return "", false
	}
	return strings.Split(r.Name, "/")[0], true
}

func getObjectURN(m senml.Pack) string {
	r, ok := m.GetStringValue(senml.FindByName("0"))
	if !ok {
		return ""
	}
	return r
}

func getObjectID(m senml.Pack) string {
	urn := getObjectURN(m)
	if urn == "" {
		return ""
	}

	if !strings.Contains(urn, ":") {
		return ""
	}

	parts := strings.Split(urn, ":")
	return parts[len(parts)-1]
}
