package iotthings

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/diwise/iot-things/internal/app/iot-things/things"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/senml"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("iot-things")

func NewMeasurementsHandler(app ThingsApp) messaging.TopicMessageHandler {
	return func(ctx context.Context, d messaging.IncomingTopicMessage, logger *slog.Logger) {
		var err error

		ctx, span := tracer.Start(ctx, d.TopicName())
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, log := o11y.AddTraceIDToLoggerAndStoreInContext(span, logger, ctx)

		msg := struct {
			Pack      senml.Pack `json:"pack"`
			Timestamp time.Time  `json:"timestamp"`
		}{}

		err = json.Unmarshal(d.Body(), &msg)
		if err != nil {
			log.Error("could not unmarshal message", "err", err.Error())
			return
		}

		if msg.Pack.Validate() != nil {
			log.Error("message contains an invalid package")
			return
		}

		deviceID, ok := getDeviceID(msg.Pack)
		if !ok {
			log.Debug("no deviceID found in package")
			return
		}

		connectedThings, err := app.GetConnectedThings(ctx, deviceID)
		if err != nil {
			log.Error("could not get connected things", "err", err.Error())
			return
		}

		if len(connectedThings) == 0 {
			log.Debug("no connected things found")
			return
		}

		m, err := convPack(ctx, msg.Pack)
		if err != nil {
			log.Error("could not pack measurements", "err", err.Error())
			return
		}

		errs := make([]error, 0)

		for _, t := range connectedThings {
			for _, m := range m {
				errs = append(errs, t.Handle(m, func(m things.Measurement) error {
					return app.AddMeasurement(ctx, t, m)
				}))
			}
			errs = append(errs, app.SaveThing(ctx, t))
		}

		if errors.Join(errs...) != nil {
			log.Error("errors occured when handle measurements", "err", err.Error())
			return
		}
	}
}

func convPack(ctx context.Context, pack senml.Pack) ([]things.Measurement, error) {
	log := logging.GetFromContext(ctx)

	header, ok := pack.GetRecord(senml.FindByName("0"))
	if !ok {
		return nil, fmt.Errorf("could not find header record (0)")
	}

	measurements := make([]things.Measurement, len(pack))

	urn := header.StringValue

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
		ts, _ := rec.GetTime()
		var vs *string
		if rec.StringValue != "" {
			vs = &rec.StringValue
		}

		m := things.Measurement{
			ID:          id,
			Timestamp:   ts,
			Urn:         urn,
			BoolValue:   rec.BoolValue,
			Value:       rec.Value,
			StringValue: vs,
			Unit:        rec.Unit,
		}

		measurements = append(measurements, m)
	}

	return measurements, errors.Join(errs...)
}

func getDeviceID(pack senml.Pack) (string, bool) {
	r, ok := pack.GetRecord(senml.FindByName("0"))
	if !ok {
		return "", false
	}
	return strings.Split(r.Name, "/")[0], true
}
