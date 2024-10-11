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
	"github.com/diwise/iot-things/pkg/types"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/senml"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("iot-things")

func NewMeasurementsHandler(app ThingsApp, msgCtx messaging.MsgContext) messaging.TopicMessageHandler {
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

		if len(connectedThings) == 0 { // is it OK if len > 1?
			log.Debug("no connected things found")
			return
		}

		measurements, err := convPack(ctx, msg.Pack)
		if err != nil {
			log.Error("could not convert pack to measurements", "err", err.Error())
			return
		}

		errs := make([]error, 0)
		changes := map[string]int{}

		for i, t := range connectedThings { // for each connected thing... is it valid to connect a sensor to multiple things?
			for _, m := range measurements {
				err := t.Handle(m, func(m things.Measurements) error { // handle each measurement
					var errs []error

					for _, v := range m.Values() {
						errs = append(errs, app.AddValue(ctx, t, v)) // add value to storage. A value is a measurement with the thingID instead of the deviceID
						changes[t.ID()] = i
					}

					return errors.Join(errs...)
				})
				if err != nil {
					errs = append(errs, err)
					continue
				}
				t.SetValue(m) // adds the current measurement to its (ref)device
			}
			errs = append(errs, app.SaveThing(ctx, t))
		}

		if errors.Join(errs...) != nil {
			log.Error("errors occured when handle measurements", "err", err.Error())
			return
		}

		if len(changes) > 0 {
			for _, v := range changes {
				err = msgCtx.PublishOnTopic(ctx, &types.ThingUpdated{ // for each updated connected thing, publish thing.updated
					ID:        connectedThings[v].ID(),
					Type:      connectedThings[v].Type(),
					Tenant:    connectedThings[v].Tenant(),
					Timestamp: msg.Timestamp,
				})
				if err != nil {
					log.Error("could not publish thing update", "err", err.Error())
					return
				}
			}
		}
	}
}

func convPack(ctx context.Context, pack senml.Pack) ([]things.Value, error) {
	log := logging.GetFromContext(ctx)

	header, ok := pack.GetRecord(senml.FindByName("0"))
	if !ok {
		return nil, fmt.Errorf("could not find header record (0)")
	}

	values := make([]things.Value, len(pack))

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

		m := things.Value{
			ID:          id,
			Timestamp:   ts.UTC(),
			Urn:         urn,
			BoolValue:   rec.BoolValue,
			Value:       rec.Value,
			StringValue: vs,
			Unit:        rec.Unit,
		}

		values = append(values, m)
	}

	return values, errors.Join(errs...)
}

func getDeviceID(pack senml.Pack) (string, bool) {
	r, ok := pack.GetRecord(senml.FindByName("0"))
	if !ok {
		return "", false
	}
	return strings.Split(r.Name, "/")[0], true
}
