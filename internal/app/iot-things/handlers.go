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

		_, ok := extractDeviceID(msg.Pack)
		if !ok {
			log.Debug("no deviceID found in package")
			return
		}

		log.Debug("received measurements", "pack", msg.Pack)

		measurements, err := convPack(ctx, msg.Pack)
		if err != nil {
			log.Error("could not convert pack to measurements", "err", err.Error())
			return
		}

		if len(measurements) == 0 {
			log.Debug("no measurements found in pack")
			return
		}

		app.HandleMeasurements(ctx, measurements)
	}
}

func unique(arr []string) []string {
	unique := make(map[string]struct{})
	for _, s := range arr {
		unique[s] = struct{}{}
	}

	result := make([]string, 0, len(unique))
	for s := range unique {
		result = append(result, s)
	}

	return result
}

func removeInternalState(t things.Thing) map[string]any {
	m := make(map[string]any)
	b, err := json.Marshal(t)
	if err != nil {
		return m
	}
	err = json.Unmarshal(b, &m)
	if err != nil {
		return m
	}

	if refDevices, ok := m["refDevices"]; ok {
		if ref, ok := refDevices.([]any); ok {
			for _, device := range ref {
				x := device.(map[string]any)
				delete(x, "measurements")
			}
			m["refDevices"] = ref
		}
	}

	// remove internal fields (i.e. fields starting with "_")
	for k := range m {
		if strings.HasPrefix(k, "_") {
			delete(m, k)
		}
	}

	return m
}

func convPack(ctx context.Context, pack senml.Pack) ([]things.Measurement, error) {
	log := logging.GetFromContext(ctx)

	header, ok := pack.GetRecord(senml.FindByName("0"))
	if !ok {
		return nil, fmt.Errorf("could not find header record (0)")
	}

	measurements := make([]things.Measurement, 0)

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

		if id == "" || urn == "" {
			continue
		}

		m := things.Measurement{
			ID:          id,
			Timestamp:   ts.UTC(),
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

func extractDeviceID(pack senml.Pack) (string, bool) {
	r, ok := pack.GetRecord(senml.FindByName("0"))
	if !ok {
		return "", false
	}
	return strings.Split(r.Name, "/")[0], true
}
