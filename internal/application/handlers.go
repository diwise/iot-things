package application

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/diwise/iot-things/internal/application/things"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/senml"
	diwisepkg "github.com/diwise/senml/diwise"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var tracer = otel.Tracer("iot-things")

var errMissingTenant = errors.New("message contains no tenant")

func NewMeasurementsHandler(c context.Context, app ThingsApp) messaging.TopicMessageHandler {
	log := logging.GetFromContext(c)

	totalCounter, err := otel.Meter("iot-things/measurements").Int64Counter(
		"diwise.things.measurements.total",
		metric.WithUnit("1"),
		metric.WithDescription("Total number of received measurements"),
	)

	if err != nil {
		log.Error("failed to create otel total measurements counter", "err", err.Error())
	}

	return func(ctx context.Context, topicMessage messaging.IncomingTopicMessage, logger *slog.Logger) error {
		var err error

		logger = logger.With("topic_name", topicMessage.TopicName())

		ctx, span := tracer.Start(ctx, "receive-measurements")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, log := o11y.AddTraceIDToLoggerAndStoreInContext(span, logger, ctx)

		msg := struct {
			Pack      senml.Pack `json:"pack"`
			Timestamp time.Time  `json:"timestamp"`
		}{}

		err = json.Unmarshal(topicMessage.Body(), &msg)
		if err != nil {
			log.Error("could not unmarshal message", "err", err.Error())
			return messaging.Permanent(err)
		}

		// Referenstid för relativa SenML-tider: mottagningstid.
		parsed, err := diwisepkg.Parse(msg.Pack, time.Now().UTC())
		if err != nil {
			log.Error("message contains an invalid package", "err", err.Error())
			return messaging.Permanent(err)
		}

		deviceID := parsed.DeviceID()
		tenant := parsed.Tenant()

		if tenant == "" {
			// Core berikar och validerar tenant; ett tomt värde är ett
			// kontraktsbrott som aldrig läker vid retry.
			log.Error("message contains no tenant")
			return messaging.Permanent(errMissingTenant)
		}

		logger = logger.With("device_id", deviceID, "tenant", tenant)

		measurements := convPack(parsed)

		if len(measurements) == 0 {
			log.Warn("no measurements found in pack")
			return nil
		}

		totalCounter.Add(ctx, 1)

		ctx = logging.NewContextWithLogger(ctx, logger)

		// Fel propageras så att rapporten kan återlevereras. Rapporten
		// dedupliceras per sak på meddelandets id, så en retry kör inte
		// tillståndsmaskinen igen. Historik är idempotent per (tid, id).
		if err := app.HandleMeasurements(ctx, tenant, topicMessage.MessageID(), measurements); err != nil {
			log.Error("could not handle measurements", "err", err.Error())
			return err
		}

		return nil
	}
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

	return StripInternalState(m)
}

// StripInternalState tar bort interna fält ur en serialiserad sak: cachade
// mätningar under refDevices samt fält som börjar med "_". Enda
// implementationen; används både för thing.updated och i API-presentationen.
func StripInternalState(m map[string]any) map[string]any {
	if refDevices, ok := m["refDevices"]; ok {
		if ref, ok := refDevices.([]any); ok {
			for _, device := range ref {
				x, ok := device.(map[string]any)
				if !ok {
					continue
				}
				delete(x, "measurements")
			}
			m["refDevices"] = ref
		}
	}

	for k := range m {
		if strings.HasPrefix(k, "_") {
			delete(m, k)
		}
	}

	return m
}

// convPack konverterar varje objektobservation till mätningar med
// observationens egen URN och effektiva metadata. ID är det fullständiga
// recordnamnet (unikt per enhet/objekt/kanal/resurs); resursuppslag sker
// alltid inom rätt observation så likadana resursnummer från olika objekt
// aldrig sammanblandas.
func convPack(parsed *diwisepkg.Pack) []things.Measurement {
	measurements := make([]things.Measurement, 0)

	for _, o := range parsed.Objects() {
		urn := o.URN()
		meta := o.Metadata()

		var source *string
		if meta.Source != "" {
			vs := meta.Source
			source = &vs
		}

		for _, r := range o.Resources() {
			name := r.Name[strings.LastIndex(r.Name, "/")+1:]
			n, err := strconv.Atoi(name)
			if err != nil || n == 0 {
				continue
			}

			if r.Value == nil && r.BoolValue == nil {
				continue
			}

			ts, _ := r.GetTime()

			var vs *string
			if r.StringValue != "" {
				v := r.StringValue
				vs = &v
			}

			m := things.Measurement{
				ID:          r.Name,
				Timestamp:   ts.UTC(),
				Urn:         urn,
				BoolValue:   r.BoolValue,
				Value:       r.Value,
				StringValue: vs,
				Unit:        r.Unit,
				Source:      source,
				Ref:         deviceID(r.Name),
			}

			measurements = append(measurements, m)
		}
	}

	return measurements
}

func deviceID(id string) string {
	parts := strings.Split(id, "/")
	if len(parts) > 0 {
		return parts[0]
	}
	return id
}
