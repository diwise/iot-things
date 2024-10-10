package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/senml"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
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

		things, err := app.GetConnectedThings(ctx, deviceID)
		if err != nil {
			log.Error("could not get connected things", "err", err.Error())
			return
		}

		if len(things) == 0 {
			log.Debug("no connected things found")
			return
		}

		m, err := packToMeasurements(ctx, msg.Pack)
		if err != nil {
			log.Error("could not pack measurements", "err", err.Error())
			return
		}

		errs := make([]error, 0)

		for _, t := range things {
			for _, m := range m {
				errs = append(errs, t.Handle(m, func(m Measurement) error {
					return app.AddMeasurement(ctx, t, m)
				}))
			}
		}

		if errors.Join(errs...) != nil {
			log.Error("errors occured when handle measurements", "err", err.Error())
			return
		}
	}
}
