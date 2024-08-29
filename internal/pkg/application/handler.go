package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/diwise/iot-things/internal/pkg/storage"
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

func NewTopicMessageHandler(reader ThingReader, writer ThingWriter) messaging.TopicMessageHandler {
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

		deviceThingID := fmt.Sprintf("urn:diwise:device:%s", id)

		b, err := reader.RetrieveRelatedThings(ctx, storage.WithThingID(deviceThingID))
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
			log.Debug("no releated thing found for device (3)", "device_id", id)
			return
		}

		measurements, err := getMeasurements(ctx, m.Pack)
		if err != nil {
			log.Error("could not get measurements from pack", "err", err.Error(), "device_id", id)
			return
		}

		for _, inc := range included {
			thingID := fmt.Sprintf("urn:diwise:%s:%s", strings.ToLower(inc.Type), strings.ToLower(inc.ID))

			thingBytes, _, err := reader.RetrieveThing(ctx, storage.WithThingID(thingID))
			if err != nil {
				log.Error("could not fetch thing to add values to", "err", err.Error(), "thing_id", thingID)
				return
			}

			var thing Thing
			err = json.Unmarshal(thingBytes, &thing)
			if err != nil {
				log.Error("could not unmarshal thing", "err", err.Error(), "thing_id", thingID)
				return
			}

			update := false

			if len(thing.Measurements) == 0 {
				log.Debug("no current measurements found, add all in pack to thing", "thing_id", thingID)
				thing.Measurements = measurements
				update = true
			} else {
				for i, tm := range measurements {
					id := slices.IndexFunc(thing.Measurements, func(mm Measurement) bool {
						return strings.EqualFold(tm.ID, mm.ID)
					})
					if id > -1 {						
						if measurements[i].Timestamp.After(thing.Measurements[id].Timestamp) {
							continue
						}

						log.Debug("update existing measurement", "measurement_id", measurements[i].ID)
						update = true
						thing.Measurements[id] = measurements[i]
					} else {
						update = true
						log.Debug("append new measurement", "measurement_id", measurements[i].ID)
						thing.Measurements = append(thing.Measurements, tm)
					}
				}
			}

			if update {
				var thingMap map[string]any
				err = json.Unmarshal(thingBytes, &thingMap)
				if err != nil {
					log.Error("could not unmarshal thing to map[string]any", "err", err.Error())
					return
				}
				thingMap["measurements"] = thing.Measurements

				thingMapBytes, err := json.Marshal(thingMap)
				if err != nil {
					log.Error("could not marshal map", "err", err.Error())
					return
				}

				err = writer.UpdateThing(ctx, thingMapBytes)
				if err != nil {
					log.Error("could not update thing with measurements", "err", err.Error())
					return
				}
			}

			log.Debug(fmt.Sprintf("%d measurement(s) added or updated on thing %s", len(measurements), thingID))
		}
	}
}

func getMeasurements(ctx context.Context, pack senml.Pack) ([]Measurement, error) {
	log := logging.GetFromContext(ctx)

	header, ok := pack.GetRecord(senml.FindByName("0"))
	if !ok {
		return nil, fmt.Errorf("could not find header record (0)")
	}

	measurements := make([]Measurement, 0)

	//deviceID := strings.Split(header.Name, "/")[0]
	urn := header.StringValue
	//lat, lon, _ := pack.GetLatLon()

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

		measurement := NewMeasurement(ts, id,  urn)
		measurement.BoolValue = rec.BoolValue
		measurement.Value = rec.Value
		measurement.StringValue = rec.StringValue
		//measurement.Lat = lat
		//measurement.Lon = lon
		measurement.Unit = rec.Unit

		measurements = append(measurements, measurement)
	}

	return measurements, errors.Join(errs...)
}

func getDeviceID(m senml.Pack) (string, bool) {
	r, ok := m.GetRecord(senml.FindByName("0"))
	if !ok {
		return "", false
	}
	return strings.Split(r.Name, "/")[0], true
}
