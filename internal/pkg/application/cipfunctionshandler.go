package application

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/diwise/iot-things/internal/pkg/storage"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
)

func NewCipFunctionsHandler(reader ThingReader, writer ThingWriter) messaging.TopicMessageHandler {
	return func(ctx context.Context, d messaging.IncomingTopicMessage, logger *slog.Logger) {
		var err error

		ctx, span := tracer.Start(ctx, d.TopicName())
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, log := o11y.AddTraceIDToLoggerAndStoreInContext(span, logger, ctx)

		switch d.ContentType() {
		case "application/vnd.diwise.wastecontainer+json":
			err = handleWasteContainer(ctx, reader, writer, d.Body())
		case "application/vnd.diwise.sewer+json":
			err = handleSewer(ctx, reader, writer, d.Body())
		case "application/vnd.diwise.sewagepumpingstation+json":
			err = handleSewagePumpingstation(ctx, reader, writer, d.Body())
		case "application/vnd.diwise.combinedsewageoverflow+json":
			err = handleCombinedSewageOverflow(ctx, reader, writer, d.Body())
		default:
			log.Debug("content type not supported", "content_type", d.ContentType())
		}

		if err != nil {
			log.Error("failed to handle message", "err", err.Error(), "content_type", d.ContentType())
		}
	}
}

type stateMap map[string]any

func (s stateMap) ID() string {
	return s["id"].(string)
}
func (s stateMap) Type() string {
	return s["type"].(string)
}
func unmarshal(b []byte) (stateMap, error) {
	m := make(map[string]any)
	err := json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}
func (s stateMap) ToMap() map[string]any {
	delete(s, "id")
	delete(s, "type")
	delete(s, "tenant")

	//TODO: this is a temporary solution to remove the keys that are not needed
	//      The state contains part of the thing, so this will be redundant
	delete(s, "combinedsewageoverflow")
	delete(s, "wastecontainer")
	delete(s, "sewer")
	delete(s, "sewagepumpingstation")
	delete(s, "passage")

	return s
}

func update(ctx context.Context, writer ThingWriter, key string, b []byte, v any) error {
	thingMap := make(map[string]any)
	err := json.Unmarshal(b, &thingMap)
	if err != nil {
		return err
	}

	thingMap[key] = v
	thingMapBytes, err := json.Marshal(thingMap)
	if err != nil {
		return err
	}

	return writer.UpdateThing(ctx, thingMapBytes)
}

func updateState(ctx context.Context, reader ThingReader, writer ThingWriter, b []byte) error {
	state, err := unmarshal(b)
	if err != nil {
		return err
	}

	thingBytes, _, err := reader.RetrieveThing(ctx, storage.WithID(state.ID()), storage.WithType([]string{state.Type()}), storage.WithMeasurements("true"), storage.WithState("true"))
	if err != nil {
		return err
	}

	return update(ctx, writer, "state", thingBytes, state.ToMap())
}

func handleWasteContainer(ctx context.Context, reader ThingReader, writer ThingWriter, b []byte) error {
	return updateState(ctx, reader, writer, b)
}

func handleSewer(ctx context.Context, reader ThingReader, writer ThingWriter, b []byte) error {
	return updateState(ctx, reader, writer, b)
}

func handleSewagePumpingstation(ctx context.Context, reader ThingReader, writer ThingWriter, b []byte) error {
	return updateState(ctx, reader, writer, b)
}

func handleCombinedSewageOverflow(ctx context.Context, reader ThingReader, writer ThingWriter, b []byte) error {
	return updateState(ctx, reader, writer, b)
}
