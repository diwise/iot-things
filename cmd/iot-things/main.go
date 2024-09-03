package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net/http"
	"os"

	"github.com/diwise/iot-things/internal/pkg/application"
	"github.com/diwise/iot-things/internal/pkg/presentation/api"
	"github.com/diwise/iot-things/internal/pkg/storage"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/go-chi/chi/v5"
)

const serviceName string = "iot-things"

func main() {
	serviceVersion := buildinfo.SourceVersion()

	ctx, log, cleanup := o11y.Init(context.Background(), serviceName, serviceVersion)
	defer cleanup()

	var opaFilePath, thingsFilePath string
	flag.StringVar(&opaFilePath, "policies", "/opt/diwise/config/authz.rego", "An authorization policy file")
	flag.StringVar(&thingsFilePath, "things", "/opt/diwise/config/things.csv", "A file with things")
	flag.Parse()

	db, err := storage.New(ctx, storage.LoadConfiguration(ctx))
	if err != nil {
		log.Error("could not configure storage", "err", err.Error())
		os.Exit(1)
	}

	app := application.New(db, db)

	r, err := setupRouter(ctx, opaFilePath, app)
	if err != nil {
		log.Error("could not setup router", "err", err.Error())
		os.Exit(1)
	}

	err = seedThings(ctx, thingsFilePath, app)
	if err != nil {
		log.Error("file with things found but could not seed data", "err", err.Error())
		os.Exit(1)
	}

	config := messaging.LoadConfiguration(ctx, serviceName, log)
	messenger, err := messaging.Initialize(ctx, config)
	if err != nil {
		log.Error("failed to init messenger")
		os.Exit(1)
	}
	messenger.Start()

	messenger.RegisterTopicMessageHandler("message.#", application.NewMeasurementsHandler(db, db))
	messenger.RegisterTopicMessageHandler("cip-function.updated", application.NewCipFunctionsHandler(db, db))

	err = http.ListenAndServe(":8080", r)
	if err != nil {
		log.Error("could not listen and serve", "err", err.Error())
		os.Exit(1)
	}
}

func setupRouter(ctx context.Context, opaFilePath string, app application.App) (*chi.Mux, error) {
	policies, err := os.Open(opaFilePath)
	if err != nil {
		return nil, fmt.Errorf("unable to open opa policy file: %s", err.Error())
	}
	defer policies.Close()

	r, err := api.Register(ctx, app, policies)
	if err != nil {
		os.Exit(1)
	}

	return r, nil
}

func seedThings(ctx context.Context, thingsFilePath string, app application.App) error {
	things, err := os.Open(thingsFilePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	defer things.Close()

	return app.Seed(ctx, things)
}
