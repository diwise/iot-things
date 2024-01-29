package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/diwise/iot-entities/internal/pkg/application"
	"github.com/diwise/iot-entities/internal/pkg/presentation/api"
	"github.com/diwise/iot-entities/internal/pkg/storage"
	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/go-chi/chi/v5"
)

const serviceName string = "iot-entities"

func main() {
	serviceVersion := buildinfo.SourceVersion()

	ctx, log, cleanup := o11y.Init(context.Background(), serviceName, serviceVersion)
	defer cleanup()

	var opaFilePath string
	flag.StringVar(&opaFilePath, "policies", "/opt/diwise/config/authz.rego", "An authorization policy file")
	flag.Parse()

	db, err := storage.New(ctx, storage.LoadConfiguration(ctx))
	if err != nil {
		log.Error("could not configure storage", "err", err.Error())
		os.Exit(1)
	}

	app := application.New(db)

	r, err := setupRouter(ctx, opaFilePath, app)
	if err != nil {
		log.Error("could not setup router", "err", err.Error())
		os.Exit(1)
	}

	err = http.ListenAndServe(":8080", r)
	if err != nil {
		log.Error("could listen and serve", "err", err.Error())
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
