package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/diwise/iot-things/internal/app/api"
	app "github.com/diwise/iot-things/internal/app/iot-things"

	"github.com/diwise/iot-things/internal/pkg/storage"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/go-chi/chi/v5"
)

const serviceName string = "iot-things"

func main() {
	serviceVersion := buildinfo.SourceVersion()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, log, cleanup := o11y.Init(ctx, serviceName, serviceVersion)
	defer cleanup()

	var opa, fp string

	flag.StringVar(&opa, "policies", "/opt/diwise/config/authz.rego", "An authorization policy file")
	flag.StringVar(&fp, "things", "/opt/diwise/config/things.csv", "A file with things")
	flag.Parse()

	s, err := storage.New(ctx, storage.LoadConfiguration(ctx))
	if err != nil {
		log.Error("could not configure storage", "err", err.Error())
		os.Exit(1)
	}

	a := app.New(s, s)

	r, err := newRouter(ctx, opa, a)
	if err != nil {
		log.Error("could not setup router", "err", err.Error())
		os.Exit(1)
	}

	err = seed(ctx, fp, a)
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
	messenger.RegisterTopicMessageHandler("message.accepted", app.NewMeasurementsHandler(a, messenger))	
	
	webServer := &http.Server{Addr: ":8080" , Handler: r}

	go func() {
		if err := webServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("could not listen and serve", "err", err.Error())
			os.Exit(1)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	webServer.Shutdown(ctx)
	messenger.Close()
	s.Close()
}

func newRouter(ctx context.Context, opa string, a app.ThingsApp) (*chi.Mux, error) {
	policies, err := os.Open(opa)
	if err != nil {
		return nil, fmt.Errorf("unable to open opa policy file: %s", err.Error())
	}
	defer policies.Close()

	r, err := api.Register(ctx, a, policies)
	if err != nil {
		os.Exit(1)
	}

	return r, nil
}

func seed(ctx context.Context, fp string, a app.ThingsApp) error {
	log := logging.GetFromContext(ctx)
	things, err := os.Open(fp)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			log.Debug("no file with things found", "path", fp)
			return nil
		}
		return err
	}
	defer things.Close()

	return a.Seed(ctx, things)
}
