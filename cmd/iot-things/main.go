package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/diwise/iot-things/internal/app/api"
	app "github.com/diwise/iot-things/internal/app/iot-things"

	"github.com/diwise/iot-things/internal/pkg/storage"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/go-chi/chi/v5"
)

const serviceName string = "iot-things"

func main() {
	serviceVersion := buildinfo.SourceVersion()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, log, cleanup := o11y.Init(ctx, serviceName, serviceVersion, "json")
	defer cleanup()

	var opa, fp, cfgFile string

	flag.StringVar(&opa, "policies", "/opt/diwise/config/authz.rego", "An authorization policy file")
	flag.StringVar(&fp, "things", "/opt/diwise/config/things.csv", "A file with things")
	flag.StringVar(&cfgFile, "config", "/opt/diwise/config/config.yaml", "A yaml file with configuration")
	flag.Parse()

	s, err := storage.New(ctx, storage.LoadConfiguration(ctx))
	exitIf(err, log, "could not configure storage")
	defer s.Close()

	config := messaging.LoadConfiguration(ctx, serviceName, log)
	messenger, err := messaging.Initialize(ctx, config)
	exitIf(err, log, "failed to init messenger")
	messenger.Start()
	defer messenger.Close()

	a, err := newApp(ctx, s, s, messenger, cfgFile)
	exitIf(err, log, "could not configure application")

	messenger.RegisterTopicMessageHandler("message.accepted", app.NewMeasurementsHandler(a, messenger))

	r, err := newRouter(ctx, opa, a)
	exitIf(err, log, "could not setup router")

	err = seed(ctx, fp, a)
	exitIf(err, log, "file with things found but could not seed data")

	port := env.GetVariableOrDefault(ctx, "SERVICE_PORT", "8080")

	webServer := &http.Server{Addr: ":" + port, Handler: r}

	go func() {
		if err := webServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			exitIf(err, log, "could not listen and serve", "port", port)
		}
	}()

	defer webServer.Shutdown(ctx)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}

func newApp(ctx context.Context, r app.ThingsReader, w app.ThingsWriter, m messaging.MsgContext, cfgFilePath string) (app.ThingsApp, error) {
	f, err := os.Open(cfgFilePath)
	if err != nil {
		return nil, fmt.Errorf("unable to open config file: %s", err.Error())
	}
	defer f.Close()

	a := app.New(ctx, r, w, m)
	err = a.LoadConfig(ctx, f)
	if err != nil {
		return nil, fmt.Errorf("unable to load config: %s", err.Error())
	}

	return a, nil
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

func exitIf(err error, logger *slog.Logger, msg string, args ...any) {
	if err != nil {
		logger.With(args...).Error(msg, "err", err.Error())
		time.Sleep(2 * time.Second)
		os.Exit(1)
	}
}
