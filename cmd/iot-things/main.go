package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/diwise/iot-things/internal/application"
	"github.com/diwise/iot-things/internal/infrastructure/storage"
	"github.com/diwise/iot-things/internal/presentation/api"
	k8shandlers "github.com/diwise/service-chassis/pkg/infrastructure/net/http/handlers"

	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/servicerunner"
)

const serviceName string = "iot-things"

func defaultFlags() flagMap {
	return flagMap{
		listenAddress: "0.0.0.0",
		servicePort:   "8080",
		controlPort:   "8000",
		enableTracing: "true",

		dbHost:     "",
		dbUser:     "",
		dbPassword: "",
		dbPort:     "5432",
		dbName:     "diwise",
		dbSSLMode:  "disable",

		policiesFile: "/opt/diwise/config/authz.rego",
		thingsFile:   "/opt/diwise/config/things.csv",
		configFile:   "/opt/diwise/config/config.yaml",
	}
}

func main() {
	ctx, flags := parseExternalConfig(context.Background(), defaultFlags())

	serviceVersion := buildinfo.SourceVersion()
	ctx, logger, cleanup := o11y.Init(ctx, serviceName, serviceVersion, "json")
	defer cleanup()

	policies, err := os.Open(flags[policiesFile])
	exitIf(err, logger, "unable to open opa policy file")

	things, err := os.Open(flags[thingsFile])
	exitIf(err, logger, "unable to open things file")

	config, err := os.Open(flags[configFile])
	exitIf(err, logger, "unable to open config file")

	ctx, cancel := context.WithCancel(ctx)

	cfg := &appConfig{
		cancel: cancel,
	}

	runner, err := initialize(ctx, flags, cfg, policies, things, config)
	exitIf(err, logger, "failed to initialize service runner")

	err = runner.Run(ctx)
	exitIf(err, logger, "failed to start service runner")
}

func initialize(ctx context.Context, flags flagMap, cfg *appConfig, policiesFile, thingsFile, configFile io.ReadCloser) (servicerunner.Runner[appConfig], error) {

	log := logging.GetFromContext(ctx)

	probes := map[string]k8shandlers.ServiceProber{
		"rabbitmq":  func(context.Context) (string, error) { return "ok", nil },
		"timescale": func(context.Context) (string, error) { return "ok", nil },
	}

	var msgCtx messaging.MsgContext
	var app application.ThingsApp

	s, err := storage.New(ctx, storage.NewConfig(flags[dbHost], flags[dbUser], flags[dbPassword], flags[dbPort], flags[dbName], flags[dbSSLMode]))
	exitIf(err, log, "could not configure storage")

	msgCtx, err = messaging.Initialize(ctx, messaging.LoadConfiguration(ctx, serviceName, log))
	exitIf(err, log, "failed to init messenger")

	_, runner := servicerunner.New(ctx, *cfg,
		webserver("control", listen(flags[listenAddress]), port(flags[controlPort]),
			pprof(), liveness(func() error { return nil }), readiness(probes),
		),
		webserver("public", listen(flags[listenAddress]), port(flags[servicePort]), tracing(flags[enableTracing] == "true"),
			muxinit(func(ctx context.Context, identifier string, port string, appCfg *appConfig, handler *http.ServeMux) error {
				defer policiesFile.Close()
				return api.RegisterHandlers(ctx, handler, app, policiesFile)
			}),
		),
		oninit(func(ctx context.Context, ac *appConfig) error {
			log.Debug("initializing servicerunner")
			defer configFile.Close()
			defer thingsFile.Close()

			msgCtx.Start()
			app, err = newApp(ctx, s, s, msgCtx, configFile)
			if err != nil {
				return fmt.Errorf("unable to initialize app: %s", err.Error())
			}

			seed(ctx, thingsFile, app)

			return nil
		}),
		onstarting(func(ctx context.Context, appCfg *appConfig) (err error) {
			log.Debug("starting servicerunner")

			msgCtx.Start()
			msgCtx.RegisterTopicMessageHandler("message.accepted", application.NewMeasurementsHandler(app, msgCtx))

			return nil
		}),
		onshutdown(func(ctx context.Context, appCfg *appConfig) error {
			log.Debug("shutdown servicerunner")

			appCfg.cancel()

			return nil
		}),
	)

	return runner, nil
}

func parseExternalConfig(ctx context.Context, flags flagMap) (context.Context, flagMap) {
	// Allow environment variables to override certain defaults
	envOrDef := env.GetVariableOrDefault

	flags[listenAddress] = envOrDef(ctx, "LISTEN_ADDRESS", flags[listenAddress])
	flags[controlPort] = envOrDef(ctx, "CONTROL_PORT", flags[controlPort])
	flags[servicePort] = envOrDef(ctx, "SERVICE_PORT", flags[servicePort])

	flags[policiesFile] = envOrDef(ctx, "POLICIES_FILE", flags[policiesFile])
	flags[thingsFile] = envOrDef(ctx, "THINGS_FILE", flags[thingsFile])
	flags[configFile] = envOrDef(ctx, "CONFIG_FILE", flags[configFile])

	flags[dbHost] = envOrDef(ctx, "POSTGRES_HOST", flags[dbHost])
	flags[dbPort] = envOrDef(ctx, "POSTGRES_PORT", flags[dbPort])
	flags[dbName] = envOrDef(ctx, "POSTGRES_DBNAME", flags[dbName])
	flags[dbUser] = envOrDef(ctx, "POSTGRES_USER", flags[dbUser])
	flags[dbPassword] = envOrDef(ctx, "POSTGRES_PASSWORD", flags[dbPassword])
	flags[dbSSLMode] = envOrDef(ctx, "POSTGRES_SSLMODE", flags[dbSSLMode])

	apply := func(f flagType) func(string) error {
		return func(value string) error {
			flags[f] = value
			return nil
		}
	}

	// Allow command line arguments to override defaults and environment variables
	flag.Func("policies", "an authorization policy file", apply(policiesFile))
	flag.Func("things", "list of known things", apply(thingsFile))
	flag.Func("config", "a yaml file with configuration", apply(configFile))
	flag.Parse()

	return ctx, flags
}

func newApp(ctx context.Context, r application.ThingsReader, w application.ThingsWriter, m messaging.MsgContext, cfg io.Reader) (application.ThingsApp, error) {
	a := application.New(ctx, r, w, m)
	err := a.LoadConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to load config: %s", err.Error())
	}

	return a, nil
}

func seed(ctx context.Context, fp io.Reader, a application.ThingsApp) error {
	return a.Seed(ctx, fp)
}

func exitIf(err error, logger *slog.Logger, msg string, args ...any) {
	if err != nil {
		logger.With(args...).Error(msg, "err", err.Error())
		time.Sleep(2 * time.Second)
		os.Exit(1)
	}
}
