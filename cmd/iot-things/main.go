package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
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

		policiesFile:      "/opt/diwise/config/authz.rego",
		authzAccessObject: "false",
		thingsFile:        "/opt/diwise/config/things.csv",
		configFile:        "/opt/diwise/config/config.yaml",

		logLevel: "debug",
	}
}

func main() {
	ctx, flags := parseExternalConfig(context.Background(), defaultFlags())

	serviceVersion := buildinfo.SourceVersion()
	ctx, logger, cleanup := o11y.Init(ctx, serviceName, serviceVersion, "json")
	defer cleanup()

	logging.SetLogLevel(parseLogLevel(flags[logLevel]))

	policies, err := os.Open(flags[policiesFile])
	exitIf(err, logger, "unable to open opa policy file")

	things, err := os.Open(flags[thingsFile])
	exitIf(err, logger, "unable to open things file")

	config, err := os.Open(flags[configFile])
	exitIf(err, logger, "unable to open config file")

	cfg := &appConfig{}

	runner, err := initialize(ctx, flags, cfg, policies, things, config)
	exitIf(err, logger, "failed to initialize service runner")

	err = runner.Run(ctx)
	exitIf(err, logger, "failed to start service runner")
}

func initialize(ctx context.Context, flags flagMap, cfg *appConfig, policiesFile, thingsFile, configFile io.ReadCloser) (servicerunner.Runner[appConfig], error) {
	log := logging.GetFromContext(ctx)

	probes := readinessProbes()

	var s storage.Storage
	var msgCtx messaging.MsgContext
	var app application.ThingsApp

	owned := &ownedResources{}

	_, runner := servicerunner.New(ctx, *cfg,
		webserver("control", listen(flags[listenAddress]), port(flags[controlPort]),
			pprof(), liveness(func() error { return nil }), readiness(probes),
		),
		webserver("public", listen(flags[listenAddress]), port(flags[servicePort]), tracing(tracingEnabled(flags)),
			muxinit(func(ctx context.Context, identifier string, port string, appCfg *appConfig, handler *http.ServeMux) error {
				defer policiesFile.Close()
				log.Debug("register api handlers...")
				return api.RegisterHandlers(ctx, handler, app, policiesFile, api.WithAccessObjectAuthorization(accessObjectEnabled(flags)))
			}),
		),
		oninit(func(ctx context.Context, ac *appConfig) error {
			log.Debug("initializing servicerunner")

			defer configFile.Close()
			defer thingsFile.Close()

			var err error

			s, err = storage.New(ctx, storageConfigFromFlags(flags))
			if err != nil {
				return fmt.Errorf("could not configure storage: %w", err)
			}

			msgCtx, err = messaging.Initialize(ctx, messaging.LoadConfiguration(ctx, serviceName, log))
			if err != nil {
				s.Close()
				s = nil
				return fmt.Errorf("failed to init messenger: %w", err)
			}

			owned.messenger = msgCtx
			owned.tracker = &handlerTracker{}
			owned.storage = s

			log.Debug("creating application...")
			app, err = newApp(ctx, s, s, msgCtx, configFile)
			if err != nil {
				s.Close()
				s = nil
				return fmt.Errorf("unable to initialize app: %s", err.Error())
			}

			owned.app = app

			log.Debug("seeding things...")
			err = seed(ctx, thingsFile, app)
			if err != nil {
				s.Close()
				s = nil
				return fmt.Errorf("unable to seed things: %s", err.Error())
			}

			return nil
		}),
		onstarting(func(ctx context.Context, appCfg *appConfig) (err error) {
			log.Debug("starting servicerunner")

			// OnStarting failures bypass OnShutdown in the runner, so
			// clean up acquired resources on every error path below.
			defer func() {
				if err != nil {
					owned.close(ctx)
				}
			}()

			log.Debug("starting publisher...")
			app.Start(ctx)

			log.Debug("starting messaging...")
			msgCtx.Start()

			log.Debug("register topic handler...")
			tracked := &trackingMessenger{MsgContext: msgCtx, tracker: owned.tracker}
			err = tracked.RegisterTopicMessageHandler("message.accepted", application.NewMeasurementsHandler(ctx, app))
			if err != nil {
				return fmt.Errorf("unable to register message handler: %s", err.Error())
			}

			return nil
		}),
		onshutdown(func(ctx context.Context, appCfg *appConfig) error {
			log.Debug("shutdown servicerunner")

			owned.close(ctx)

			return nil
		}),
	)

	return runner, nil
}

// tracingEnabled is the minimal production seam for the tracing toggle.
// Only the exact string "true" enables tracing; ParseBool spellings
// such as "TRUE" or "1" intentionally do not.
func tracingEnabled(flags flagMap) bool {
	return flags[enableTracing] == "true"
}

// accessObjectEnabled is the minimal production seam for the
// access-object toggle, using strconv semantics: invalid values
// silently keep the legacy tenants model.
func accessObjectEnabled(flags flagMap) bool {
	v, _ := strconv.ParseBool(flags[authzAccessObject])
	return v
}

// readinessProbes returns the named readiness stubs. Per harmonization
// standard they always report OK and never call any dependency.
func readinessProbes() map[string]k8shandlers.ServiceProber {
	return map[string]k8shandlers.ServiceProber{
		"rabbitmq":  func(context.Context) (string, error) { return "ok", nil },
		"timescale": func(context.Context) (string, error) { return "ok", nil },
	}
}

// Shutdown budget for admitted handler drain, within the runner's 30s
// shutdown hook budget. The hook itself never receives the runner's
// timeout, so shutdown derives its own bound here.
const shutdownHandlerDrainTimeout = 10 * time.Second

// handlerTracker tracks admitted topic-message deliveries so shutdown
// can await them. The messaging library acknowledges on dispatch and its
// Close only joins the dispatch loop, never the handler goroutines.
type handlerTracker struct {
	wg sync.WaitGroup
}

func (t *handlerTracker) track(next messaging.TopicMessageHandler) messaging.TopicMessageHandler {
	return func(ctx context.Context, msg messaging.IncomingTopicMessage, log *slog.Logger) {
		t.wg.Add(1)
		defer t.wg.Done()
		next(ctx, msg, log)
	}
}

// wait blocks until tracked handlers complete or the timeout elapses,
// reporting whether all handlers finished.
func (t *handlerTracker) wait(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		t.wg.Wait()
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// trackingMessenger decorates handler registration with delivery
// tracking. All other MsgContext behavior is forwarded unchanged.
type trackingMessenger struct {
	messaging.MsgContext
	tracker *handlerTracker
}

func (m *trackingMessenger) RegisterTopicMessageHandler(routingKey string, h messaging.TopicMessageHandler) error {
	return m.MsgContext.RegisterTopicMessageHandler(routingKey, m.tracker.track(h))
}

// ownedResources tracks the resources created during OnInit so shutdown
// is nil-safe, ordered and idempotent. The underlying messenger Close is
// not safe to call twice, hence the sync.Once guard.
//
// Shutdown order: stop inflow (messenger), await admitted handlers
// within budget, stop and join the publisher, then close storage. HTTP
// servers stay live until after OnShutdown returns (runner behavior);
// that residual window is documented, not fixed here.
type ownedResources struct {
	once      sync.Once
	messenger messaging.MsgContext
	tracker   *handlerTracker
	app       interface{ Stop() }
	storage   interface{ Close() }
}

func (o *ownedResources) close(context.Context) {
	o.once.Do(func() {
		if o.messenger != nil {
			o.messenger.Close()
		}
		if o.tracker != nil {
			o.tracker.wait(shutdownHandlerDrainTimeout)
		}
		if o.app != nil {
			o.app.Stop()
		}
		if o.storage != nil {
			o.storage.Close()
		}
	})
}

// storageConfigFromFlags builds the storage configuration explicitly
// from cmd-owned flags. This is the only path used to configure
// storage; storage.LoadConfiguration was removed as dead code
// (THINGS-001).
func storageConfigFromFlags(flags flagMap) storage.Config {
	return storage.NewConfig(flags[dbHost], flags[dbUser], flags[dbPassword], flags[dbPort], flags[dbName], flags[dbSSLMode])
}

func parseExternalConfig(ctx context.Context, flags flagMap) (context.Context, flagMap) {
	// Allow environment variables to override certain defaults
	envOrDef := env.GetVariableOrDefault

	flags[listenAddress] = envOrDef(ctx, "LISTEN_ADDRESS", flags[listenAddress])
	flags[controlPort] = envOrDef(ctx, "CONTROL_PORT", flags[controlPort])
	flags[servicePort] = envOrDef(ctx, "SERVICE_PORT", flags[servicePort])

	flags[policiesFile] = envOrDef(ctx, "POLICIES_FILE", flags[policiesFile])
	flags[authzAccessObject] = envOrDef(ctx, "AUTHZ_ACCESS_OBJECT_ENABLED", flags[authzAccessObject])
	flags[thingsFile] = envOrDef(ctx, "THINGS_FILE", flags[thingsFile])
	flags[configFile] = envOrDef(ctx, "CONFIG_FILE", flags[configFile])

	flags[dbHost] = envOrDef(ctx, "POSTGRES_HOST", flags[dbHost])
	flags[dbPort] = envOrDef(ctx, "POSTGRES_PORT", flags[dbPort])
	flags[dbName] = envOrDef(ctx, "POSTGRES_DBNAME", flags[dbName])
	flags[dbUser] = envOrDef(ctx, "POSTGRES_USER", flags[dbUser])
	flags[dbPassword] = envOrDef(ctx, "POSTGRES_PASSWORD", flags[dbPassword])
	flags[dbSSLMode] = envOrDef(ctx, "POSTGRES_SSLMODE", flags[dbSSLMode])

	flags[logLevel] = envOrDef(ctx, "LOG_LEVEL", flags[logLevel])

	apply := func(f flagType) func(string) error {
		return func(value string) error {
			flags[f] = value
			return nil
		}
	}

	// Allow command line arguments to override defaults and environment variables
	flag.Func("policies", "an authorization policy file", apply(policiesFile))
	flag.Func("authz-access-object", "enable access-object authorization policy result model", apply(authzAccessObject))
	flag.Func("things", "list of known things", apply(thingsFile))
	flag.Func("config", "a yaml file with configuration", apply(configFile))
	flag.Func("loglevel", "set the log level", apply(logLevel))
	flag.Parse()

	return ctx, flags
}

func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelDebug
	}
}

func newApp(ctx context.Context, r application.ThingsReader, w application.ThingsWriter, m messaging.MsgContext, cfg io.Reader) (application.ThingsApp, error) {
	a := application.New(r, w, m)
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
