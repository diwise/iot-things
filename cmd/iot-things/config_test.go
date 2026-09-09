package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"testing"

	"github.com/diwise/iot-things/internal/infrastructure/storage"
	"github.com/matryer/is"
)

// THINGS-001: locks that storage configuration is built explicitly
// from cmd-owned flags, field by field, including the connection
// string handed to the pool.
func TestStorageConfigFromFlags(t *testing.T) {
	is := is.New(t)

	flags := defaultFlags()
	flags[dbHost] = "dbhost"
	flags[dbUser] = "dbuser"
	flags[dbPassword] = "secret"
	flags[dbPort] = "5433"
	flags[dbName] = "thingsdb"
	flags[dbSSLMode] = "require"

	cfg := storageConfigFromFlags(flags)

	is.Equal(cfg, storage.NewConfig("dbhost", "dbuser", "secret", "5433", "thingsdb", "require"))
	is.Equal(cfg.ConnStr(), "postgres://dbuser:secret@dbhost:5433/thingsdb?sslmode=require")
}

// THINGS-001: locks that defaults flow unchanged into the storage
// configuration.
func TestStorageConfigFromDefaultFlags(t *testing.T) {
	is := is.New(t)

	cfg := storageConfigFromFlags(defaultFlags())

	is.Equal(cfg.ConnStr(), "postgres://:@:5432/diwise?sslmode=disable")
}

func withCleanFlags(t *testing.T, args []string) {
	t.Helper()

	oldArgs := os.Args
	oldCommandLine := flag.CommandLine
	t.Cleanup(func() {
		os.Args = oldArgs
		flag.CommandLine = oldCommandLine
	})

	flag.CommandLine = flag.NewFlagSet(args[0], flag.ContinueOnError)
	os.Args = args
}

// HARM-004: locks all current defaults.
func TestDefaultFlags(t *testing.T) {
	is := is.New(t)

	flags := defaultFlags()

	expected := map[flagType]string{
		listenAddress:     "0.0.0.0",
		servicePort:       "8080",
		controlPort:       "8000",
		enableTracing:     "true",
		dbHost:            "",
		dbUser:            "",
		dbPassword:        "",
		dbPort:            "5432",
		dbName:            "diwise",
		dbSSLMode:         "disable",
		policiesFile:      "/opt/diwise/config/authz.rego",
		authzAccessObject: "false",
		thingsFile:        "/opt/diwise/config/things.csv",
		configFile:        "/opt/diwise/config/config.yaml",
		logLevel:          "debug",
	}

	is.Equal(len(flags), len(expected))
	for key, want := range expected {
		is.Equal(flags[key], want)
	}
}

// HARM-004: locks env override precedence over defaults.
func TestEnvOverrides(t *testing.T) {
	is := is.New(t)
	withCleanFlags(t, []string{"iot-things"})

	t.Setenv("LISTEN_ADDRESS", "127.0.0.1")
	t.Setenv("SERVICE_PORT", "9090")
	t.Setenv("CONTROL_PORT", "9001")
	t.Setenv("THINGS_FILE", "/tmp/things.csv")
	t.Setenv("CONFIG_FILE", "/tmp/config.yaml")
	t.Setenv("LOG_LEVEL", "info")

	_, flags := parseExternalConfig(context.Background(), defaultFlags())

	is.Equal(flags[listenAddress], "127.0.0.1")
	is.Equal(flags[servicePort], "9090")
	is.Equal(flags[controlPort], "9001")
	is.Equal(flags[thingsFile], "/tmp/things.csv")
	is.Equal(flags[configFile], "/tmp/config.yaml")
	is.Equal(flags[logLevel], "info")
}

// HARM-004: locks CLI-over-env precedence.
func TestCLIOverridesEnv(t *testing.T) {
	is := is.New(t)
	withCleanFlags(t, []string{"iot-things", "-loglevel=error", "-things=/tmp/t.csv"})

	t.Setenv("LOG_LEVEL", "info")
	t.Setenv("THINGS_FILE", "/tmp/env.csv")

	_, flags := parseExternalConfig(context.Background(), defaultFlags())

	is.Equal(flags[logLevel], "error")
	is.Equal(flags[thingsFile], "/tmp/t.csv")
}

// HARM-004: locks the current limitation that enableTracing exists in
// the flag model but can be controlled neither via env nor CLI.
// Changing this is a deliberate decision.
func TestEnableTracingNotExternallyConfigurable(t *testing.T) {
	is := is.New(t)
	withCleanFlags(t, []string{"iot-things"})

	t.Setenv("ENABLE_TRACING", "false")

	_, flags := parseExternalConfig(context.Background(), defaultFlags())

	is.Equal(flags[enableTracing], "true")
}

// HARM-004: locks log level parsing, including the silent debug fallback.
func TestParseLogLevel(t *testing.T) {
	is := is.New(t)

	is.Equal(parseLogLevel("debug"), slog.LevelDebug)
	is.Equal(parseLogLevel("info"), slog.LevelInfo)
	is.Equal(parseLogLevel("warn"), slog.LevelWarn)
	is.Equal(parseLogLevel("warning"), slog.LevelWarn)
	is.Equal(parseLogLevel("error"), slog.LevelError)
	is.Equal(parseLogLevel("bogus"), slog.LevelDebug)
}

// REV-015: the two bool toggles intentionally use different
// interpretations. Tests target the production seams so a changed
// interpretation breaks them.
func TestBoolToggleInterpretations(t *testing.T) {
	for _, tc := range []struct {
		name         string
		value        string
		tracing      bool
		accessObject bool
	}{
		{"exact true", "true", true, true},
		{"uppercase TRUE", "TRUE", false, true},
		{"numeric 1", "1", false, true},
		{"false", "false", false, false},
		{"numeric 0", "0", false, false},
		{"empty", "", false, false},
		{"invalid", "bogus", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			flags := defaultFlags()
			flags[enableTracing] = tc.value
			flags[authzAccessObject] = tc.value

			is.Equal(tracingEnabled(flags), tc.tracing)
			is.Equal(accessObjectEnabled(flags), tc.accessObject)
		})
	}
}
