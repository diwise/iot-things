package main

import (
	"context"
	"testing"

	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

type recordingCloser struct {
	name  string
	calls *[]string
	n     int
}

func (f *recordingCloser) Close() {
	f.n++
	*f.calls = append(*f.calls, f.name)
}

// Shutdown ska ge messenger en egen deadline så att en blockerad leverans
// inte håller nedstängningen obegränsat.
func TestShutdownSuppliesMessengerDeadline(t *testing.T) {
	is := is.New(t)

	var hasDeadline bool
	messenger := &messaging.MsgContextMock{
		ShutdownFunc: func(ctx context.Context) error {
			_, hasDeadline = ctx.Deadline()
			return nil
		},
	}

	owned := &ownedResources{messenger: messenger}
	owned.close(context.Background())

	is.True(hasDeadline)
}

// Shutdown must stop inflow (messenger) and then close storage exactly
// once, in that order, even when invoked twice. The messaging library
// drains in-flight deliveries on Shutdown, so no separate handler tracker
// is needed.
func TestShutdownIsOrderedAndIdempotent(t *testing.T) {
	is := is.New(t)

	var order []string
	messenger := &messaging.MsgContextMock{
		ShutdownFunc: func(context.Context) error { order = append(order, "messenger"); return nil },
	}
	storage := &recordingCloser{name: "storage", calls: &order}

	owned := &ownedResources{
		messenger: messenger,
		storage:   storage,
	}

	ctx := context.Background()
	owned.close(ctx)
	owned.close(ctx)

	is.Equal(storage.n, 1)
	is.Equal(order, []string{"messenger", "storage"})
}

// BASE-006: shutdown with no initialized resources (e.g. failed OnInit)
// must be a safe no-op.
func TestShutdownWithoutResourcesIsSafe(t *testing.T) {
	owned := &ownedResources{}

	owned.close(context.Background())
	owned.close(context.Background())
}

// BASE-007: readiness stubs always report OK without touching any
// dependency.
func TestReadinessStubsAlwaysOK(t *testing.T) {
	is := is.New(t)

	probes := readinessProbes()
	is.Equal(len(probes), 2)

	for _, name := range []string{"rabbitmq", "timescale"} {
		status, err := probes[name](context.Background())
		is.NoErr(err)
		is.Equal(status, "ok")
	}
}
