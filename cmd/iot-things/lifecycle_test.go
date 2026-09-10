package main

import (
	"context"
	"log/slog"
	"testing"
	"time"

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

type recordingApp struct {
	calls *[]string
	n     int
}

func (f *recordingApp) Stop() {
	f.n++
	*f.calls = append(*f.calls, "app")
}

// REV-006: shutdown must stop inflow, join the publisher and close
// storage exactly once, in messenger -> app -> storage order, even
// when invoked twice. messenger.Close on a real context is not safe
// to call twice, hence the guard under test.
func TestShutdownIsOrderedAndIdempotent(t *testing.T) {
	is := is.New(t)

	var order []string
	messenger := &messaging.MsgContextMock{
		ShutdownFunc: func(context.Context) error { order = append(order, "messenger"); return nil },
	}
	app := &recordingApp{calls: &order}
	storage := &recordingCloser{name: "storage", calls: &order}

	owned := &ownedResources{
		messenger: messenger,
		tracker:   &handlerTracker{},
		app:       app,
		storage:   storage,
	}

	ctx := context.Background()
	owned.close(ctx)
	owned.close(ctx)

	is.Equal(app.n, 1)
	is.Equal(storage.n, 1)
	is.Equal(order, []string{"messenger", "app", "storage"})
}

// REV-006: tracked handlers are awaited within budget; wait reports
// whether all admitted deliveries finished.
func TestHandlerTrackerWaitsForInflight(t *testing.T) {
	is := is.New(t)

	tracker := &handlerTracker{}
	release := make(chan struct{})
	handlerStarted := make(chan struct{})

	tracked := tracker.track(func(context.Context, messaging.IncomingTopicMessage, *slog.Logger) error {
		close(handlerStarted)
		<-release
		return nil
	})

	done := make(chan bool, 1)
	go func() {
		tracked(context.Background(), nil, slog.Default())
	}()

	<-handlerStarted
	go func() { done <- tracker.wait(5 * time.Second) }()

	select {
	case <-done:
		t.Fatal("wait returned while handler still blocked")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	is.True(<-done)
}

// REV-006: wait times out instead of hanging shutdown forever.
func TestHandlerTrackerWaitTimesOut(t *testing.T) {
	is := is.New(t)

	tracker := &handlerTracker{}
	tracker.wg.Add(1)
	defer tracker.wg.Done()

	is.True(!tracker.wait(20 * time.Millisecond))
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
