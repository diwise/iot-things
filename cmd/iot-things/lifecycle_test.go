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

// BASE-006: shutdown must close owned resources exactly once, in
// messenger -> cancel -> storage order, even when invoked twice.
// messenger.Close on a real context is not safe to call twice, hence
// the guard under test.
func TestShutdownIsOrderedAndIdempotent(t *testing.T) {
	is := is.New(t)

	var order []string
	messenger := &messaging.MsgContextMock{
		CloseFunc: func() { order = append(order, "messenger") },
	}
	cancels := 0
	storage := &recordingCloser{name: "storage", calls: &order}

	owned := &ownedResources{
		messenger: messenger,
		cancel: func() {
			cancels++
			order = append(order, "cancel")
		},
		storage: storage,
	}

	ctx := context.Background()
	owned.close(ctx)
	owned.close(ctx)

	is.Equal(cancels, 1)
	is.Equal(storage.n, 1)
	is.Equal(order, []string{"messenger", "cancel", "storage"})
}

// BASE-006: shutdown with no initialized resources (e.g. failed OnInit)
// must be a safe no-op.
func TestShutdownWithoutResourcesIsSafe(t *testing.T) {
	owned := &ownedResources{}

	owned.close(context.Background())
	owned.close(context.Background())
}
