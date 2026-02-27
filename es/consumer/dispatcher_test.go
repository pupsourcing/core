package consumer

import (
	"context"
	"testing"

	"github.com/getpup/pupsourcing/es"
)

type fakeGlobalPositionReader struct {
	positions []int64
	index     int
}

func (f *fakeGlobalPositionReader) GetLatestGlobalPosition(_ context.Context, _ es.DBTX) (int64, error) {
	if len(f.positions) == 0 {
		return 0, nil
	}

	if f.index >= len(f.positions) {
		return f.positions[len(f.positions)-1], nil
	}

	position := f.positions[f.index]
	f.index++
	return position, nil
}

func TestDispatcher_PollAndNotify_OnlyOnAdvance(t *testing.T) {
	reader := &fakeGlobalPositionReader{positions: []int64{0, 5, 5, 9}}
	dispatcher := NewDispatcher(nil, reader, nil)

	ch, unsubscribe := dispatcher.Subscribe()
	defer unsubscribe()

	if err := dispatcher.pollAndNotify(context.Background()); err != nil {
		t.Fatalf("pollAndNotify failed: %v", err)
	}

	select {
	case <-ch:
		t.Fatal("unexpected signal on initial zero position")
	default:
	}

	if err := dispatcher.pollAndNotify(context.Background()); err != nil {
		t.Fatalf("pollAndNotify failed: %v", err)
	}

	select {
	case <-ch:
	default:
		t.Fatal("expected signal when position advanced")
	}

	if err := dispatcher.pollAndNotify(context.Background()); err != nil {
		t.Fatalf("pollAndNotify failed: %v", err)
	}

	select {
	case <-ch:
		t.Fatal("unexpected signal when position did not advance")
	default:
	}

	if err := dispatcher.pollAndNotify(context.Background()); err != nil {
		t.Fatalf("pollAndNotify failed: %v", err)
	}

	select {
	case <-ch:
	default:
		t.Fatal("expected signal on second advance")
	}
}

func TestDispatcher_PollAndNotify_LossySignals(t *testing.T) {
	reader := &fakeGlobalPositionReader{positions: []int64{1, 2, 3}}
	dispatcher := NewDispatcher(nil, reader, nil)

	ch, unsubscribe := dispatcher.Subscribe()
	defer unsubscribe()

	if err := dispatcher.pollAndNotify(context.Background()); err != nil {
		t.Fatalf("pollAndNotify failed: %v", err)
	}
	if err := dispatcher.pollAndNotify(context.Background()); err != nil {
		t.Fatalf("pollAndNotify failed: %v", err)
	}
	if err := dispatcher.pollAndNotify(context.Background()); err != nil {
		t.Fatalf("pollAndNotify failed: %v", err)
	}

	if len(ch) != 1 {
		t.Fatalf("expected coalesced signal buffer size 1, got %d", len(ch))
	}
}

func TestDispatcher_Subscribe_Unsubscribe(t *testing.T) {
	reader := &fakeGlobalPositionReader{positions: []int64{1}}
	dispatcher := NewDispatcher(nil, reader, nil)

	ch, unsubscribe := dispatcher.Subscribe()
	unsubscribe()

	if err := dispatcher.pollAndNotify(context.Background()); err != nil {
		t.Fatalf("pollAndNotify failed: %v", err)
	}

	select {
	case <-ch:
		t.Fatal("unexpected signal after unsubscribe")
	default:
	}
}
