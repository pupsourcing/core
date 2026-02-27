package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/store"
)

// DispatcherConfig configures the dispatcher polling loop.
type DispatcherConfig struct {
	// Logger is an optional logger for observability.
	Logger es.Logger

	// PollInterval controls how often the dispatcher checks for a new latest global position.
	// Default is 100ms.
	PollInterval time.Duration
}

// DefaultDispatcherConfig returns the default dispatcher configuration.
func DefaultDispatcherConfig() DispatcherConfig {
	return DispatcherConfig{
		Logger:       nil,
		PollInterval: 100 * time.Millisecond,
	}
}

// Dispatcher polls for latest global position and emits best-effort wake signals.
// It is an optimization-only component and does not participate in correctness.
type Dispatcher struct {
	db          es.DBTX
	reader      store.GlobalPositionReader
	subscribers map[int]chan struct{}
	config      DispatcherConfig

	mu sync.RWMutex

	lastSeenPosition int64
	nextSubscriberID int
}

var _ WakeupSource = (*Dispatcher)(nil)

// NewDispatcher creates a dispatcher that polls the given global position reader.
func NewDispatcher(db es.DBTX, reader store.GlobalPositionReader, config *DispatcherConfig) *Dispatcher {
	cfg := DefaultDispatcherConfig()
	if config != nil {
		cfg = *config
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = DefaultDispatcherConfig().PollInterval
	}

	return &Dispatcher{
		db:          db,
		reader:      reader,
		config:      cfg,
		subscribers: make(map[int]chan struct{}),
	}
}

// Subscribe registers for wake signals.
// Signals are lossy/coalesced (non-blocking send into a buffered channel).
func (d *Dispatcher) Subscribe() (signals <-chan struct{}, unsubscribe func()) {
	ch := make(chan struct{}, 1)

	d.mu.Lock()
	id := d.nextSubscriberID
	d.nextSubscriberID++
	d.subscribers[id] = ch
	d.mu.Unlock()

	var once sync.Once
	unsubscribe = func() {
		once.Do(func() {
			d.mu.Lock()
			delete(d.subscribers, id)
			d.mu.Unlock()
		})
	}

	return ch, unsubscribe
}

// Run starts the dispatcher loop until context cancellation.
func (d *Dispatcher) Run(ctx context.Context) error {
	if d.config.Logger != nil {
		d.config.Logger.Info(ctx, "dispatcher starting", "poll_interval", d.config.PollInterval)
	}

	if err := d.pollAndNotify(ctx); err != nil && d.config.Logger != nil {
		d.config.Logger.Error(ctx, "dispatcher poll error", "error", err)
	}

	ticker := time.NewTicker(d.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if d.config.Logger != nil {
				d.config.Logger.Info(ctx, "dispatcher stopped", "reason", ctx.Err())
			}
			return ctx.Err()
		case <-ticker.C:
			if err := d.pollAndNotify(ctx); err != nil && d.config.Logger != nil {
				d.config.Logger.Error(ctx, "dispatcher poll error", "error", err)
			}
		}
	}
}

func (d *Dispatcher) pollAndNotify(ctx context.Context) error {
	position, err := d.reader.GetLatestGlobalPosition(ctx, d.db)
	if err != nil {
		return err
	}

	d.mu.Lock()
	if position <= d.lastSeenPosition {
		d.mu.Unlock()
		return nil
	}

	d.lastSeenPosition = position
	subscribers := make([]chan struct{}, 0, len(d.subscribers))
	for _, ch := range d.subscribers {
		subscribers = append(subscribers, ch)
	}
	d.mu.Unlock()

	for _, ch := range subscribers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}

	return nil
}
