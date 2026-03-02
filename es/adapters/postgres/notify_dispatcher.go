package postgres

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lib/pq"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/consumer"
)

// NotifyDispatcherConfig configures the LISTEN/NOTIFY-based dispatcher.
type NotifyDispatcherConfig struct {
	// Logger is an optional logger for observability.
	Logger es.Logger

	// Channel is the Postgres NOTIFY channel to listen on.
	// Must match the channel configured via WithNotifyChannel on the Store.
	// Default: "pupsourcing_events".
	Channel string

	// FallbackInterval is the maximum time to wait without a notification
	// before performing a fallback wake signal. This guards against missed
	// notifications due to connection drops or other transient issues.
	// Default: 30s.
	FallbackInterval time.Duration

	// MinReconnectInterval is the minimum delay before reconnecting after a listener error.
	// Default: 5s.
	MinReconnectInterval time.Duration

	// MaxReconnectInterval is the maximum delay before reconnecting after repeated errors.
	// Default: 60s.
	MaxReconnectInterval time.Duration
}

func defaultNotifyDispatcherConfig() NotifyDispatcherConfig {
	return NotifyDispatcherConfig{
		Channel:              "pupsourcing_events",
		FallbackInterval:     30 * time.Second,
		MinReconnectInterval: 5 * time.Second,
		MaxReconnectInterval: 60 * time.Second,
	}
}

// NotifyDispatcher implements consumer.WakeupSource using Postgres LISTEN/NOTIFY.
// It listens on a Postgres channel for event append notifications and fans out
// wake signals to subscribed segment processors. A fallback timer ensures
// segments still wake up even if the LISTEN connection drops temporarily.
type NotifyDispatcher struct {
	subscribers      map[int]chan struct{}
	connStr          string
	config           NotifyDispatcherConfig
	nextSubscriberID int
	mu               sync.RWMutex
}

var _ consumer.WakeupSource = (*NotifyDispatcher)(nil)

// NewNotifyDispatcher creates a dispatcher that uses Postgres LISTEN/NOTIFY
// for instant event notifications. The connStr must be a valid Postgres
// connection string (e.g., "postgres://user:pass@host/db?sslmode=disable").
func NewNotifyDispatcher(connStr string, config *NotifyDispatcherConfig) *NotifyDispatcher {
	cfg := defaultNotifyDispatcherConfig()
	if config != nil {
		if config.Channel != "" {
			cfg.Channel = config.Channel
		}
		if config.FallbackInterval > 0 {
			cfg.FallbackInterval = config.FallbackInterval
		}
		if config.MinReconnectInterval > 0 {
			cfg.MinReconnectInterval = config.MinReconnectInterval
		}
		if config.MaxReconnectInterval > 0 {
			cfg.MaxReconnectInterval = config.MaxReconnectInterval
		}
		if config.Logger != nil {
			cfg.Logger = config.Logger
		}
	}

	return &NotifyDispatcher{
		connStr:     connStr,
		config:      cfg,
		subscribers: make(map[int]chan struct{}),
	}
}

// Subscribe registers for wake signals.
// Signals are lossy/coalesced (non-blocking send into a buffered channel).
func (d *NotifyDispatcher) Subscribe() (signals <-chan struct{}, unsubscribe func()) {
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

// Run starts the LISTEN/NOTIFY loop until context cancellation.
// This must be called in a goroutine. The Worker starts it automatically
// when a NotifyDispatcher is provided via WithWakeupSource.
func (d *NotifyDispatcher) Run(ctx context.Context) error {
	if d.config.Logger != nil {
		d.config.Logger.Info(ctx, "notify dispatcher starting",
			"channel", d.config.Channel,
			"fallback_interval", d.config.FallbackInterval)
	}

	listener := pq.NewListener(
		d.connStr,
		d.config.MinReconnectInterval,
		d.config.MaxReconnectInterval,
		d.listenerEventCallback(ctx),
	)
	defer listener.Close()

	if err := listener.Listen(d.config.Channel); err != nil {
		return fmt.Errorf("failed to listen on channel %q: %w", d.config.Channel, err)
	}

	if d.config.Logger != nil {
		d.config.Logger.Info(ctx, "notify dispatcher listening", "channel", d.config.Channel)
	}

	return d.listenLoop(ctx, listener)
}

// listenerEventCallback returns a callback for pq.Listener connection events.
func (d *NotifyDispatcher) listenerEventCallback(ctx context.Context) func(pq.ListenerEventType, error) {
	return func(ev pq.ListenerEventType, err error) {
		if d.config.Logger == nil {
			return
		}
		switch ev {
		case pq.ListenerEventConnected:
			d.config.Logger.Info(ctx, "notify dispatcher connected")
		case pq.ListenerEventDisconnected:
			d.config.Logger.Error(ctx, "notify dispatcher disconnected", "error", err)
		case pq.ListenerEventReconnected:
			d.config.Logger.Info(ctx, "notify dispatcher reconnected")
		case pq.ListenerEventConnectionAttemptFailed:
			d.config.Logger.Error(ctx, "notify dispatcher reconnect failed", "error", err)
		}
	}
}

// listenLoop processes notifications and fallback timer events.
func (d *NotifyDispatcher) listenLoop(ctx context.Context, listener *pq.Listener) error {
	fallbackTimer := time.NewTimer(d.config.FallbackInterval)
	defer fallbackTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			if d.config.Logger != nil {
				d.config.Logger.Info(ctx, "notify dispatcher stopped", "reason", ctx.Err())
			}
			return ctx.Err()

		case notification := <-listener.Notify:
			d.handleNotification(ctx, notification)
			resetTimer(fallbackTimer, d.config.FallbackInterval)

		case <-fallbackTimer.C:
			if d.config.Logger != nil {
				d.config.Logger.Debug(ctx, "notify dispatcher fallback wake")
			}
			d.broadcast()
			fallbackTimer.Reset(d.config.FallbackInterval)
		}
	}
}

// handleNotification processes a single notification from the listener.
func (d *NotifyDispatcher) handleNotification(ctx context.Context, notification *pq.Notification) {
	if notification == nil {
		// nil notification means the connection was lost and re-established.
		if d.config.Logger != nil {
			d.config.Logger.Info(ctx, "notify dispatcher reconnect wake")
		}
		d.broadcast()
		return
	}

	if d.config.Logger != nil {
		d.config.Logger.Debug(ctx, "notify dispatcher received",
			"channel", notification.Channel,
			"payload", notification.Extra)
	}
	d.broadcast()
}

// resetTimer safely resets a timer, draining its channel if needed.
func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// broadcast sends a non-blocking wake signal to all subscribers.
func (d *NotifyDispatcher) broadcast() {
	d.mu.RLock()
	subscribers := make([]chan struct{}, 0, len(d.subscribers))
	for _, ch := range d.subscribers {
		subscribers = append(subscribers, ch)
	}
	d.mu.RUnlock()

	for _, ch := range subscribers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
