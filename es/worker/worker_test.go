package worker

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/consumer"
	"github.com/pupsourcing/core/es/store"
)

// mockLogger is a simple mock implementation of es.Logger for testing.
type mockLogger struct {
	debugCalls int
	infoCalls  int
	errorCalls int
}

func (m *mockLogger) Debug(_ context.Context, _ string, _ ...interface{}) {
	m.debugCalls++
}

func (m *mockLogger) Info(_ context.Context, _ string, _ ...interface{}) {
	m.infoCalls++
}

func (m *mockLogger) Error(_ context.Context, _ string, _ ...interface{}) {
	m.errorCalls++
}

// mockPartitionStrategy is a mock implementation of consumer.PartitionStrategy for testing.
type mockPartitionStrategy struct {
	called bool
}

func (m *mockPartitionStrategy) ShouldProcess(_ string, _, _ int) bool {
	m.called = true
	return true
}

type mockWakeupSource struct{}

func (m *mockWakeupSource) Subscribe() (signals <-chan struct{}, unsubscribe func()) {
	return make(chan struct{}, 1), func() {}
}

type noopConsumer struct {
	name string
}

func (c *noopConsumer) Name() string {
	return c.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to match Consumer interface
func (c *noopConsumer) Handle(_ context.Context, _ *sql.Tx, _ es.PersistedEvent) error {
	return nil
}

type blockingProcessor struct{}

func (p *blockingProcessor) Run(ctx context.Context, _ consumer.Consumer) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestDefaultConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()

	// Verify all default values
	tests := map[string]struct {
		got  interface{}
		want interface{}
	}{
		"TotalSegments": {
			got:  cfg.TotalSegments,
			want: 16,
		},
		"HeartbeatInterval": {
			got:  cfg.HeartbeatInterval,
			want: 5 * time.Second,
		},
		"StaleThreshold": {
			got:  cfg.StaleThreshold,
			want: 30 * time.Second,
		},
		"RebalanceInterval": {
			got:  cfg.RebalanceInterval,
			want: 10 * time.Second,
		},
		"BatchSize": {
			got:  cfg.BatchSize,
			want: 100,
		},
		"PollInterval": {
			got:  cfg.PollInterval,
			want: 100 * time.Millisecond,
		},
		"MaxPollInterval": {
			got:  cfg.MaxPollInterval,
			want: 5 * time.Second,
		},
		"MaxPostBatchPause": {
			got:  cfg.MaxPostBatchPause,
			want: 100 * time.Millisecond,
		},
		"TransientErrorRetryMaxAttempts": {
			got:  cfg.TransientErrorRetryMaxAttempts,
			want: 0,
		},
		"HealthAuditInterval": {
			got:  cfg.HealthAuditInterval,
			want: time.Duration(0),
		},
		"PollBackoffFactor": {
			got:  cfg.PollBackoffFactor,
			want: 2.0,
		},
		"WakeupJitter": {
			got:  cfg.WakeupJitter,
			want: 25 * time.Millisecond,
		},
		"EnableDispatcher": {
			got:  cfg.EnableDispatcher,
			want: true,
		},
		"DispatcherPollInterval": {
			got:  cfg.DispatcherPollInterval,
			want: 200 * time.Millisecond,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("got %v, want %v", tc.got, tc.want)
			}
		})
	}

	// PartitionStrategy should be non-nil and specifically HashPartitionStrategy
	t.Run("PartitionStrategy is not nil", func(t *testing.T) {
		if cfg.PartitionStrategy == nil {
			t.Error("PartitionStrategy should not be nil")
		}
	})

	t.Run("PartitionStrategy is HashPartitionStrategy", func(t *testing.T) {
		if _, ok := cfg.PartitionStrategy.(consumer.HashPartitionStrategy); !ok {
			t.Errorf("PartitionStrategy should be HashPartitionStrategy, got %T", cfg.PartitionStrategy)
		}
	})

	// Logger should be nil by default
	t.Run("Logger is nil", func(t *testing.T) {
		if cfg.Logger != nil {
			t.Error("Logger should be nil by default")
		}
	})

	// WakeupSource should be nil by default
	t.Run("WakeupSource is nil", func(t *testing.T) {
		if cfg.WakeupSource != nil {
			t.Error("WakeupSource should be nil by default")
		}
	})
}

func TestOptions(t *testing.T) {
	t.Parallel()

	logger := &mockLogger{}
	strategy := &mockPartitionStrategy{}

	tests := map[string]struct {
		option    Option
		checkFunc func(*testing.T, *Config)
	}{
		"WithTotalSegments": {
			option: WithTotalSegments(32),
			checkFunc: func(t *testing.T, c *Config) {
				t.Helper()
				if c.TotalSegments != 32 {
					t.Errorf("TotalSegments: got %d, want 32", c.TotalSegments)
				}
			},
		},
		"WithHeartbeatInterval": {
			option: WithHeartbeatInterval(3 * time.Second),
			checkFunc: func(t *testing.T, c *Config) {
				t.Helper()
				if c.HeartbeatInterval != 3*time.Second {
					t.Errorf("HeartbeatInterval: got %v, want 3s", c.HeartbeatInterval)
				}
			},
		},
		"WithStaleThreshold": {
			option: WithStaleThreshold(15 * time.Second),
			checkFunc: func(t *testing.T, c *Config) {
				t.Helper()
				if c.StaleThreshold != 15*time.Second {
					t.Errorf("StaleThreshold: got %v, want 15s", c.StaleThreshold)
				}
			},
		},
		"WithRebalanceInterval": {
			option: WithRebalanceInterval(5 * time.Second),
			checkFunc: func(t *testing.T, c *Config) {
				t.Helper()
				if c.RebalanceInterval != 5*time.Second {
					t.Errorf("RebalanceInterval: got %v, want 5s", c.RebalanceInterval)
				}
			},
		},
		"WithBatchSize": {
			option: WithBatchSize(200),
			checkFunc: func(t *testing.T, c *Config) {
				t.Helper()
				if c.BatchSize != 200 {
					t.Errorf("BatchSize: got %d, want 200", c.BatchSize)
				}
			},
		},
		"WithPollInterval": {
			option: WithPollInterval(50 * time.Millisecond),
			checkFunc: func(t *testing.T, c *Config) {
				t.Helper()
				if c.PollInterval != 50*time.Millisecond {
					t.Errorf("PollInterval: got %v, want 50ms", c.PollInterval)
				}
			},
		},
		"WithMaxPollInterval": {
			option: WithMaxPollInterval(2 * time.Second),
			checkFunc: func(t *testing.T, c *Config) {
				t.Helper()
				if c.MaxPollInterval != 2*time.Second {
					t.Errorf("MaxPollInterval: got %v, want 2s", c.MaxPollInterval)
				}
			},
		},
		"WithMaxPostBatchPause": {
			option: WithMaxPostBatchPause(250 * time.Millisecond),
			checkFunc: func(t *testing.T, c *Config) {
				t.Helper()
				if c.MaxPostBatchPause != 250*time.Millisecond {
					t.Errorf("MaxPostBatchPause: got %v, want 250ms", c.MaxPostBatchPause)
				}
			},
		},
		"WithTransientErrorRetry": {
			option: WithTransientErrorRetry(3),
			checkFunc: func(t *testing.T, c *Config) {
				t.Helper()
				if c.TransientErrorRetryMaxAttempts != 3 {
					t.Errorf("TransientErrorRetryMaxAttempts: got %d, want 3", c.TransientErrorRetryMaxAttempts)
				}
			},
		},
		"WithHealthAuditInterval": {
			option: WithHealthAuditInterval(7 * time.Second),
			checkFunc: func(t *testing.T, c *Config) {
				t.Helper()
				if c.HealthAuditInterval != 7*time.Second {
					t.Errorf("HealthAuditInterval: got %v, want 7s", c.HealthAuditInterval)
				}
			},
		},
		"WithPollBackoffFactor": {
			option: WithPollBackoffFactor(1.5),
			checkFunc: func(t *testing.T, c *Config) {
				t.Helper()
				if c.PollBackoffFactor != 1.5 {
					t.Errorf("PollBackoffFactor: got %v, want 1.5", c.PollBackoffFactor)
				}
			},
		},
		"WithWakeupJitter": {
			option: WithWakeupJitter(10 * time.Millisecond),
			checkFunc: func(t *testing.T, c *Config) {
				t.Helper()
				if c.WakeupJitter != 10*time.Millisecond {
					t.Errorf("WakeupJitter: got %v, want 10ms", c.WakeupJitter)
				}
			},
		},
		"WithDispatcher disabled": {
			option: WithDispatcher(false),
			checkFunc: func(t *testing.T, c *Config) {
				t.Helper()
				if c.EnableDispatcher != false {
					t.Errorf("EnableDispatcher: got %v, want false", c.EnableDispatcher)
				}
			},
		},
		"WithDispatcher enabled": {
			option: WithDispatcher(true),
			checkFunc: func(t *testing.T, c *Config) {
				t.Helper()
				if c.EnableDispatcher != true {
					t.Errorf("EnableDispatcher: got %v, want true", c.EnableDispatcher)
				}
			},
		},
		"WithDispatcherPollInterval": {
			option: WithDispatcherPollInterval(500 * time.Millisecond),
			checkFunc: func(t *testing.T, c *Config) {
				t.Helper()
				if c.DispatcherPollInterval != 500*time.Millisecond {
					t.Errorf("DispatcherPollInterval: got %v, want 500ms", c.DispatcherPollInterval)
				}
			},
		},
		"WithLogger": {
			option: WithLogger(logger),
			checkFunc: func(t *testing.T, c *Config) {
				t.Helper()
				if c.Logger == nil {
					t.Error("Logger should not be nil after WithLogger")
				}
				if c.Logger != logger {
					t.Errorf("Logger: got %v, want %v", c.Logger, logger)
				}
			},
		},
		"WithPartitionStrategy": {
			option: WithPartitionStrategy(strategy),
			checkFunc: func(t *testing.T, c *Config) {
				t.Helper()
				if c.PartitionStrategy == nil {
					t.Error("PartitionStrategy should not be nil after WithPartitionStrategy")
				}
				// Verify it's the same instance by checking if it's our mock
				if _, ok := c.PartitionStrategy.(*mockPartitionStrategy); !ok {
					t.Errorf("PartitionStrategy should be *mockPartitionStrategy, got %T", c.PartitionStrategy)
				}
			},
		},
		"WithWakeupSource": {
			option: WithWakeupSource(&mockWakeupSource{}),
			checkFunc: func(t *testing.T, c *Config) {
				t.Helper()
				if c.WakeupSource == nil {
					t.Error("WakeupSource should not be nil after WithWakeupSource")
				}
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.option(cfg)
			tc.checkFunc(t, cfg)
		})
	}
}

func TestOptionsCompose(t *testing.T) {
	t.Parallel()

	logger := &mockLogger{}
	strategy := &mockPartitionStrategy{}

	cfg := DefaultConfig()

	// Apply multiple options
	options := []Option{
		WithTotalSegments(32),
		WithHeartbeatInterval(3 * time.Second),
		WithStaleThreshold(15 * time.Second),
		WithRebalanceInterval(5 * time.Second),
		WithBatchSize(200),
		WithPollInterval(50 * time.Millisecond),
		WithMaxPollInterval(2 * time.Second),
		WithMaxPostBatchPause(250 * time.Millisecond),
		WithTransientErrorRetry(3),
		WithHealthAuditInterval(7 * time.Second),
		WithPollBackoffFactor(1.5),
		WithWakeupJitter(10 * time.Millisecond),
		WithDispatcher(false),
		WithDispatcherPollInterval(500 * time.Millisecond),
		WithLogger(logger),
		WithPartitionStrategy(strategy),
		WithWakeupSource(&mockWakeupSource{}),
	}

	for _, opt := range options {
		opt(cfg)
	}

	// Verify all options took effect and didn't interfere with each other
	checks := map[string]struct {
		got  interface{}
		want interface{}
	}{
		"TotalSegments": {
			got:  cfg.TotalSegments,
			want: 32,
		},
		"HeartbeatInterval": {
			got:  cfg.HeartbeatInterval,
			want: 3 * time.Second,
		},
		"StaleThreshold": {
			got:  cfg.StaleThreshold,
			want: 15 * time.Second,
		},
		"RebalanceInterval": {
			got:  cfg.RebalanceInterval,
			want: 5 * time.Second,
		},
		"BatchSize": {
			got:  cfg.BatchSize,
			want: 200,
		},
		"PollInterval": {
			got:  cfg.PollInterval,
			want: 50 * time.Millisecond,
		},
		"MaxPollInterval": {
			got:  cfg.MaxPollInterval,
			want: 2 * time.Second,
		},
		"MaxPostBatchPause": {
			got:  cfg.MaxPostBatchPause,
			want: 250 * time.Millisecond,
		},
		"TransientErrorRetryMaxAttempts": {
			got:  cfg.TransientErrorRetryMaxAttempts,
			want: 3,
		},
		"HealthAuditInterval": {
			got:  cfg.HealthAuditInterval,
			want: 7 * time.Second,
		},
		"PollBackoffFactor": {
			got:  cfg.PollBackoffFactor,
			want: 1.5,
		},
		"WakeupJitter": {
			got:  cfg.WakeupJitter,
			want: 10 * time.Millisecond,
		},
		"EnableDispatcher": {
			got:  cfg.EnableDispatcher,
			want: false,
		},
		"DispatcherPollInterval": {
			got:  cfg.DispatcherPollInterval,
			want: 500 * time.Millisecond,
		},
	}

	for name, tc := range checks {
		t.Run(name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("got %v, want %v", tc.got, tc.want)
			}
		})
	}

	t.Run("Logger", func(t *testing.T) {
		if cfg.Logger != logger {
			t.Errorf("Logger: got %v, want %v", cfg.Logger, logger)
		}
	})

	t.Run("PartitionStrategy", func(t *testing.T) {
		if cfg.PartitionStrategy == nil {
			t.Error("PartitionStrategy should not be nil")
		}
		// Verify it's the mock we set
		if _, ok := cfg.PartitionStrategy.(*mockPartitionStrategy); !ok {
			t.Errorf("PartitionStrategy should be *mockPartitionStrategy, got %T", cfg.PartitionStrategy)
		}
	})

	t.Run("WakeupSource", func(t *testing.T) {
		if cfg.WakeupSource == nil {
			t.Error("WakeupSource should not be nil")
		}
	})
}

func TestRunNoConsumers(t *testing.T) {
	t.Parallel()

	// Create a worker with minimal setup
	factory := func(_ *consumer.SegmentProcessorConfig) consumer.ProcessorRunner {
		return nil // Not used in this test
	}

	w := New(nil, nil, factory)

	ctx := context.Background()
	err := w.Run(ctx)

	if err == nil {
		t.Fatal("expected error when calling Run with no consumers, got nil")
	}

	expectedMsg := "at least one consumer"
	if !containsString(err.Error(), expectedMsg) {
		t.Errorf("error message should contain %q, got: %v", expectedMsg, err)
	}
}

func TestRunPropagatesMaxPostBatchPause(t *testing.T) {
	t.Parallel()

	var capturedCfg *consumer.SegmentProcessorConfig
	factory := func(cfg *consumer.SegmentProcessorConfig) consumer.ProcessorRunner {
		cfgCopy := *cfg
		capturedCfg = &cfgCopy
		return &blockingProcessor{}
	}

	w := New(nil, nil, factory,
		WithDispatcher(false),
		WithMaxPostBatchPause(250*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := w.Run(ctx, &noopConsumer{name: "config-propagation-test"})
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}

	if capturedCfg == nil {
		t.Fatal("expected segment processor config to be captured")
	}
	if capturedCfg.MaxPostBatchPause != 250*time.Millisecond {
		t.Errorf("MaxPostBatchPause: got %v, want 250ms", capturedCfg.MaxPostBatchPause)
	}
}

func TestRunPropagatesTransientErrorRetry(t *testing.T) {
	t.Parallel()

	var capturedCfg *consumer.SegmentProcessorConfig
	factory := func(cfg *consumer.SegmentProcessorConfig) consumer.ProcessorRunner {
		cfgCopy := *cfg
		capturedCfg = &cfgCopy
		return &blockingProcessor{}
	}

	w := New(nil, nil, factory,
		WithDispatcher(false),
		WithTransientErrorRetry(4),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := w.Run(ctx, &noopConsumer{name: "retry-config-propagation-test"})
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}

	if capturedCfg == nil {
		t.Fatal("expected segment processor config to be captured")
	}
	if capturedCfg.TransientErrorRetryMaxAttempts != 4 {
		t.Errorf("TransientErrorRetryMaxAttempts: got %d, want 4", capturedCfg.TransientErrorRetryMaxAttempts)
	}
}

func TestRunPropagatesHealthAuditInterval(t *testing.T) {
	t.Parallel()

	var capturedCfg *consumer.SegmentProcessorConfig
	factory := func(cfg *consumer.SegmentProcessorConfig) consumer.ProcessorRunner {
		cfgCopy := *cfg
		capturedCfg = &cfgCopy
		return &blockingProcessor{}
	}

	w := New(nil, nil, factory,
		WithDispatcher(false),
		WithHealthAuditInterval(9*time.Second),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := w.Run(ctx, &noopConsumer{name: "health-audit-config-propagation-test"})
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}

	if capturedCfg == nil {
		t.Fatal("expected segment processor config to be captured")
	}
	if capturedCfg.HealthAuditInterval != 9*time.Second {
		t.Errorf("HealthAuditInterval: got %v, want 9s", capturedCfg.HealthAuditInterval)
	}
}

func TestNew(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		db        *sql.DB
		posReader store.GlobalPositionReader
		factory   ProcessorFactory
		opts      []Option
		validate  func(*testing.T, *Worker)
	}{
		"with no options": {
			db:        &sql.DB{},
			posReader: nil,
			factory: func(_ *consumer.SegmentProcessorConfig) consumer.ProcessorRunner {
				return nil
			},
			opts: nil,
			validate: func(t *testing.T, w *Worker) {
				t.Helper()
				if w == nil {
					t.Fatal("New should return non-nil Worker")
				}
				if w.config == nil {
					t.Fatal("Worker config should not be nil")
				}
				// Verify defaults are applied
				if w.config.TotalSegments != 16 {
					t.Errorf("TotalSegments: got %d, want 16", w.config.TotalSegments)
				}
				if w.config.EnableDispatcher != true {
					t.Errorf("EnableDispatcher: got %v, want true", w.config.EnableDispatcher)
				}
			},
		},
		"with options": {
			db:        &sql.DB{},
			posReader: nil,
			factory: func(_ *consumer.SegmentProcessorConfig) consumer.ProcessorRunner {
				return nil
			},
			opts: []Option{
				WithTotalSegments(32),
				WithBatchSize(500),
			},
			validate: func(t *testing.T, w *Worker) {
				t.Helper()
				if w == nil {
					t.Fatal("New should return non-nil Worker")
				}
				if w.config == nil {
					t.Fatal("Worker config should not be nil")
				}
				// Verify options were applied
				if w.config.TotalSegments != 32 {
					t.Errorf("TotalSegments: got %d, want 32", w.config.TotalSegments)
				}
				if w.config.BatchSize != 500 {
					t.Errorf("BatchSize: got %d, want 500", w.config.BatchSize)
				}
				// Verify other defaults are preserved
				if w.config.EnableDispatcher != true {
					t.Errorf("EnableDispatcher: got %v, want true", w.config.EnableDispatcher)
				}
			},
		},
		"with all options": {
			db:        &sql.DB{},
			posReader: nil,
			factory: func(_ *consumer.SegmentProcessorConfig) consumer.ProcessorRunner {
				return nil
			},
			opts: []Option{
				WithTotalSegments(8),
				WithHeartbeatInterval(2 * time.Second),
				WithStaleThreshold(20 * time.Second),
				WithRebalanceInterval(7 * time.Second),
				WithBatchSize(150),
				WithPollInterval(75 * time.Millisecond),
				WithMaxPollInterval(3 * time.Second),
				WithMaxPostBatchPause(300 * time.Millisecond),
				WithTransientErrorRetry(5),
				WithHealthAuditInterval(12 * time.Second),
				WithPollBackoffFactor(1.8),
				WithWakeupJitter(15 * time.Millisecond),
				WithDispatcher(false),
				WithDispatcherPollInterval(300 * time.Millisecond),
				WithLogger(&mockLogger{}),
				WithPartitionStrategy(&mockPartitionStrategy{}),
				WithWakeupSource(&mockWakeupSource{}),
			},
			validate: func(t *testing.T, w *Worker) {
				t.Helper()
				if w == nil {
					t.Fatal("New should return non-nil Worker")
				}
				if w.config == nil {
					t.Fatal("Worker config should not be nil")
				}
				// Verify all custom options were applied
				if w.config.TotalSegments != 8 {
					t.Errorf("TotalSegments: got %d, want 8", w.config.TotalSegments)
				}
				if w.config.HeartbeatInterval != 2*time.Second {
					t.Errorf("HeartbeatInterval: got %v, want 2s", w.config.HeartbeatInterval)
				}
				if w.config.StaleThreshold != 20*time.Second {
					t.Errorf("StaleThreshold: got %v, want 20s", w.config.StaleThreshold)
				}
				if w.config.RebalanceInterval != 7*time.Second {
					t.Errorf("RebalanceInterval: got %v, want 7s", w.config.RebalanceInterval)
				}
				if w.config.BatchSize != 150 {
					t.Errorf("BatchSize: got %d, want 150", w.config.BatchSize)
				}
				if w.config.PollInterval != 75*time.Millisecond {
					t.Errorf("PollInterval: got %v, want 75ms", w.config.PollInterval)
				}
				if w.config.MaxPollInterval != 3*time.Second {
					t.Errorf("MaxPollInterval: got %v, want 3s", w.config.MaxPollInterval)
				}
				if w.config.MaxPostBatchPause != 300*time.Millisecond {
					t.Errorf("MaxPostBatchPause: got %v, want 300ms", w.config.MaxPostBatchPause)
				}
				if w.config.TransientErrorRetryMaxAttempts != 5 {
					t.Errorf("TransientErrorRetryMaxAttempts: got %d, want 5", w.config.TransientErrorRetryMaxAttempts)
				}
				if w.config.HealthAuditInterval != 12*time.Second {
					t.Errorf("HealthAuditInterval: got %v, want 12s", w.config.HealthAuditInterval)
				}
				if w.config.PollBackoffFactor != 1.8 {
					t.Errorf("PollBackoffFactor: got %v, want 1.8", w.config.PollBackoffFactor)
				}
				if w.config.WakeupJitter != 15*time.Millisecond {
					t.Errorf("WakeupJitter: got %v, want 15ms", w.config.WakeupJitter)
				}
				if w.config.EnableDispatcher != false {
					t.Errorf("EnableDispatcher: got %v, want false", w.config.EnableDispatcher)
				}
				if w.config.DispatcherPollInterval != 300*time.Millisecond {
					t.Errorf("DispatcherPollInterval: got %v, want 300ms", w.config.DispatcherPollInterval)
				}
				if w.config.Logger == nil {
					t.Error("Logger should not be nil")
				}
				if w.config.PartitionStrategy == nil {
					t.Error("PartitionStrategy should not be nil")
				}
				if w.config.WakeupSource == nil {
					t.Error("WakeupSource should not be nil")
				}
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			w := New(tc.db, tc.posReader, tc.factory, tc.opts...)
			tc.validate(t, w)
		})
	}
}

// containsString checks if s contains substr.
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || substr == "" || indexString(s, substr) >= 0)
}

// indexString returns the index of substr in s, or -1 if not found.
func indexString(s, substr string) int {
	n := len(substr)
	if n == 0 {
		return 0
	}
	for i := 0; i+n <= len(s); i++ {
		if s[i:i+n] == substr {
			return i
		}
	}
	return -1
}
