package consumer

import (
	"testing"
	"time"
)

func TestDefaultSegmentProcessorConfig(t *testing.T) {
	config := DefaultSegmentProcessorConfig()

	// Verify segment management defaults
	if config.TotalSegments != DefaultSegments {
		t.Errorf("TotalSegments = %d, want %d", config.TotalSegments, DefaultSegments)
	}
	if config.HeartbeatInterval != 5*time.Second {
		t.Errorf("HeartbeatInterval = %v, want %v", config.HeartbeatInterval, 5*time.Second)
	}
	if config.StaleThreshold != 30*time.Second {
		t.Errorf("StaleThreshold = %v, want %v", config.StaleThreshold, 30*time.Second)
	}
	if config.RebalanceInterval != 10*time.Second {
		t.Errorf("RebalanceInterval = %v, want %v", config.RebalanceInterval, 10*time.Second)
	}

	// Verify processing defaults
	if config.BatchSize != 100 {
		t.Errorf("BatchSize = %d, want %d", config.BatchSize, 100)
	}
	if _, ok := config.PartitionStrategy.(HashPartitionStrategy); !ok {
		t.Errorf("PartitionStrategy = %T, want HashPartitionStrategy", config.PartitionStrategy)
	}
	if config.Logger != nil {
		t.Errorf("Logger = %v, want nil", config.Logger)
	}

	// Verify polling defaults
	if config.PollInterval != 100*time.Millisecond {
		t.Errorf("PollInterval = %v, want %v", config.PollInterval, 100*time.Millisecond)
	}
	if config.MaxPollInterval != 5*time.Second {
		t.Errorf("MaxPollInterval = %v, want %v", config.MaxPollInterval, 5*time.Second)
	}
	if config.MaxPostBatchPause != 100*time.Millisecond {
		t.Errorf("MaxPostBatchPause = %v, want %v", config.MaxPostBatchPause, 100*time.Millisecond)
	}
	if config.TransientErrorRetryMaxAttempts != 0 {
		t.Errorf("TransientErrorRetryMaxAttempts = %d, want 0", config.TransientErrorRetryMaxAttempts)
	}
	if config.PollBackoffFactor != 2.0 {
		t.Errorf("PollBackoffFactor = %v, want %v", config.PollBackoffFactor, 2.0)
	}

	// Verify wakeup defaults
	if config.WakeupJitter != 25*time.Millisecond {
		t.Errorf("WakeupJitter = %v, want %v", config.WakeupJitter, 25*time.Millisecond)
	}
	if config.WakeupSource != nil {
		t.Errorf("WakeupSource = %v, want nil", config.WakeupSource)
	}

	// Verify run mode default
	if config.RunMode != RunModeContinuous {
		t.Errorf("RunMode = %v, want %v", config.RunMode, RunModeContinuous)
	}
}

func TestDefaultSegmentProcessorConfig_StaleThresholdInvariant(t *testing.T) {
	config := DefaultSegmentProcessorConfig()

	// StaleThreshold must be significantly greater than HeartbeatInterval
	// to avoid false positives from network delays, clock skew, etc.
	if config.StaleThreshold <= config.HeartbeatInterval {
		t.Errorf("StaleThreshold (%v) must be greater than HeartbeatInterval (%v)",
			config.StaleThreshold, config.HeartbeatInterval)
	}

	// Verify reasonable safety margin (at least 3x)
	if config.StaleThreshold < 3*config.HeartbeatInterval {
		t.Errorf("StaleThreshold (%v) should be at least 3x HeartbeatInterval (%v) for safety margin",
			config.StaleThreshold, config.HeartbeatInterval)
	}
}

func TestDefaultSegments_Constant(t *testing.T) {
	// Verify the constant value matches documentation and expectations
	if DefaultSegments != 16 {
		t.Errorf("DefaultSegments = %d, want 16", DefaultSegments)
	}

	// Verify it's a power of 2 (important for hash distribution)
	if DefaultSegments&(DefaultSegments-1) != 0 {
		t.Errorf("DefaultSegments (%d) should be a power of 2 for optimal hash distribution", DefaultSegments)
	}
}

func TestSegmentProcessorConfig_ZeroValues(t *testing.T) {
	// Test that zero-value config doesn't panic when accessed
	var config SegmentProcessorConfig

	if config.TotalSegments != 0 {
		t.Errorf("zero-value TotalSegments = %d, want 0", config.TotalSegments)
	}
	if config.HeartbeatInterval != 0 {
		t.Errorf("zero-value HeartbeatInterval = %v, want 0", config.HeartbeatInterval)
	}
	if config.StaleThreshold != 0 {
		t.Errorf("zero-value StaleThreshold = %v, want 0", config.StaleThreshold)
	}
	if config.RebalanceInterval != 0 {
		t.Errorf("zero-value RebalanceInterval = %v, want 0", config.RebalanceInterval)
	}
	if config.BatchSize != 0 {
		t.Errorf("zero-value BatchSize = %d, want 0", config.BatchSize)
	}
	if config.PartitionStrategy != nil {
		t.Errorf("zero-value PartitionStrategy = %v, want nil", config.PartitionStrategy)
	}
	if config.Logger != nil {
		t.Errorf("zero-value Logger = %v, want nil", config.Logger)
	}
	if config.PollInterval != 0 {
		t.Errorf("zero-value PollInterval = %v, want 0", config.PollInterval)
	}
	if config.MaxPollInterval != 0 {
		t.Errorf("zero-value MaxPollInterval = %v, want 0", config.MaxPollInterval)
	}
	if config.MaxPostBatchPause != 0 {
		t.Errorf("zero-value MaxPostBatchPause = %v, want 0", config.MaxPostBatchPause)
	}
	if config.TransientErrorRetryMaxAttempts != 0 {
		t.Errorf("zero-value TransientErrorRetryMaxAttempts = %d, want 0", config.TransientErrorRetryMaxAttempts)
	}
	if config.PollBackoffFactor != 0.0 {
		t.Errorf("zero-value PollBackoffFactor = %v, want 0.0", config.PollBackoffFactor)
	}
	if config.WakeupJitter != 0 {
		t.Errorf("zero-value WakeupJitter = %v, want 0", config.WakeupJitter)
	}
	if config.WakeupSource != nil {
		t.Errorf("zero-value WakeupSource = %v, want nil", config.WakeupSource)
	}
	if config.RunMode != RunMode(0) {
		t.Errorf("zero-value RunMode = %v, want 0", config.RunMode)
	}
}

func TestSegmentProcessorConfig_CustomValues(t *testing.T) {
	// Test that we can set custom values
	config := SegmentProcessorConfig{
		TotalSegments:                  32,
		HeartbeatInterval:              10 * time.Second,
		StaleThreshold:                 60 * time.Second,
		RebalanceInterval:              20 * time.Second,
		BatchSize:                      200,
		PollInterval:                   200 * time.Millisecond,
		MaxPollInterval:                10 * time.Second,
		MaxPostBatchPause:              150 * time.Millisecond,
		TransientErrorRetryMaxAttempts: 3,
		PollBackoffFactor:              1.5,
		WakeupJitter:                   50 * time.Millisecond,
		RunMode:                        RunModeOneOff,
	}

	if config.TotalSegments != 32 {
		t.Errorf("TotalSegments = %d, want 32", config.TotalSegments)
	}
	if config.HeartbeatInterval != 10*time.Second {
		t.Errorf("HeartbeatInterval = %v, want %v", config.HeartbeatInterval, 10*time.Second)
	}
	if config.StaleThreshold != 60*time.Second {
		t.Errorf("StaleThreshold = %v, want %v", config.StaleThreshold, 60*time.Second)
	}
	if config.RebalanceInterval != 20*time.Second {
		t.Errorf("RebalanceInterval = %v, want %v", config.RebalanceInterval, 20*time.Second)
	}
	if config.BatchSize != 200 {
		t.Errorf("BatchSize = %d, want 200", config.BatchSize)
	}
	if config.PollInterval != 200*time.Millisecond {
		t.Errorf("PollInterval = %v, want %v", config.PollInterval, 200*time.Millisecond)
	}
	if config.MaxPollInterval != 10*time.Second {
		t.Errorf("MaxPollInterval = %v, want %v", config.MaxPollInterval, 10*time.Second)
	}
	if config.MaxPostBatchPause != 150*time.Millisecond {
		t.Errorf("MaxPostBatchPause = %v, want %v", config.MaxPostBatchPause, 150*time.Millisecond)
	}
	if config.TransientErrorRetryMaxAttempts != 3 {
		t.Errorf("TransientErrorRetryMaxAttempts = %d, want 3", config.TransientErrorRetryMaxAttempts)
	}
	if config.PollBackoffFactor != 1.5 {
		t.Errorf("PollBackoffFactor = %v, want 1.5", config.PollBackoffFactor)
	}
	if config.WakeupJitter != 50*time.Millisecond {
		t.Errorf("WakeupJitter = %v, want %v", config.WakeupJitter, 50*time.Millisecond)
	}
	if config.RunMode != RunModeOneOff {
		t.Errorf("RunMode = %v, want %v", config.RunMode, RunModeOneOff)
	}
}

func TestSegmentProcessorConfig_EdgeCaseIntervals(t *testing.T) {
	tests := []struct {
		name              string
		heartbeatInterval time.Duration
		staleThreshold    time.Duration
	}{
		{
			name:              "equal intervals (invalid but allowed)",
			heartbeatInterval: 5 * time.Second,
			staleThreshold:    5 * time.Second,
		},
		{
			name:              "stale threshold less than heartbeat (invalid but allowed)",
			heartbeatInterval: 10 * time.Second,
			staleThreshold:    5 * time.Second,
		},
		{
			name:              "minimal difference",
			heartbeatInterval: 5 * time.Second,
			staleThreshold:    5*time.Second + time.Millisecond,
		},
		{
			name:              "very small intervals",
			heartbeatInterval: time.Millisecond,
			staleThreshold:    2 * time.Millisecond,
		},
		{
			name:              "very large intervals",
			heartbeatInterval: 24 * time.Hour,
			staleThreshold:    72 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := SegmentProcessorConfig{
				HeartbeatInterval: tt.heartbeatInterval,
				StaleThreshold:    tt.staleThreshold,
			}

			// Verify values are set as expected
			if config.HeartbeatInterval != tt.heartbeatInterval {
				t.Errorf("HeartbeatInterval = %v, want %v", config.HeartbeatInterval, tt.heartbeatInterval)
			}
			if config.StaleThreshold != tt.staleThreshold {
				t.Errorf("StaleThreshold = %v, want %v", config.StaleThreshold, tt.staleThreshold)
			}
		})
	}
}

func TestSegmentProcessorConfig_EdgeCaseSegmentCounts(t *testing.T) {
	tests := []struct {
		name          string
		totalSegments int
	}{
		{
			name:          "single segment",
			totalSegments: 1,
		},
		{
			name:          "two segments",
			totalSegments: 2,
		},
		{
			name:          "odd number of segments",
			totalSegments: 7,
		},
		{
			name:          "large segment count",
			totalSegments: 1024,
		},
		{
			name:          "very large segment count",
			totalSegments: 65536,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := SegmentProcessorConfig{
				TotalSegments: tt.totalSegments,
			}

			if config.TotalSegments != tt.totalSegments {
				t.Errorf("TotalSegments = %d, want %d", config.TotalSegments, tt.totalSegments)
			}
		})
	}
}

func TestSegmentProcessorConfig_EdgeCaseBatchSizes(t *testing.T) {
	tests := []struct {
		name      string
		batchSize int
	}{
		{
			name:      "batch size of one",
			batchSize: 1,
		},
		{
			name:      "very small batch",
			batchSize: 5,
		},
		{
			name:      "large batch",
			batchSize: 10000,
		},
		{
			name:      "very large batch",
			batchSize: 1000000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := SegmentProcessorConfig{
				BatchSize: tt.batchSize,
			}

			if config.BatchSize != tt.batchSize {
				t.Errorf("BatchSize = %d, want %d", config.BatchSize, tt.batchSize)
			}
		})
	}
}

func TestSegmentProcessorConfig_PollBackoffBehavior(t *testing.T) {
	tests := []struct {
		name              string
		pollBackoffFactor float64
	}{
		{
			name:              "backoff disabled (zero)",
			pollBackoffFactor: 0.0,
		},
		{
			name:              "backoff disabled (one)",
			pollBackoffFactor: 1.0,
		},
		{
			name:              "slow backoff",
			pollBackoffFactor: 1.1,
		},
		{
			name:              "aggressive backoff",
			pollBackoffFactor: 10.0,
		},
		{
			name:              "negative backoff (invalid but allowed)",
			pollBackoffFactor: -1.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := SegmentProcessorConfig{
				PollBackoffFactor: tt.pollBackoffFactor,
			}

			if config.PollBackoffFactor != tt.pollBackoffFactor {
				t.Errorf("PollBackoffFactor = %v, want %v", config.PollBackoffFactor, tt.pollBackoffFactor)
			}
		})
	}
}

func TestSegmentProcessorConfig_RunModes(t *testing.T) {
	tests := []struct {
		name    string
		runMode RunMode
		want    RunMode
	}{
		{
			name:    "continuous mode",
			runMode: RunModeContinuous,
			want:    RunModeContinuous,
		},
		{
			name:    "one-off mode",
			runMode: RunModeOneOff,
			want:    RunModeOneOff,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := SegmentProcessorConfig{
				RunMode: tt.runMode,
			}

			if config.RunMode != tt.want {
				t.Errorf("RunMode = %v, want %v", config.RunMode, tt.want)
			}
		})
	}
}
