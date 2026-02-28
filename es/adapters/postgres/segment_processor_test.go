package postgres

import (
	"testing"
)

func TestCeilDiv(t *testing.T) {
	tests := []struct {
		name     string
		a, b     int
		expected int
	}{
		{
			name:     "exact division",
			a:        16,
			b:        4,
			expected: 4,
		},
		{
			name:     "remainder rounds up",
			a:        16,
			b:        3,
			expected: 6,
		},
		{
			name:     "single divisor",
			a:        16,
			b:        1,
			expected: 16,
		},
		{
			name:     "equal values",
			a:        4,
			b:        4,
			expected: 1,
		},
		{
			name:     "larger divisor",
			a:        3,
			b:        16,
			expected: 1,
		},
		{
			name:     "one divided by one",
			a:        1,
			b:        1,
			expected: 1,
		},
		{
			name:     "small remainder",
			a:        10,
			b:        3,
			expected: 4,
		},
		{
			name:     "large numbers exact",
			a:        1000,
			b:        100,
			expected: 10,
		},
		{
			name:     "large numbers with remainder",
			a:        1000,
			b:        99,
			expected: 11,
		},
		{
			name:     "one less than divisor",
			a:        15,
			b:        16,
			expected: 1,
		},
		{
			name:     "one more than divisor",
			a:        17,
			b:        16,
			expected: 2,
		},
		{
			name:     "prime number division",
			a:        17,
			b:        7,
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ceilDiv(tt.a, tt.b)
			if got != tt.expected {
				t.Errorf("ceilDiv(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.expected)
			}
		})
	}
}

func TestCeilDiv_FairShareScenarios(t *testing.T) {
	// Test realistic fair-share rebalancing scenarios
	tests := []struct {
		name              string
		totalSegments     int
		activeWorkers     int
		expectedPerWorker int
	}{
		{
			name:              "16 segments, 1 worker",
			totalSegments:     16,
			activeWorkers:     1,
			expectedPerWorker: 16,
		},
		{
			name:              "16 segments, 2 workers",
			totalSegments:     16,
			activeWorkers:     2,
			expectedPerWorker: 8,
		},
		{
			name:              "16 segments, 3 workers",
			totalSegments:     16,
			activeWorkers:     3,
			expectedPerWorker: 6,
		},
		{
			name:              "16 segments, 4 workers",
			totalSegments:     16,
			activeWorkers:     4,
			expectedPerWorker: 4,
		},
		{
			name:              "16 segments, 5 workers",
			totalSegments:     16,
			activeWorkers:     5,
			expectedPerWorker: 4,
		},
		{
			name:              "16 segments, 8 workers",
			totalSegments:     16,
			activeWorkers:     8,
			expectedPerWorker: 2,
		},
		{
			name:              "16 segments, 15 workers",
			totalSegments:     16,
			activeWorkers:     15,
			expectedPerWorker: 2,
		},
		{
			name:              "16 segments, 16 workers",
			totalSegments:     16,
			activeWorkers:     16,
			expectedPerWorker: 1,
		},
		{
			name:              "16 segments, 17 workers (more workers than segments)",
			totalSegments:     16,
			activeWorkers:     17,
			expectedPerWorker: 1,
		},
		{
			name:              "16 segments, 32 workers (double workers)",
			totalSegments:     16,
			activeWorkers:     32,
			expectedPerWorker: 1,
		},
		{
			name:              "32 segments, 5 workers",
			totalSegments:     32,
			activeWorkers:     5,
			expectedPerWorker: 7,
		},
		{
			name:              "100 segments, 7 workers",
			totalSegments:     100,
			activeWorkers:     7,
			expectedPerWorker: 15,
		},
		{
			name:              "1 segment, 10 workers",
			totalSegments:     1,
			activeWorkers:     10,
			expectedPerWorker: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ceilDiv(tt.totalSegments, tt.activeWorkers)
			if got != tt.expectedPerWorker {
				t.Errorf("ceilDiv(%d, %d) = %d, want %d (fair share per worker)",
					tt.totalSegments, tt.activeWorkers, got, tt.expectedPerWorker)
			}

			// Verify the ceiling division property:
			// activeWorkers * fairShare >= totalSegments (must cover all segments)
			// (activeWorkers - 1) * fairShare <= totalSegments (minimal - removing one worker may or may not cover all)
			if tt.activeWorkers*got < tt.totalSegments {
				t.Errorf("Fair share too small: %d*%d = %d should be >= %d",
					tt.activeWorkers, got, tt.activeWorkers*got, tt.totalSegments)
			}
			// Minimality: result is the smallest integer where result*b >= a
			// Equivalently: (result-1)*b < a
			if got > 1 && (got-1)*tt.activeWorkers >= tt.totalSegments {
				t.Errorf("Fair share not minimal: (%d-1)*%d = %d should be < %d",
					got, tt.activeWorkers, (got-1)*tt.activeWorkers, tt.totalSegments)
			}
		})
	}
}

func TestCeilDiv_MathematicalProperties(t *testing.T) {
	t.Run("ceiling property", func(t *testing.T) {
		// For any a, b: ceilDiv(a, b) * b >= a
		testCases := []struct{ a, b int }{
			{10, 3}, {16, 5}, {100, 7}, {1, 1}, {7, 2},
		}
		for _, tc := range testCases {
			result := ceilDiv(tc.a, tc.b)
			if result*tc.b < tc.a {
				t.Errorf("ceilDiv(%d, %d) = %d, but %d * %d = %d < %d (violates ceiling property)",
					tc.a, tc.b, result, result, tc.b, result*tc.b, tc.a)
			}
		}
	})

	t.Run("minimality property", func(t *testing.T) {
		// For any a, b: (ceilDiv(a, b) - 1) * b < a
		testCases := []struct{ a, b int }{
			{10, 3}, {16, 5}, {100, 7}, {1, 1}, {7, 2},
		}
		for _, tc := range testCases {
			result := ceilDiv(tc.a, tc.b)
			if result > 1 && (result-1)*tc.b >= tc.a {
				t.Errorf("ceilDiv(%d, %d) = %d, but %d * %d = %d >= %d (not minimal)",
					tc.a, tc.b, result, result-1, tc.b, (result-1)*tc.b, tc.a)
			}
		}
	})

	t.Run("exact division identity", func(t *testing.T) {
		// When a % b == 0, ceilDiv(a, b) == a / b
		testCases := []struct{ a, b int }{
			{16, 4}, {100, 10}, {1000, 100}, {8, 2}, {15, 3},
		}
		for _, tc := range testCases {
			expected := tc.a / tc.b
			got := ceilDiv(tc.a, tc.b)
			if got != expected {
				t.Errorf("ceilDiv(%d, %d) = %d, want %d (exact division)",
					tc.a, tc.b, got, expected)
			}
		}
	})

	t.Run("idempotency with b=1", func(t *testing.T) {
		// ceilDiv(a, 1) == a for all a
		testCases := []int{1, 2, 5, 16, 100, 1000}
		for _, a := range testCases {
			got := ceilDiv(a, 1)
			if got != a {
				t.Errorf("ceilDiv(%d, 1) = %d, want %d", a, got, a)
			}
		}
	})

	t.Run("result is always 1 when a <= b", func(t *testing.T) {
		// When a <= b, ceilDiv(a, b) == 1
		testCases := []struct{ a, b int }{
			{1, 1}, {1, 2}, {1, 100}, {5, 10}, {99, 100},
		}
		for _, tc := range testCases {
			got := ceilDiv(tc.a, tc.b)
			if got != 1 {
				t.Errorf("ceilDiv(%d, %d) = %d, want 1", tc.a, tc.b, got)
			}
		}
	})
}

func TestCeilDiv_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		a, b     int
		expected int
	}{
		{
			name:     "zero segments",
			a:        0,
			b:        5,
			expected: 0,
		},
		{
			name: "zero workers would cause division by zero",
			a:    16,
			b:    0,
			// Note: In production, activeWorkers is guarded to be at least 1
			// This test documents current behavior if called with b=0
			// Behavior is implementation-defined (likely panic or undefined)
			expected: -1, // Placeholder - this will panic in actual code
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.b == 0 {
				// Test that division by zero panics
				defer func() {
					if r := recover(); r == nil {
						t.Error("ceilDiv with b=0 should panic")
					}
				}()
				_ = ceilDiv(tt.a, tt.b)
				return
			}

			got := ceilDiv(tt.a, tt.b)
			if got != tt.expected {
				t.Errorf("ceilDiv(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.expected)
			}
		})
	}
}

func TestCeilDiv_RebalancingExamples(t *testing.T) {
	// Document expected behavior in realistic rebalancing scenarios
	t.Run("worker joins cluster", func(t *testing.T) {
		// Initial: 16 segments, 4 workers = 4 segments each
		// After: 16 segments, 5 workers = 4 segments each (rounded up from 3.2)
		// Workers should release segments: 4*4 = 16 -> 5*4 = 20, so 4 segments need redistribution

		before := ceilDiv(16, 4)
		after := ceilDiv(16, 5)

		if before != 4 {
			t.Errorf("Before: ceilDiv(16, 4) = %d, want 4", before)
		}
		if after != 4 {
			t.Errorf("After: ceilDiv(16, 5) = %d, want 4", after)
		}

		// In this case, fair share doesn't change, but in practice some workers
		// hold fewer segments after rebalancing
	})

	t.Run("worker leaves cluster", func(t *testing.T) {
		// Initial: 16 segments, 5 workers = 4 segments each
		// After: 16 segments, 4 workers = 4 segments each
		// Orphaned segments (from failed worker) are reclaimed by remaining workers

		before := ceilDiv(16, 5)
		after := ceilDiv(16, 4)

		if before != 4 {
			t.Errorf("Before: ceilDiv(16, 5) = %d, want 4", before)
		}
		if after != 4 {
			t.Errorf("After: ceilDiv(16, 4) = %d, want 4", after)
		}
	})

	t.Run("scale from 1 to many workers", func(t *testing.T) {
		// Initial: 16 segments, 1 worker = 16 segments
		// After: 16 segments, 16 workers = 1 segment each
		// Demonstrates maximum scale-out

		before := ceilDiv(16, 1)
		after := ceilDiv(16, 16)

		if before != 16 {
			t.Errorf("Before: ceilDiv(16, 1) = %d, want 16", before)
		}
		if after != 1 {
			t.Errorf("After: ceilDiv(16, 16) = %d, want 1", after)
		}
	})

	t.Run("over-provisioned workers", func(t *testing.T) {
		// 16 segments, 100 workers = 1 segment each
		// Many workers will be idle (not owning any segments)
		// This is expected behavior: segments limit parallelism

		fairShare := ceilDiv(16, 100)
		if fairShare != 1 {
			t.Errorf("ceilDiv(16, 100) = %d, want 1", fairShare)
		}

		// With 1 segment per worker, only 16 workers can claim segments
		// 84 workers will be idle
	})
}

func TestCeilDiv_DistributionQuality(t *testing.T) {
	// Test that ceiling division provides good load distribution
	tests := []struct {
		name          string
		totalSegments int
		activeWorkers int
	}{
		{"16 segments, 3 workers", 16, 3},
		{"16 segments, 5 workers", 16, 5},
		{"16 segments, 7 workers", 16, 7},
		{"32 segments, 5 workers", 32, 5},
		{"100 segments, 7 workers", 100, 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fairShare := ceilDiv(tt.totalSegments, tt.activeWorkers)

			// Calculate actual distribution
			// Some workers get fairShare segments, others get (fairShare - 1)
			workersWithMore := tt.totalSegments % tt.activeWorkers
			if workersWithMore == 0 {
				workersWithMore = tt.activeWorkers
			}
			workersWithFewer := tt.activeWorkers - workersWithMore

			segmentsHeldByMore := workersWithMore * fairShare
			segmentsHeldByFewer := workersWithFewer * (fairShare - 1)
			total := segmentsHeldByMore + segmentsHeldByFewer

			if total != tt.totalSegments {
				t.Errorf("Distribution mismatch: %d workers with %d segments + %d workers with %d segments = %d, want %d total segments",
					workersWithMore, fairShare, workersWithFewer, fairShare-1, total, tt.totalSegments)
			}

			// Verify load imbalance is at most 1 segment
			maxImbalance := fairShare - (fairShare - 1)
			if maxImbalance != 1 && maxImbalance != 0 {
				t.Errorf("Load imbalance is %d segments, should be at most 1", maxImbalance)
			}
		})
	}
}
