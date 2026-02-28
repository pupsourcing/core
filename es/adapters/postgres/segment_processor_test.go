package postgres

import (
	"sort"
	"testing"
)

// rankBasedFairShare computes the fair share for a worker at the given rank.
// This mirrors the logic in calculateFairShare for unit testing.
func rankBasedFairShare(totalSegments, workerCount, rank int) int {
	floor := totalSegments / workerCount
	remainder := totalSegments % workerCount
	if rank < remainder {
		return floor + 1
	}
	return floor
}

func TestRankBasedFairShare(t *testing.T) {
	tests := []struct {
		name          string
		totalSegments int
		workerCount   int
		// expected[i] = fair share for worker at rank i
		expected []int
	}{
		{
			name:          "16 segments, 1 worker",
			totalSegments: 16,
			workerCount:   1,
			expected:      []int{16},
		},
		{
			name:          "16 segments, 2 workers",
			totalSegments: 16,
			workerCount:   2,
			expected:      []int{8, 8},
		},
		{
			name:          "16 segments, 3 workers",
			totalSegments: 16,
			workerCount:   3,
			expected:      []int{6, 5, 5},
		},
		{
			name:          "16 segments, 4 workers",
			totalSegments: 16,
			workerCount:   4,
			expected:      []int{4, 4, 4, 4},
		},
		{
			name:          "16 segments, 5 workers",
			totalSegments: 16,
			workerCount:   5,
			expected:      []int{4, 3, 3, 3, 3},
		},
		{
			name:          "16 segments, 6 workers",
			totalSegments: 16,
			workerCount:   6,
			expected:      []int{3, 3, 3, 3, 2, 2},
		},
		{
			name:          "16 segments, 7 workers",
			totalSegments: 16,
			workerCount:   7,
			expected:      []int{3, 3, 2, 2, 2, 2, 2},
		},
		{
			name:          "16 segments, 8 workers",
			totalSegments: 16,
			workerCount:   8,
			expected:      []int{2, 2, 2, 2, 2, 2, 2, 2},
		},
		{
			name:          "16 segments, 16 workers",
			totalSegments: 16,
			workerCount:   16,
			expected:      []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		},
		{
			name:          "16 segments, 17 workers (more workers than segments)",
			totalSegments: 16,
			workerCount:   17,
			expected:      []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0},
		},
		{
			name:          "100 segments, 7 workers",
			totalSegments: 100,
			workerCount:   7,
			expected:      []int{15, 15, 14, 14, 14, 14, 14},
		},
		{
			name:          "32 segments, 5 workers",
			totalSegments: 32,
			workerCount:   5,
			expected:      []int{7, 7, 6, 6, 6},
		},
		{
			name:          "1 segment, 10 workers",
			totalSegments: 1,
			workerCount:   10,
			expected:      []int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			totalAssigned := 0
			for rank := 0; rank < tt.workerCount; rank++ {
				got := rankBasedFairShare(tt.totalSegments, tt.workerCount, rank)
				if got != tt.expected[rank] {
					t.Errorf("rank %d: got fair share %d, want %d", rank, got, tt.expected[rank])
				}
				totalAssigned += got
			}

			// All segments must be assigned (or at least accounted for)
			if totalAssigned != tt.totalSegments {
				t.Errorf("total assigned = %d, want %d", totalAssigned, tt.totalSegments)
			}
		})
	}
}

func TestRankBasedFairShare_Properties(t *testing.T) {
	t.Run("sum equals total segments", func(t *testing.T) {
		testCases := []struct{ total, workers int }{
			{16, 1}, {16, 2}, {16, 3}, {16, 5}, {16, 7}, {16, 16}, {16, 17},
			{100, 7}, {32, 5}, {1, 10}, {1000, 99},
		}
		for _, tc := range testCases {
			sum := 0
			for rank := 0; rank < tc.workers; rank++ {
				sum += rankBasedFairShare(tc.total, tc.workers, rank)
			}
			if sum != tc.total {
				t.Errorf("total=%d, workers=%d: sum=%d, want %d", tc.total, tc.workers, sum, tc.total)
			}
		}
	})

	t.Run("max imbalance is at most 1", func(t *testing.T) {
		testCases := []struct{ total, workers int }{
			{16, 3}, {16, 5}, {16, 7}, {100, 7}, {32, 5},
		}
		for _, tc := range testCases {
			minShare := rankBasedFairShare(tc.total, tc.workers, tc.workers-1)
			maxShare := rankBasedFairShare(tc.total, tc.workers, 0)
			if maxShare-minShare > 1 {
				t.Errorf("total=%d, workers=%d: max=%d, min=%d, imbalance=%d > 1",
					tc.total, tc.workers, maxShare, minShare, maxShare-minShare)
			}
		}
	})

	t.Run("rank order is non-increasing", func(t *testing.T) {
		testCases := []struct{ total, workers int }{
			{16, 3}, {16, 5}, {100, 7}, {32, 5}, {1, 10},
		}
		for _, tc := range testCases {
			prev := rankBasedFairShare(tc.total, tc.workers, 0)
			for rank := 1; rank < tc.workers; rank++ {
				curr := rankBasedFairShare(tc.total, tc.workers, rank)
				if curr > prev {
					t.Errorf("total=%d, workers=%d: rank %d has %d > rank %d has %d (not non-increasing)",
						tc.total, tc.workers, rank, curr, rank-1, prev)
				}
				prev = curr
			}
		}
	})

	t.Run("deterministic across workers", func(t *testing.T) {
		// All workers compute the same fair share for the same rank
		// Simulates 5 workers independently computing their fair share
		workerIDs := []string{"aaa", "bbb", "ccc", "ddd", "eee"}
		sort.Strings(workerIDs)

		totalSegments := 16
		for i, wid := range workerIDs {
			// Each worker finds its rank
			rank := -1
			for j, w := range workerIDs {
				if w == wid {
					rank = j
					break
				}
			}
			if rank != i {
				t.Fatalf("unexpected rank for %s: got %d, want %d", wid, rank, i)
			}

			fs := rankBasedFairShare(totalSegments, len(workerIDs), rank)
			if i == 0 && fs != 4 {
				t.Errorf("worker %s (rank 0): fair share = %d, want 4", wid, fs)
			}
			if i > 0 && fs != 3 {
				t.Errorf("worker %s (rank %d): fair share = %d, want 3", wid, i, fs)
			}
		}
	})
}

func TestRankBasedFairShare_RebalancingExamples(t *testing.T) {
	t.Run("worker joins cluster (4 to 5 workers)", func(t *testing.T) {
		// Before: 4 workers, 16 segments → 4/4/4/4
		for rank := 0; rank < 4; rank++ {
			fs := rankBasedFairShare(16, 4, rank)
			if fs != 4 {
				t.Errorf("before: rank %d fair share = %d, want 4", rank, fs)
			}
		}

		// After: 5 workers, 16 segments → 4/3/3/3/3 (rank 0 gets extra)
		expected := []int{4, 3, 3, 3, 3}
		for rank := 0; rank < 5; rank++ {
			fs := rankBasedFairShare(16, 5, rank)
			if fs != expected[rank] {
				t.Errorf("after: rank %d fair share = %d, want %d", rank, fs, expected[rank])
			}
		}
	})

	t.Run("worker leaves cluster (5 to 4 workers)", func(t *testing.T) {
		// Before: 5 workers → 4/3/3/3/3
		// After: 4 workers → 4/4/4/4
		for rank := 0; rank < 4; rank++ {
			fs := rankBasedFairShare(16, 4, rank)
			if fs != 4 {
				t.Errorf("rank %d fair share = %d, want 4", rank, fs)
			}
		}
	})

	t.Run("scale from 1 to 16 workers", func(t *testing.T) {
		before := rankBasedFairShare(16, 1, 0)
		if before != 16 {
			t.Errorf("1 worker: fair share = %d, want 16", before)
		}

		for rank := 0; rank < 16; rank++ {
			fs := rankBasedFairShare(16, 16, rank)
			if fs != 1 {
				t.Errorf("16 workers, rank %d: fair share = %d, want 1", rank, fs)
			}
		}
	})

	t.Run("over-provisioned workers", func(t *testing.T) {
		// 16 segments, 100 workers: ranks 0-15 get 1, ranks 16-99 get 0
		for rank := 0; rank < 100; rank++ {
			fs := rankBasedFairShare(16, 100, rank)
			if rank < 16 && fs != 1 {
				t.Errorf("rank %d: fair share = %d, want 1", rank, fs)
			}
			if rank >= 16 && fs != 0 {
				t.Errorf("rank %d: fair share = %d, want 0", rank, fs)
			}
		}
	})
}
