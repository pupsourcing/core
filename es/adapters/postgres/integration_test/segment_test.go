// Package integration_test contains integration tests for the Postgres adapter.
// These tests require a running PostgreSQL instance.
//
// Run with: go test -tags=integration ./es/adapters/postgres/integration_test/...
//
//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/adapters/postgres"
	"github.com/pupsourcing/core/es/consumer"
)

func TestSegmentStore_InitializeSegments(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-consumer-init"
	totalSegments := 4

	t.Run("initialize segments creates correct records", func(t *testing.T) {
		err := store.InitializeSegments(ctx, db, consumerName, totalSegments)
		if err != nil {
			t.Fatalf("Failed to initialize segments: %v", err)
		}

		// Verify segments exist with correct fields
		segments, err := store.GetSegments(ctx, db, consumerName)
		if err != nil {
			t.Fatalf("Failed to get segments: %v", err)
		}

		if len(segments) != totalSegments {
			t.Errorf("Expected %d segments, got %d", totalSegments, len(segments))
		}

		for i, seg := range segments {
			if seg.ConsumerName != consumerName {
				t.Errorf("Segment %d: expected consumer_name=%q, got %q", i, consumerName, seg.ConsumerName)
			}
			if seg.SegmentID != i {
				t.Errorf("Segment %d: expected segment_id=%d, got %d", i, i, seg.SegmentID)
			}
			if seg.TotalSegments != totalSegments {
				t.Errorf("Segment %d: expected total_segments=%d, got %d", i, totalSegments, seg.TotalSegments)
			}
			if seg.OwnerID != nil {
				t.Errorf("Segment %d: expected owner_id=nil, got %v", i, *seg.OwnerID)
			}
			if seg.Checkpoint != 0 {
				t.Errorf("Segment %d: expected checkpoint=0, got %d", i, seg.Checkpoint)
			}
		}
	})

	t.Run("initialize segments is idempotent", func(t *testing.T) {
		// Call again - should not error
		err := store.InitializeSegments(ctx, db, consumerName, totalSegments)
		if err != nil {
			t.Fatalf("Second initialization failed: %v", err)
		}

		// Verify still 4 segments
		segments, err := store.GetSegments(ctx, db, consumerName)
		if err != nil {
			t.Fatalf("Failed to get segments: %v", err)
		}

		if len(segments) != totalSegments {
			t.Errorf("Expected %d segments after re-init, got %d", totalSegments, len(segments))
		}
	})
}

func TestSegmentStore_InitializeSegments_DefaultsToZero(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-consumer-no-prior"

	err := store.InitializeSegments(ctx, db, consumerName, 4)
	if err != nil {
		t.Fatalf("InitializeSegments failed: %v", err)
	}

	segments, err := store.GetSegments(ctx, db, consumerName)
	if err != nil {
		t.Fatalf("GetSegments failed: %v", err)
	}

	for _, seg := range segments {
		if seg.Checkpoint != 0 {
			t.Errorf("Segment %d: expected checkpoint=0, got %d", seg.SegmentID, seg.Checkpoint)
		}
	}
}

func TestSegmentStore_InitializeSegments_DoesNotOverwriteExistingCheckpoints(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-consumer-idempotent"

	// First init — segments start at 0
	err := store.InitializeSegments(ctx, db, consumerName, 4)
	if err != nil {
		t.Fatalf("First InitializeSegments failed: %v", err)
	}

	// Advance segment 0 checkpoint to 300
	err = store.UpdateSegmentCheckpoint(ctx, db, consumerName, 0, 300)
	if err != nil {
		t.Fatalf("UpdateSegmentCheckpoint failed: %v", err)
	}

	// Re-initialize — should NOT overwrite segment 0 back to 0
	err = store.InitializeSegments(ctx, db, consumerName, 4)
	if err != nil {
		t.Fatalf("Second InitializeSegments failed: %v", err)
	}

	cp, err := store.GetSegmentCheckpoint(ctx, db, consumerName, 0)
	if err != nil {
		t.Fatalf("GetSegmentCheckpoint failed: %v", err)
	}

	if cp != 300 {
		t.Errorf("Segment 0: expected checkpoint=300 (preserved), got %d", cp)
	}
}

func TestSegmentStore_ClaimAndRelease(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-consumer-claim"
	totalSegments := 4
	ownerID := "worker-1"

	// Initialize segments
	err := store.InitializeSegments(ctx, db, consumerName, totalSegments)
	if err != nil {
		t.Fatalf("Failed to initialize segments: %v", err)
	}

	t.Run("claim segment returns first available", func(t *testing.T) {
		seg, err := store.ClaimSegment(ctx, db, consumerName, ownerID)
		if err != nil {
			t.Fatalf("Failed to claim segment: %v", err)
		}

		if seg == nil {
			t.Fatal("Expected to claim a segment, got nil")
		}

		if seg.SegmentID != 0 {
			t.Errorf("Expected to claim segment 0 first, got %d", seg.SegmentID)
		}

		if seg.OwnerID == nil || *seg.OwnerID != ownerID {
			t.Errorf("Expected owner_id=%q, got %v", ownerID, seg.OwnerID)
		}
	})

	t.Run("release segment makes it unclaimed", func(t *testing.T) {
		err := store.ReleaseSegment(ctx, db, consumerName, 0, ownerID)
		if err != nil {
			t.Fatalf("Failed to release segment: %v", err)
		}

		// Verify it's unclaimed
		segments, err := store.GetSegments(ctx, db, consumerName)
		if err != nil {
			t.Fatalf("Failed to get segments: %v", err)
		}

		seg0 := segments[0]
		if seg0.OwnerID != nil {
			t.Errorf("Expected segment 0 to be unclaimed, got owner_id=%v", *seg0.OwnerID)
		}
	})
}

func TestSegmentStore_ClaimAllSegments(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-consumer-claim-all"
	totalSegments := 4
	ownerID := "worker-1"

	// Initialize segments
	err := store.InitializeSegments(ctx, db, consumerName, totalSegments)
	if err != nil {
		t.Fatalf("Failed to initialize segments: %v", err)
	}

	t.Run("claim all segments returns each once", func(t *testing.T) {
		claimedIDs := make(map[int]bool)

		for i := 0; i < totalSegments; i++ {
			seg, err := store.ClaimSegment(ctx, db, consumerName, ownerID)
			if err != nil {
				t.Fatalf("Failed to claim segment %d: %v", i, err)
			}
			if seg == nil {
				t.Fatalf("Expected to claim segment %d, got nil", i)
			}

			if claimedIDs[seg.SegmentID] {
				t.Errorf("Claimed segment %d twice", seg.SegmentID)
			}
			claimedIDs[seg.SegmentID] = true
		}

		if len(claimedIDs) != totalSegments {
			t.Errorf("Expected to claim %d unique segments, got %d", totalSegments, len(claimedIDs))
		}
	})

	t.Run("claim when all owned returns nil", func(t *testing.T) {
		seg, err := store.ClaimSegment(ctx, db, consumerName, ownerID)
		if err != nil {
			t.Fatalf("Expected nil error, got: %v", err)
		}
		if seg != nil {
			t.Errorf("Expected nil segment when all claimed, got segment %d", seg.SegmentID)
		}
	})
}

func TestSegmentStore_ReclaimStaleSegments(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-consumer-reclaim"
	totalSegments := 4
	ownerID := "worker-1"

	// Initialize and claim segments
	err := store.InitializeSegments(ctx, db, consumerName, totalSegments)
	if err != nil {
		t.Fatalf("Failed to initialize segments: %v", err)
	}

	// Register the worker in the registry
	err = store.RegisterWorker(ctx, db, consumerName, ownerID)
	if err != nil {
		t.Fatalf("Failed to register worker: %v", err)
	}

	// Claim 2 segments
	seg0, err := store.ClaimSegment(ctx, db, consumerName, ownerID)
	if err != nil || seg0 == nil {
		t.Fatalf("Failed to claim segment 0: %v", err)
	}

	seg1, err := store.ClaimSegment(ctx, db, consumerName, ownerID)
	if err != nil || seg1 == nil {
		t.Fatalf("Failed to claim segment 1: %v", err)
	}

	t.Run("reclaim with active worker does nothing", func(t *testing.T) {
		staleThreshold := 30 * time.Second
		count, err := store.ReclaimStaleSegments(ctx, db, consumerName, staleThreshold)
		if err != nil {
			t.Fatalf("Failed to reclaim: %v", err)
		}

		if count != 0 {
			t.Errorf("Expected 0 stale segments, reclaimed %d", count)
		}
	})

	t.Run("reclaim with stale worker releases segments", func(t *testing.T) {
		// Manually set old heartbeat in the worker registry
		oldTime := time.Now().Add(-60 * time.Second)
		_, err := db.ExecContext(ctx, `
			UPDATE consumer_workers 
			SET last_heartbeat = $1 
			WHERE consumer_name = $2 AND worker_id = $3
		`, oldTime, consumerName, ownerID)
		if err != nil {
			t.Fatalf("Failed to set old heartbeat: %v", err)
		}

		staleThreshold := 30 * time.Second
		count, err := store.ReclaimStaleSegments(ctx, db, consumerName, staleThreshold)
		if err != nil {
			t.Fatalf("Failed to reclaim: %v", err)
		}

		if count != 2 {
			t.Errorf("Expected to reclaim 2 stale segments, got %d", count)
		}

		// Verify segments are unclaimed
		segments, err := store.GetSegments(ctx, db, consumerName)
		if err != nil {
			t.Fatalf("Failed to get segments: %v", err)
		}

		for i := 0; i < 2; i++ {
			if segments[i].OwnerID != nil {
				t.Errorf("Segment %d should be unclaimed, got owner_id=%v",
					i, *segments[i].OwnerID)
			}
		}
	})
}

//nolint:gocyclo // Integration test intentionally tests multiple scenarios comprehensively
func TestSegmentStore_GetSegments(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-consumer-get"
	totalSegments := 4
	owner1 := "worker-1"
	owner2 := "worker-2"

	// Initialize segments
	err := store.InitializeSegments(ctx, db, consumerName, totalSegments)
	if err != nil {
		t.Fatalf("Failed to initialize segments: %v", err)
	}

	// Claim some segments
	seg0, err := store.ClaimSegment(ctx, db, consumerName, owner1)
	if err != nil || seg0 == nil {
		t.Fatalf("Failed to claim segment for owner1: %v", err)
	}

	seg1, err := store.ClaimSegment(ctx, db, consumerName, owner2)
	if err != nil || seg1 == nil {
		t.Fatalf("Failed to claim segment for owner2: %v", err)
	}

	t.Run("get segments returns complete picture", func(t *testing.T) {
		segments, err := store.GetSegments(ctx, db, consumerName)
		if err != nil {
			t.Fatalf("Failed to get segments: %v", err)
		}

		if len(segments) != totalSegments {
			t.Fatalf("Expected %d segments, got %d", totalSegments, len(segments))
		}

		// Verify ownership
		if segments[0].OwnerID == nil || *segments[0].OwnerID != owner1 {
			t.Errorf("Segment 0 should be owned by %q, got %v", owner1, segments[0].OwnerID)
		}
		if segments[1].OwnerID == nil || *segments[1].OwnerID != owner2 {
			t.Errorf("Segment 1 should be owned by %q, got %v", owner2, segments[1].OwnerID)
		}
		if segments[2].OwnerID != nil {
			t.Errorf("Segment 2 should be unclaimed, got %v", *segments[2].OwnerID)
		}
		if segments[3].OwnerID != nil {
			t.Errorf("Segment 3 should be unclaimed, got %v", *segments[3].OwnerID)
		}

		// Verify segments are ordered by segment_id
		for i, seg := range segments {
			if seg.SegmentID != i {
				t.Errorf("Expected segment %d at index %d, got %d", i, i, seg.SegmentID)
			}
		}
	})
}

func TestSegmentStore_Checkpoint(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-consumer-checkpoint"
	totalSegments := 4
	segmentID := 0

	// Initialize segments
	err := store.InitializeSegments(ctx, db, consumerName, totalSegments)
	if err != nil {
		t.Fatalf("Failed to initialize segments: %v", err)
	}

	t.Run("initial checkpoint is zero", func(t *testing.T) {
		checkpoint, err := store.GetSegmentCheckpoint(ctx, db, consumerName, segmentID)
		if err != nil {
			t.Fatalf("Failed to get checkpoint: %v", err)
		}

		if checkpoint != 0 {
			t.Errorf("Expected initial checkpoint=0, got %d", checkpoint)
		}
	})

	t.Run("update and read checkpoint", func(t *testing.T) {
		newCheckpoint := int64(12345)
		err := store.UpdateSegmentCheckpoint(ctx, db, consumerName, segmentID, newCheckpoint)
		if err != nil {
			t.Fatalf("Failed to update checkpoint: %v", err)
		}

		checkpoint, err := store.GetSegmentCheckpoint(ctx, db, consumerName, segmentID)
		if err != nil {
			t.Fatalf("Failed to get checkpoint: %v", err)
		}

		if checkpoint != newCheckpoint {
			t.Errorf("Expected checkpoint=%d, got %d", newCheckpoint, checkpoint)
		}
	})

	t.Run("checkpoint persists across operations", func(t *testing.T) {
		// Verify via GetSegments too
		segments, err := store.GetSegments(ctx, db, consumerName)
		if err != nil {
			t.Fatalf("Failed to get segments: %v", err)
		}

		if segments[segmentID].Checkpoint != 12345 {
			t.Errorf("Expected checkpoint=12345 in segment, got %d",
				segments[segmentID].Checkpoint)
		}
	})
}

//nolint:gocyclo // Integration test intentionally tests multiple scenarios comprehensively
func TestSegmentProcessor_OneOff(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-segment-processor"
	totalSegments := 4

	// Append test events - one per aggregate (Append requires same aggregate per batch)
	const eventCount = 20

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	for i := 0; i < eventCount; i++ {
		event := es.Event{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    fmt.Sprintf("agg-%d", i),
			EventID:        uuid.New(),
			EventType:      "TestEvent",
			EventVersion:   1,
			Payload:        []byte(fmt.Sprintf(`{"index":%d}`, i)),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		}
		_, err = store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			//nolint:errcheck
			tx.Rollback()
			t.Fatalf("Failed to append event %d: %v", i, err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit events: %v", err)
	}

	// Create counting consumer
	var processedCount int64
	var processedEvents []es.PersistedEvent
	var mu sync.Mutex

	cons := &testConsumer{
		name: consumerName,
		handler: func(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
			atomic.AddInt64(&processedCount, 1)
			mu.Lock()
			processedEvents = append(processedEvents, event)
			mu.Unlock()
			return nil
		},
	}

	// Configure segment processor in one-off mode
	config := consumer.DefaultSegmentProcessorConfig()
	config.RunMode = consumer.RunModeOneOff
	config.TotalSegments = totalSegments
	config.BatchSize = 5 // Process in multiple batches

	processor := postgres.NewSegmentProcessor(db, store, config)

	t.Run("one-off mode processes all events exactly once", func(t *testing.T) {
		err := processor.Run(ctx, cons)
		if err != nil {
			t.Fatalf("Processor failed: %v", err)
		}

		// Verify all events were processed
		if processedCount != eventCount {
			t.Errorf("Expected %d events processed, got %d", eventCount, processedCount)
		}

		// Verify no duplicates
		seen := make(map[string]bool)
		for _, evt := range processedEvents {
			key := evt.EventID.String()
			if seen[key] {
				t.Errorf("Event %s processed twice", key)
			}
			seen[key] = true
		}
	})

	t.Run("checkpoints are saved", func(t *testing.T) {
		// Check that segment checkpoints were updated
		segments, err := store.GetSegments(ctx, db, consumerName)
		if err != nil {
			t.Fatalf("Failed to get segments: %v", err)
		}

		// At least one segment should have non-zero checkpoint
		hasCheckpoint := false
		for _, seg := range segments {
			if seg.Checkpoint > 0 {
				hasCheckpoint = true
			}
		}

		if !hasCheckpoint {
			t.Error("Expected at least one segment to have non-zero checkpoint")
		}
	})

	t.Run("segments are released after completion", func(t *testing.T) {
		segments, err := store.GetSegments(ctx, db, consumerName)
		if err != nil {
			t.Fatalf("Failed to get segments: %v", err)
		}

		// All segments should be unclaimed after one-off completes
		for i, seg := range segments {
			if seg.OwnerID != nil {
				t.Errorf("Segment %d should be unclaimed after completion, got owner_id=%v",
					i, *seg.OwnerID)
			}
		}
	})

	t.Run("running again processes nothing", func(t *testing.T) {
		// Reset counter
		atomic.StoreInt64(&processedCount, 0)
		processedEvents = nil

		cons2 := &testConsumer{
			name: consumerName,
			handler: func(_ context.Context, _ *sql.Tx, _ es.PersistedEvent) error {
				atomic.AddInt64(&processedCount, 1)
				return nil
			},
		}

		processor2 := postgres.NewSegmentProcessor(db, store, config)
		err := processor2.Run(ctx, cons2)
		if err != nil {
			t.Fatalf("Second run failed: %v", err)
		}

		// Should process nothing (already at checkpoint)
		if processedCount != 0 {
			t.Errorf("Expected 0 events on second run, got %d", processedCount)
		}
	})
}

func TestSegmentProcessor_OneOff_EventDistribution(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-segment-distribution"
	totalSegments := 4

	// Append events with different aggregate IDs to test distribution
	const eventCount = 16 // Evenly divisible by 4 segments

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	for i := 0; i < eventCount; i++ {
		event := es.Event{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    fmt.Sprintf("aggregate-%d", i),
			EventID:        uuid.New(),
			EventType:      "TestEvent",
			EventVersion:   1,
			Payload:        []byte(fmt.Sprintf(`{"id":%d}`, i)),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		}
		_, err = store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			//nolint:errcheck
			tx.Rollback()
			t.Fatalf("Failed to append event %d: %v", i, err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit events: %v", err)
	}

	// Track which events were processed
	var processedCount int64
	processedAggregates := make(map[string]bool)
	var aggMu sync.Mutex

	cons := &testConsumer{
		name: consumerName,
		handler: func(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
			atomic.AddInt64(&processedCount, 1)
			aggMu.Lock()
			processedAggregates[event.AggregateID] = true
			aggMu.Unlock()
			return nil
		},
	}

	// Run processor
	config := consumer.DefaultSegmentProcessorConfig()
	config.RunMode = consumer.RunModeOneOff
	config.TotalSegments = totalSegments
	config.BatchSize = 4

	processor := postgres.NewSegmentProcessor(db, store, config)

	t.Run("all events processed across segments", func(t *testing.T) {
		err := processor.Run(ctx, cons)
		if err != nil {
			t.Fatalf("Processor failed: %v", err)
		}

		if processedCount != eventCount {
			t.Errorf("Expected %d events, got %d", eventCount, processedCount)
		}

		// Verify all unique aggregates were processed
		if len(processedAggregates) != eventCount {
			t.Errorf("Expected %d unique aggregates, got %d",
				eventCount, len(processedAggregates))
		}
	})
}

func TestSegmentProcessor_OneOff_ErrorHandling(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-segment-error"
	totalSegments := 2

	// Append test events - one per aggregate
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	for i, aggID := range []string{"agg-1", "agg-2"} {
		event := es.Event{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggID,
			EventID:        uuid.New(),
			EventType:      "TestEvent",
			EventVersion:   1,
			Payload:        []byte(fmt.Sprintf(`{"index":%d}`, i+1)),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		}
		_, err = store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			//nolint:errcheck
			tx.Rollback()
			t.Fatalf("Failed to append event %s: %v", aggID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit events: %v", err)
	}

	t.Run("consumer error stops processor", func(t *testing.T) {
		cons := &testConsumer{
			name: consumerName,
			handler: func(_ context.Context, _ *sql.Tx, _ es.PersistedEvent) error {
				// Fail on first event
				return fmt.Errorf("simulated consumer error")
			},
		}

		config := consumer.DefaultSegmentProcessorConfig()
		config.RunMode = consumer.RunModeOneOff
		config.TotalSegments = totalSegments
		config.BatchSize = 10

		processor := postgres.NewSegmentProcessor(db, store, config)

		err := processor.Run(ctx, cons)
		if err == nil {
			t.Fatal("Expected error, got nil")
		}

		if !strings.Contains(err.Error(), "simulated consumer error") {
			t.Errorf("Expected error to contain 'simulated consumer error', got: %v", err)
		}
	})
}

func TestWorkerRegistry_RegisterAndCount(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-registry-count"
	staleThreshold := 30 * time.Second

	t.Run("no workers initially", func(t *testing.T) {
		count, err := store.CountActiveWorkers(ctx, db, consumerName, staleThreshold)
		if err != nil {
			t.Fatalf("CountActiveWorkers failed: %v", err)
		}
		if count != 0 {
			t.Errorf("Expected 0 active workers, got %d", count)
		}
	})

	t.Run("register three workers", func(t *testing.T) {
		for _, wid := range []string{"w1", "w2", "w3"} {
			if err := store.RegisterWorker(ctx, db, consumerName, wid); err != nil {
				t.Fatalf("RegisterWorker(%s) failed: %v", wid, err)
			}
		}

		count, err := store.CountActiveWorkers(ctx, db, consumerName, staleThreshold)
		if err != nil {
			t.Fatalf("CountActiveWorkers failed: %v", err)
		}
		if count != 3 {
			t.Errorf("Expected 3 active workers, got %d", count)
		}
	})

	t.Run("register is idempotent", func(t *testing.T) {
		if err := store.RegisterWorker(ctx, db, consumerName, "w1"); err != nil {
			t.Fatalf("Re-register w1 failed: %v", err)
		}

		count, err := store.CountActiveWorkers(ctx, db, consumerName, staleThreshold)
		if err != nil {
			t.Fatalf("CountActiveWorkers failed: %v", err)
		}
		if count != 3 {
			t.Errorf("Expected 3 after re-register, got %d", count)
		}
	})

	t.Run("different consumer is isolated", func(t *testing.T) {
		if err := store.RegisterWorker(ctx, db, "other-consumer", "wx"); err != nil {
			t.Fatalf("RegisterWorker failed: %v", err)
		}

		count, err := store.CountActiveWorkers(ctx, db, consumerName, staleThreshold)
		if err != nil {
			t.Fatalf("CountActiveWorkers failed: %v", err)
		}
		if count != 3 {
			t.Errorf("Expected 3 (isolated from other consumer), got %d", count)
		}
	})
}

func TestWorkerRegistry_DeregisterReducesCount(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-registry-deregister"
	staleThreshold := 30 * time.Second

	for _, wid := range []string{"w1", "w2", "w3"} {
		if err := store.RegisterWorker(ctx, db, consumerName, wid); err != nil {
			t.Fatalf("RegisterWorker(%s) failed: %v", wid, err)
		}
	}

	t.Run("deregister one worker", func(t *testing.T) {
		if err := store.DeregisterWorker(ctx, db, consumerName, "w2"); err != nil {
			t.Fatalf("DeregisterWorker failed: %v", err)
		}

		count, err := store.CountActiveWorkers(ctx, db, consumerName, staleThreshold)
		if err != nil {
			t.Fatalf("CountActiveWorkers failed: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected 2 after deregister, got %d", count)
		}
	})

	t.Run("deregister non-existent worker is no-op", func(t *testing.T) {
		if err := store.DeregisterWorker(ctx, db, consumerName, "w99"); err != nil {
			t.Fatalf("DeregisterWorker failed: %v", err)
		}

		count, err := store.CountActiveWorkers(ctx, db, consumerName, staleThreshold)
		if err != nil {
			t.Fatalf("CountActiveWorkers failed: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected 2 unchanged, got %d", count)
		}
	})
}

func TestWorkerRegistry_PurgeStaleWorkers(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-registry-purge"
	staleThreshold := 30 * time.Second

	// Register 3 workers
	for _, wid := range []string{"w1", "w2", "w3"} {
		if err := store.RegisterWorker(ctx, db, consumerName, wid); err != nil {
			t.Fatalf("RegisterWorker(%s) failed: %v", wid, err)
		}
	}

	t.Run("purge with no stale workers removes nothing", func(t *testing.T) {
		if err := store.PurgeStaleWorkers(ctx, db, consumerName, staleThreshold); err != nil {
			t.Fatalf("PurgeStaleWorkers failed: %v", err)
		}

		count, err := store.CountActiveWorkers(ctx, db, consumerName, staleThreshold)
		if err != nil {
			t.Fatalf("CountActiveWorkers failed: %v", err)
		}
		if count != 3 {
			t.Errorf("Expected 3 (none purged), got %d", count)
		}
	})

	t.Run("purge removes stale workers only", func(t *testing.T) {
		// Make w1 and w2 stale
		oldTime := time.Now().Add(-60 * time.Second)
		_, err := db.ExecContext(ctx, `
			UPDATE consumer_workers 
			SET last_heartbeat = $1 
			WHERE consumer_name = $2 AND worker_id IN ($3, $4)
		`, oldTime, consumerName, "w1", "w2")
		if err != nil {
			t.Fatalf("Failed to set old heartbeat: %v", err)
		}

		if err := store.PurgeStaleWorkers(ctx, db, consumerName, staleThreshold); err != nil {
			t.Fatalf("PurgeStaleWorkers failed: %v", err)
		}

		// Only w3 should remain
		count, err := store.CountActiveWorkers(ctx, db, consumerName, staleThreshold)
		if err != nil {
			t.Fatalf("CountActiveWorkers failed: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 (only w3 fresh), got %d", count)
		}

		// Verify w1 and w2 rows are actually deleted
		var totalRows int
		err = db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM consumer_workers WHERE consumer_name = $1`,
			consumerName).Scan(&totalRows)
		if err != nil {
			t.Fatalf("Failed to count rows: %v", err)
		}
		if totalRows != 1 {
			t.Errorf("Expected 1 row remaining, got %d", totalRows)
		}
	})
}

//nolint:gocyclo // Integration test intentionally tests multi-worker rebalancing scenarios
func TestTwoWorkerRebalance(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-two-worker-rebalance"
	totalSegments := 16
	staleThreshold := 30 * time.Second
	w1 := "worker-1"
	w2 := "worker-2"

	// Initialize segments and register W1 with all 16 segments
	if err := store.InitializeSegments(ctx, db, consumerName, totalSegments); err != nil {
		t.Fatalf("InitializeSegments failed: %v", err)
	}
	if err := store.RegisterWorker(ctx, db, consumerName, w1); err != nil {
		t.Fatalf("RegisterWorker(w1) failed: %v", err)
	}

	for i := 0; i < totalSegments; i++ {
		seg, err := store.ClaimSegment(ctx, db, consumerName, w1)
		if err != nil || seg == nil {
			t.Fatalf("ClaimSegment %d for w1 failed: %v", i, err)
		}
	}

	// Verify W1 owns all 16
	segments, err := store.GetSegments(ctx, db, consumerName)
	if err != nil {
		t.Fatalf("GetSegments failed: %v", err)
	}
	for _, seg := range segments {
		if seg.OwnerID == nil || *seg.OwnerID != w1 {
			t.Fatalf("Expected w1 to own all segments initially")
		}
	}

	// W2 registers (simulating a new worker joining)
	if err := store.RegisterWorker(ctx, db, consumerName, w2); err != nil {
		t.Fatalf("RegisterWorker(w2) failed: %v", err)
	}

	// Both workers see 2 active workers
	count, err := store.CountActiveWorkers(ctx, db, consumerName, staleThreshold)
	if err != nil {
		t.Fatalf("CountActiveWorkers failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 active workers, got %d", count)
	}

	// Calculate fair share: ceil(16 / 2) = 8
	fairShare := (totalSegments + count - 1) / count
	if fairShare != 8 {
		t.Fatalf("Expected fair share = 8, got %d", fairShare)
	}

	// W1 releases excess (has 16, fair share is 8)
	w1Owned := 0
	for _, seg := range segments {
		if seg.OwnerID != nil && *seg.OwnerID == w1 {
			w1Owned++
		}
	}

	released := 0
	for _, seg := range segments {
		if w1Owned <= fairShare {
			break
		}
		if seg.OwnerID != nil && *seg.OwnerID == w1 {
			if err := store.ReleaseSegment(ctx, db, consumerName, seg.SegmentID, w1); err != nil {
				t.Fatalf("ReleaseSegment failed: %v", err)
			}
			released++
			w1Owned--
		}
	}

	if released != 8 {
		t.Errorf("Expected W1 to release 8, released %d", released)
	}

	// W2 claims available segments
	w2Claimed := 0
	for w2Claimed < fairShare {
		seg, err := store.ClaimSegment(ctx, db, consumerName, w2)
		if err != nil {
			t.Fatalf("ClaimSegment for w2 failed: %v", err)
		}
		if seg == nil {
			break
		}
		w2Claimed++
	}

	if w2Claimed != 8 {
		t.Errorf("Expected W2 to claim 8, claimed %d", w2Claimed)
	}

	// Verify final distribution: 8/8
	segments, err = store.GetSegments(ctx, db, consumerName)
	if err != nil {
		t.Fatalf("GetSegments failed: %v", err)
	}

	w1Count, w2Count := 0, 0
	for _, seg := range segments {
		if seg.OwnerID == nil {
			continue
		}
		switch *seg.OwnerID {
		case w1:
			w1Count++
		case w2:
			w2Count++
		}
	}

	if w1Count != 8 || w2Count != 8 {
		t.Errorf("Expected 8/8 distribution, got W1=%d W2=%d", w1Count, w2Count)
	}
}

func TestWorkerGracefulShutdown(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-graceful-shutdown"
	totalSegments := 8
	staleThreshold := 30 * time.Second
	w1 := "worker-1"
	w2 := "worker-2"

	// Setup: 2 workers, 4 segments each
	if err := store.InitializeSegments(ctx, db, consumerName, totalSegments); err != nil {
		t.Fatalf("InitializeSegments failed: %v", err)
	}
	if err := store.RegisterWorker(ctx, db, consumerName, w1); err != nil {
		t.Fatalf("RegisterWorker(w1) failed: %v", err)
	}
	if err := store.RegisterWorker(ctx, db, consumerName, w2); err != nil {
		t.Fatalf("RegisterWorker(w2) failed: %v", err)
	}

	// W1 claims 4, W2 claims 4
	for i := 0; i < 4; i++ {
		if _, err := store.ClaimSegment(ctx, db, consumerName, w1); err != nil {
			t.Fatalf("ClaimSegment for w1 failed: %v", err)
		}
	}
	for i := 0; i < 4; i++ {
		if _, err := store.ClaimSegment(ctx, db, consumerName, w2); err != nil {
			t.Fatalf("ClaimSegment for w2 failed: %v", err)
		}
	}

	// W2 gracefully shuts down: release all segments + deregister
	segments, err := store.GetSegments(ctx, db, consumerName)
	if err != nil {
		t.Fatalf("GetSegments failed: %v", err)
	}
	for _, seg := range segments {
		if seg.OwnerID != nil && *seg.OwnerID == w2 {
			if err := store.ReleaseSegment(ctx, db, consumerName, seg.SegmentID, w2); err != nil {
				t.Fatalf("ReleaseSegment for w2 failed: %v", err)
			}
		}
	}
	if err := store.DeregisterWorker(ctx, db, consumerName, w2); err != nil {
		t.Fatalf("DeregisterWorker(w2) failed: %v", err)
	}

	// W1 sees 1 active worker and claims the 4 released segments
	count, err := store.CountActiveWorkers(ctx, db, consumerName, staleThreshold)
	if err != nil {
		t.Fatalf("CountActiveWorkers failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 active worker after w2 deregister, got %d", count)
	}

	w1Claimed := 0
	for {
		seg, err := store.ClaimSegment(ctx, db, consumerName, w1)
		if err != nil {
			t.Fatalf("ClaimSegment for w1 failed: %v", err)
		}
		if seg == nil {
			break
		}
		w1Claimed++
	}

	if w1Claimed != 4 {
		t.Errorf("Expected W1 to claim 4 released segments, claimed %d", w1Claimed)
	}

	// Verify W1 owns all 8
	segments, err = store.GetSegments(ctx, db, consumerName)
	if err != nil {
		t.Fatalf("GetSegments failed: %v", err)
	}
	for _, seg := range segments {
		if seg.OwnerID == nil || *seg.OwnerID != w1 {
			t.Errorf("Segment %d should be owned by w1, got %v", seg.SegmentID, seg.OwnerID)
		}
	}
}

func TestWorkerCrashRecovery(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-crash-recovery"
	totalSegments := 8
	staleThreshold := 30 * time.Second
	w1 := "worker-1"
	w2 := "worker-2"

	// Setup: 2 workers, 4 segments each
	if err := store.InitializeSegments(ctx, db, consumerName, totalSegments); err != nil {
		t.Fatalf("InitializeSegments failed: %v", err)
	}
	if err := store.RegisterWorker(ctx, db, consumerName, w1); err != nil {
		t.Fatalf("RegisterWorker(w1) failed: %v", err)
	}
	if err := store.RegisterWorker(ctx, db, consumerName, w2); err != nil {
		t.Fatalf("RegisterWorker(w2) failed: %v", err)
	}

	for i := 0; i < 4; i++ {
		if _, err := store.ClaimSegment(ctx, db, consumerName, w1); err != nil {
			t.Fatalf("ClaimSegment for w1 failed: %v", err)
		}
	}
	for i := 0; i < 4; i++ {
		if _, err := store.ClaimSegment(ctx, db, consumerName, w2); err != nil {
			t.Fatalf("ClaimSegment for w2 failed: %v", err)
		}
	}

	// W2 crashes — simulate by making its heartbeat stale (no deregister, no release)
	oldTime := time.Now().Add(-60 * time.Second)
	_, err := db.ExecContext(ctx, `
		UPDATE consumer_workers 
		SET last_heartbeat = $1 
		WHERE consumer_name = $2 AND worker_id = $3
	`, oldTime, consumerName, w2)
	if err != nil {
		t.Fatalf("Failed to set stale heartbeat: %v", err)
	}

	// W1 runs rebalance cycle: purge stale workers, reclaim stale segments
	if err := store.PurgeStaleWorkers(ctx, db, consumerName, staleThreshold); err != nil {
		t.Fatalf("PurgeStaleWorkers failed: %v", err)
	}

	reclaimed, err := store.ReclaimStaleSegments(ctx, db, consumerName, staleThreshold)
	if err != nil {
		t.Fatalf("ReclaimStaleSegments failed: %v", err)
	}
	if reclaimed != 4 {
		t.Errorf("Expected 4 reclaimed segments, got %d", reclaimed)
	}

	// W1 now sees 1 active worker
	count, err := store.CountActiveWorkers(ctx, db, consumerName, staleThreshold)
	if err != nil {
		t.Fatalf("CountActiveWorkers failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 active worker after purge, got %d", count)
	}

	// W1 claims the 4 reclaimed segments
	w1Claimed := 0
	for {
		seg, err := store.ClaimSegment(ctx, db, consumerName, w1)
		if err != nil {
			t.Fatalf("ClaimSegment for w1 failed: %v", err)
		}
		if seg == nil {
			break
		}
		w1Claimed++
	}

	if w1Claimed != 4 {
		t.Errorf("Expected W1 to claim 4 reclaimed segments, claimed %d", w1Claimed)
	}
}

func TestConcurrentWorkerJoin(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-concurrent-join"
	totalSegments := 16
	staleThreshold := 30 * time.Second
	workerCount := 4

	if err := store.InitializeSegments(ctx, db, consumerName, totalSegments); err != nil {
		t.Fatalf("InitializeSegments failed: %v", err)
	}

	// 4 workers register simultaneously
	var wg sync.WaitGroup
	errs := make([]error, workerCount)

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			wid := fmt.Sprintf("worker-%d", idx)
			errs[idx] = store.RegisterWorker(ctx, db, consumerName, wid)
		}(i)
	}

	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("RegisterWorker(worker-%d) failed: %v", i, err)
		}
	}

	count, err := store.CountActiveWorkers(ctx, db, consumerName, staleThreshold)
	if err != nil {
		t.Fatalf("CountActiveWorkers failed: %v", err)
	}
	if count != workerCount {
		t.Errorf("Expected %d active workers, got %d", workerCount, count)
	}

	// Each worker claims its fair share (4 each)
	fairShare := totalSegments / workerCount

	claimed := make([]int, workerCount)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			wid := fmt.Sprintf("worker-%d", idx)
			for c := 0; c < fairShare; c++ {
				seg, err := store.ClaimSegment(ctx, db, consumerName, wid)
				if err != nil {
					t.Errorf("ClaimSegment for %s failed: %v", wid, err)
					return
				}
				if seg != nil {
					claimed[idx]++
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all 16 segments are claimed
	segments, err := store.GetSegments(ctx, db, consumerName)
	if err != nil {
		t.Fatalf("GetSegments failed: %v", err)
	}

	ownedCount := 0
	for _, seg := range segments {
		if seg.OwnerID != nil {
			ownedCount++
		}
	}

	if ownedCount != totalSegments {
		t.Errorf("Expected all %d segments claimed, got %d", totalSegments, ownedCount)
	}

	totalClaimed := 0
	for _, c := range claimed {
		totalClaimed += c
	}
	if totalClaimed != totalSegments {
		t.Errorf("Expected %d total claims, got %d", totalSegments, totalClaimed)
	}
}

// testConsumer is a simple consumer implementation for testing
type testConsumer struct {
	handler func(context.Context, *sql.Tx, es.PersistedEvent) error
	name    string
}

func (c *testConsumer) Name() string {
	return c.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to match Consumer interface
func (c *testConsumer) Handle(ctx context.Context, tx *sql.Tx, event es.PersistedEvent) error {
	return c.handler(ctx, tx, event)
}
