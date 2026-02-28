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
			if seg.LastHeartbeat != nil {
				t.Errorf("Segment %d: expected last_heartbeat=nil, got %v", i, *seg.LastHeartbeat)
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

		if seg.LastHeartbeat == nil {
			t.Error("Expected last_heartbeat to be set, got nil")
		} else if time.Since(*seg.LastHeartbeat) > 5*time.Second {
			// Should be recent
			t.Errorf("Expected recent heartbeat, got %v", *seg.LastHeartbeat)
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
		if seg0.LastHeartbeat != nil {
			t.Errorf("Expected segment 0 last_heartbeat=nil, got %v", *seg0.LastHeartbeat)
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

func TestSegmentStore_Heartbeat(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-consumer-heartbeat"
	totalSegments := 4
	ownerID := "worker-1"

	// Initialize and claim segments
	err := store.InitializeSegments(ctx, db, consumerName, totalSegments)
	if err != nil {
		t.Fatalf("Failed to initialize segments: %v", err)
	}

	seg1, err := store.ClaimSegment(ctx, db, consumerName, ownerID)
	if err != nil {
		t.Fatalf("Failed to claim segment: %v", err)
	}
	if seg1 == nil {
		t.Fatal("Expected to claim a segment")
	}

	firstHeartbeat := *seg1.LastHeartbeat

	t.Run("heartbeat updates last_heartbeat", func(t *testing.T) {
		// Wait a bit to ensure timestamp changes
		time.Sleep(100 * time.Millisecond)

		err := store.Heartbeat(ctx, db, consumerName, ownerID)
		if err != nil {
			t.Fatalf("Failed to send heartbeat: %v", err)
		}

		// Verify last_heartbeat was updated
		segments, err := store.GetSegments(ctx, db, consumerName)
		if err != nil {
			t.Fatalf("Failed to get segments: %v", err)
		}

		seg0 := segments[0]
		if seg0.LastHeartbeat == nil {
			t.Fatal("Expected last_heartbeat to be set")
		}

		if !seg0.LastHeartbeat.After(firstHeartbeat) {
			t.Errorf("Expected heartbeat to be updated: before=%v, after=%v",
				firstHeartbeat, *seg0.LastHeartbeat)
		}
	})

	t.Run("heartbeat only affects owned segments", func(t *testing.T) {
		segments, err := store.GetSegments(ctx, db, consumerName)
		if err != nil {
			t.Fatalf("Failed to get segments: %v", err)
		}

		// Segments 1, 2, 3 should still be unclaimed
		for i := 1; i < totalSegments; i++ {
			if segments[i].LastHeartbeat != nil {
				t.Errorf("Expected segment %d last_heartbeat=nil, got %v",
					i, *segments[i].LastHeartbeat)
			}
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

	// Claim 2 segments
	seg0, err := store.ClaimSegment(ctx, db, consumerName, ownerID)
	if err != nil || seg0 == nil {
		t.Fatalf("Failed to claim segment 0: %v", err)
	}

	seg1, err := store.ClaimSegment(ctx, db, consumerName, ownerID)
	if err != nil || seg1 == nil {
		t.Fatalf("Failed to claim segment 1: %v", err)
	}

	t.Run("reclaim with recent heartbeat does nothing", func(t *testing.T) {
		staleThreshold := 30 * time.Second
		count, err := store.ReclaimStaleSegments(ctx, db, consumerName, staleThreshold)
		if err != nil {
			t.Fatalf("Failed to reclaim: %v", err)
		}

		if count != 0 {
			t.Errorf("Expected 0 stale segments, reclaimed %d", count)
		}
	})

	t.Run("reclaim with old heartbeat releases segments", func(t *testing.T) {
		// Manually set old heartbeat timestamp
		oldTime := time.Now().Add(-60 * time.Second)
		_, err := db.ExecContext(ctx, `
			UPDATE consumer_segments 
			SET last_heartbeat = $1 
			WHERE consumer_name = $2 AND owner_id = $3
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
			if segments[i].LastHeartbeat != nil {
				t.Errorf("Segment %d should have nil heartbeat, got %v",
					i, *segments[i].LastHeartbeat)
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

	processor := postgres.NewSegmentProcessor(db, store, &config)

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

		processor2 := postgres.NewSegmentProcessor(db, store, &config)
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

	processor := postgres.NewSegmentProcessor(db, store, &config)

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

		processor := postgres.NewSegmentProcessor(db, store, &config)

		err := processor.Run(ctx, cons)
		if err == nil {
			t.Fatal("Expected error, got nil")
		}

		if !strings.Contains(err.Error(), "simulated consumer error") {
			t.Errorf("Expected error to contain 'simulated consumer error', got: %v", err)
		}
	})
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
