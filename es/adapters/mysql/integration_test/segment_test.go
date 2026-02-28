// Package integration_test contains integration tests for the MySQL adapter.
// These tests require a running MySQL/MariaDB instance.
//
// Start MySQL: docker run -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=pupsourcing_test mysql:8
// Run with: go test -tags=integration ./es/adapters/mysql/integration_test/...
//
//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/adapters/mysql"
	"github.com/pupsourcing/core/es/consumer"
)

// testConsumer is a simple consumer for testing.
type testConsumer struct {
	name   string
	events []es.PersistedEvent
	mu     sync.Mutex
}

func newTestConsumer(name string) *testConsumer {
	return &testConsumer{
		name:   name,
		events: make([]es.PersistedEvent, 0),
	}
}

func (p *testConsumer) Name() string {
	return p.name
}

func (p *testConsumer) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
	return nil
}

func (p *testConsumer) EventCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
}

func TestSegmentStore_InitializeSegments(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := mysql.NewStore(mysql.DefaultStoreConfig())

	tests := map[string]struct {
		consumerName  string
		totalSegments int
		wantErr       bool
	}{
		"initialize 4 segments": {
			consumerName:  "test_consumer_init",
			totalSegments: 4,
			wantErr:       false,
		},
		"initialize 1 segment": {
			consumerName:  "test_consumer_single",
			totalSegments: 1,
			wantErr:       false,
		},
		"initialize 10 segments": {
			consumerName:  "test_consumer_ten",
			totalSegments: 10,
			wantErr:       false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("Failed to begin transaction: %v", err)
			}
			defer tx.Rollback()

			err = store.InitializeSegments(ctx, tx, tt.consumerName, tt.totalSegments)
			if (err != nil) != tt.wantErr {
				t.Errorf("InitializeSegments() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err != nil {
				return
			}

			if err := tx.Commit(); err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			// Verify segments were created
			tx2, _ := db.BeginTx(ctx, nil)
			defer tx2.Rollback()

			segments, err := store.GetSegments(ctx, tx2, tt.consumerName)
			if err != nil {
				t.Fatalf("Failed to get segments: %v", err)
			}

			if len(segments) != tt.totalSegments {
				t.Errorf("Expected %d segments, got %d", tt.totalSegments, len(segments))
			}

			// Verify all segments are unclaimed
			for i, seg := range segments {
				if seg.SegmentID != i {
					t.Errorf("Segment %d: expected segment_id %d, got %d", i, i, seg.SegmentID)
				}
				if seg.OwnerID != nil {
					t.Errorf("Segment %d: expected unclaimed, got owner_id %s", i, *seg.OwnerID)
				}
				if seg.Checkpoint != 0 {
					t.Errorf("Segment %d: expected checkpoint 0, got %d", i, seg.Checkpoint)
				}
				if seg.TotalSegments != tt.totalSegments {
					t.Errorf("Segment %d: expected total_segments %d, got %d", i, tt.totalSegments, seg.TotalSegments)
				}
			}
		})
	}
}

func TestSegmentStore_InitializeSegments_Idempotent(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := mysql.NewStore(mysql.DefaultStoreConfig())
	consumerName := "test_consumer_idempotent"

	// Initialize segments first time
	tx1, _ := db.BeginTx(ctx, nil)
	err := store.InitializeSegments(ctx, tx1, consumerName, 4)
	if err != nil {
		t.Fatalf("First InitializeSegments() failed: %v", err)
	}
	tx1.Commit()

	// Initialize segments second time (should be idempotent)
	tx2, _ := db.BeginTx(ctx, nil)
	err = store.InitializeSegments(ctx, tx2, consumerName, 4)
	if err != nil {
		t.Fatalf("Second InitializeSegments() failed: %v", err)
	}
	tx2.Commit()

	// Verify still only 4 segments
	tx3, _ := db.BeginTx(ctx, nil)
	defer tx3.Rollback()

	segments, err := store.GetSegments(ctx, tx3, consumerName)
	if err != nil {
		t.Fatalf("Failed to get segments: %v", err)
	}

	if len(segments) != 4 {
		t.Errorf("Expected 4 segments after idempotent call, got %d", len(segments))
	}
}

func TestSegmentStore_ClaimAndRelease(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := mysql.NewStore(mysql.DefaultStoreConfig())
	consumerName := "test_consumer_claim"
	ownerID := uuid.New().String()

	// Initialize segments
	tx1, _ := db.BeginTx(ctx, nil)
	err := store.InitializeSegments(ctx, tx1, consumerName, 4)
	if err != nil {
		t.Fatalf("InitializeSegments() failed: %v", err)
	}
	tx1.Commit()

	// Claim one segment
	tx2, _ := db.BeginTx(ctx, nil)
	segment, err := store.ClaimSegment(ctx, tx2, consumerName, ownerID)
	if err != nil {
		t.Fatalf("ClaimSegment() failed: %v", err)
	}
	tx2.Commit()

	if segment == nil {
		t.Fatal("Expected segment to be claimed, got nil")
	}

	// Verify segment is claimed
	if segment.OwnerID == nil {
		t.Error("Expected segment to have owner_id, got nil")
	} else if *segment.OwnerID != ownerID {
		t.Errorf("Expected owner_id %s, got %s", ownerID, *segment.OwnerID)
	}

	if segment.SegmentID != 0 {
		t.Errorf("Expected segment_id 0 (first segment), got %d", segment.SegmentID)
	}

	if segment.ConsumerName != consumerName {
		t.Errorf("Expected consumer_name %s, got %s", consumerName, segment.ConsumerName)
	}

	// Release the segment
	tx3, _ := db.BeginTx(ctx, nil)
	err = store.ReleaseSegment(ctx, tx3, consumerName, segment.SegmentID, ownerID)
	if err != nil {
		t.Fatalf("ReleaseSegment() failed: %v", err)
	}
	tx3.Commit()

	// Verify segment is unclaimed
	tx4, _ := db.BeginTx(ctx, nil)
	defer tx4.Rollback()

	segments, err := store.GetSegments(ctx, tx4, consumerName)
	if err != nil {
		t.Fatalf("Failed to get segments: %v", err)
	}

	releasedSegment := segments[segment.SegmentID]
	if releasedSegment.OwnerID != nil {
		t.Errorf("Expected segment to be unclaimed after release, got owner_id %s", *releasedSegment.OwnerID)
	}
}

func TestSegmentStore_ClaimAllSegments(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := mysql.NewStore(mysql.DefaultStoreConfig())
	consumerName := "test_consumer_claim_all"
	ownerID := uuid.New().String()

	// Initialize 4 segments
	tx1, _ := db.BeginTx(ctx, nil)
	err := store.InitializeSegments(ctx, tx1, consumerName, 4)
	if err != nil {
		t.Fatalf("InitializeSegments() failed: %v", err)
	}
	tx1.Commit()

	// Claim all 4 segments
	claimed := make([]*int, 0, 4)
	for i := 0; i < 4; i++ {
		tx, _ := db.BeginTx(ctx, nil)
		segment, err := store.ClaimSegment(ctx, tx, consumerName, ownerID)
		if err != nil {
			t.Fatalf("ClaimSegment() iteration %d failed: %v", i, err)
		}
		tx.Commit()

		if segment == nil {
			t.Fatalf("Expected segment on iteration %d, got nil", i)
		}
		claimed = append(claimed, &segment.SegmentID)
	}

	if len(claimed) != 4 {
		t.Errorf("Expected 4 segments claimed, got %d", len(claimed))
	}

	// Try to claim a 5th segment (should return nil)
	tx5, _ := db.BeginTx(ctx, nil)
	segment5, err := store.ClaimSegment(ctx, tx5, consumerName, ownerID)
	if err != nil {
		t.Fatalf("ClaimSegment() for 5th segment failed: %v", err)
	}
	tx5.Commit()

	if segment5 != nil {
		t.Errorf("Expected nil when no segments available, got segment %d", segment5.SegmentID)
	}

	// Verify all segments are owned
	tx6, _ := db.BeginTx(ctx, nil)
	defer tx6.Rollback()

	segments, err := store.GetSegments(ctx, tx6, consumerName)
	if err != nil {
		t.Fatalf("Failed to get segments: %v", err)
	}

	for i, seg := range segments {
		if seg.OwnerID == nil {
			t.Errorf("Segment %d: expected to be claimed, got nil owner_id", i)
		}
	}
}

func TestSegmentStore_WorkerRegistry(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := mysql.NewStore(mysql.DefaultStoreConfig())
	consumerName := "test_consumer_registry"
	workerID := uuid.New().String()

	t.Run("register and count workers", func(t *testing.T) {
		tx1, _ := db.BeginTx(ctx, nil)
		err := store.RegisterWorker(ctx, tx1, consumerName, workerID)
		if err != nil {
			t.Fatalf("RegisterWorker() failed: %v", err)
		}
		tx1.Commit()

		tx2, _ := db.BeginTx(ctx, nil)
		count, err := store.CountActiveWorkers(ctx, tx2, consumerName, 30*time.Second)
		if err != nil {
			t.Fatalf("CountActiveWorkers() failed: %v", err)
		}
		tx2.Commit()

		if count != 1 {
			t.Errorf("Expected 1 active worker, got %d", count)
		}
	})

	t.Run("deregister reduces count", func(t *testing.T) {
		tx1, _ := db.BeginTx(ctx, nil)
		err := store.DeregisterWorker(ctx, tx1, consumerName, workerID)
		if err != nil {
			t.Fatalf("DeregisterWorker() failed: %v", err)
		}
		tx1.Commit()

		tx2, _ := db.BeginTx(ctx, nil)
		count, err := store.CountActiveWorkers(ctx, tx2, consumerName, 30*time.Second)
		if err != nil {
			t.Fatalf("CountActiveWorkers() failed: %v", err)
		}
		tx2.Commit()

		if count != 0 {
			t.Errorf("Expected 0 active workers after deregister, got %d", count)
		}
	})

	t.Run("register is upsert (idempotent)", func(t *testing.T) {
		tx1, _ := db.BeginTx(ctx, nil)
		err := store.RegisterWorker(ctx, tx1, consumerName, workerID)
		if err != nil {
			t.Fatalf("First RegisterWorker() failed: %v", err)
		}
		tx1.Commit()

		// Wait a bit
		time.Sleep(50 * time.Millisecond)

		tx2, _ := db.BeginTx(ctx, nil)
		err = store.RegisterWorker(ctx, tx2, consumerName, workerID)
		if err != nil {
			t.Fatalf("Second RegisterWorker() failed: %v", err)
		}
		tx2.Commit()

		tx3, _ := db.BeginTx(ctx, nil)
		count, err := store.CountActiveWorkers(ctx, tx3, consumerName, 30*time.Second)
		if err != nil {
			t.Fatalf("CountActiveWorkers() failed: %v", err)
		}
		tx3.Commit()

		if count != 1 {
			t.Errorf("Expected 1 active worker after double register, got %d", count)
		}
	})
}

func TestSegmentStore_ReclaimStaleSegments(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := mysql.NewStore(mysql.DefaultStoreConfig())
	consumerName := "test_consumer_reclaim"
	ownerID := uuid.New().String()

	// Initialize and register the worker
	tx1, _ := db.BeginTx(ctx, nil)
	err := store.InitializeSegments(ctx, tx1, consumerName, 3)
	if err != nil {
		t.Fatalf("InitializeSegments() failed: %v", err)
	}
	tx1.Commit()

	tx1b, _ := db.BeginTx(ctx, nil)
	err = store.RegisterWorker(ctx, tx1b, consumerName, ownerID)
	if err != nil {
		t.Fatalf("RegisterWorker() failed: %v", err)
	}
	tx1b.Commit()

	// Claim all 3 segments
	for i := 0; i < 3; i++ {
		tx, _ := db.BeginTx(ctx, nil)
		_, err := store.ClaimSegment(ctx, tx, consumerName, ownerID)
		if err != nil {
			t.Fatalf("ClaimSegment() failed: %v", err)
		}
		tx.Commit()
	}

	// Manually set worker's heartbeat to an old timestamp to simulate stale worker
	_, err = db.ExecContext(ctx, `
		UPDATE consumer_workers
		SET last_heartbeat = NOW(6) - INTERVAL 60 SECOND
		WHERE consumer_name = ? AND worker_id = ?
	`, consumerName, ownerID)
	if err != nil {
		t.Fatalf("Failed to set stale heartbeat: %v", err)
	}

	// Reclaim stale segments (threshold: 30 seconds)
	tx2, _ := db.BeginTx(ctx, nil)
	reclaimed, err := store.ReclaimStaleSegments(ctx, tx2, consumerName, 30*time.Second)
	if err != nil {
		t.Fatalf("ReclaimStaleSegments() failed: %v", err)
	}
	tx2.Commit()

	if reclaimed != 3 {
		t.Errorf("Expected 3 segments reclaimed, got %d", reclaimed)
	}

	// Verify segments are now unclaimed
	tx3, _ := db.BeginTx(ctx, nil)
	defer tx3.Rollback()

	segments, err := store.GetSegments(ctx, tx3, consumerName)
	if err != nil {
		t.Fatalf("Failed to get segments: %v", err)
	}

	for i, seg := range segments {
		if seg.OwnerID != nil {
			t.Errorf("Segment %d: expected to be unclaimed after reclaim, got owner_id %s", i, *seg.OwnerID)
		}
	}
}

func TestSegmentStore_ReclaimStaleSegments_NotStale(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := mysql.NewStore(mysql.DefaultStoreConfig())
	consumerName := "test_consumer_not_stale"
	ownerID := uuid.New().String()

	// Initialize and register worker
	tx1, _ := db.BeginTx(ctx, nil)
	err := store.InitializeSegments(ctx, tx1, consumerName, 2)
	if err != nil {
		t.Fatalf("InitializeSegments() failed: %v", err)
	}
	tx1.Commit()

	tx1b, _ := db.BeginTx(ctx, nil)
	err = store.RegisterWorker(ctx, tx1b, consumerName, ownerID)
	if err != nil {
		t.Fatalf("RegisterWorker() failed: %v", err)
	}
	tx1b.Commit()

	tx2, _ := db.BeginTx(ctx, nil)
	_, err = store.ClaimSegment(ctx, tx2, consumerName, ownerID)
	if err != nil {
		t.Fatalf("ClaimSegment() failed: %v", err)
	}
	tx2.Commit()

	// Try to reclaim with threshold (should not reclaim since worker is active)
	tx3, _ := db.BeginTx(ctx, nil)
	reclaimed, err := store.ReclaimStaleSegments(ctx, tx3, consumerName, 10*time.Second)
	if err != nil {
		t.Fatalf("ReclaimStaleSegments() failed: %v", err)
	}
	tx3.Commit()

	if reclaimed != 0 {
		t.Errorf("Expected 0 segments reclaimed (not stale), got %d", reclaimed)
	}

	// Verify segment is still claimed
	tx4, _ := db.BeginTx(ctx, nil)
	defer tx4.Rollback()

	segments, err := store.GetSegments(ctx, tx4, consumerName)
	if err != nil {
		t.Fatalf("Failed to get segments: %v", err)
	}

	if segments[0].OwnerID == nil {
		t.Error("Expected segment to still be claimed")
	}
}

func TestSegmentStore_GetSegments(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := mysql.NewStore(mysql.DefaultStoreConfig())
	consumerName := "test_consumer_get"
	owner1 := uuid.New().String()
	owner2 := uuid.New().String()

	// Initialize 6 segments
	tx1, _ := db.BeginTx(ctx, nil)
	err := store.InitializeSegments(ctx, tx1, consumerName, 6)
	if err != nil {
		t.Fatalf("InitializeSegments() failed: %v", err)
	}
	tx1.Commit()

	// Claim 2 segments with owner1
	for i := 0; i < 2; i++ {
		tx, _ := db.BeginTx(ctx, nil)
		_, err := store.ClaimSegment(ctx, tx, consumerName, owner1)
		if err != nil {
			t.Fatalf("ClaimSegment() for owner1 failed: %v", err)
		}
		tx.Commit()
	}

	// Claim 3 segments with owner2
	for i := 0; i < 3; i++ {
		tx, _ := db.BeginTx(ctx, nil)
		_, err := store.ClaimSegment(ctx, tx, consumerName, owner2)
		if err != nil {
			t.Fatalf("ClaimSegment() for owner2 failed: %v", err)
		}
		tx.Commit()
	}

	// Get all segments
	tx2, _ := db.BeginTx(ctx, nil)
	defer tx2.Rollback()

	segments, err := store.GetSegments(ctx, tx2, consumerName)
	if err != nil {
		t.Fatalf("GetSegments() failed: %v", err)
	}

	if len(segments) != 6 {
		t.Errorf("Expected 6 segments, got %d", len(segments))
	}

	// Count ownership
	var owner1Count, owner2Count, unclaimedCount int
	for _, seg := range segments {
		if seg.OwnerID == nil {
			unclaimedCount++
		} else if *seg.OwnerID == owner1 {
			owner1Count++
		} else if *seg.OwnerID == owner2 {
			owner2Count++
		}
	}

	if owner1Count != 2 {
		t.Errorf("Expected 2 segments owned by owner1, got %d", owner1Count)
	}
	if owner2Count != 3 {
		t.Errorf("Expected 3 segments owned by owner2, got %d", owner2Count)
	}
	if unclaimedCount != 1 {
		t.Errorf("Expected 1 unclaimed segment, got %d", unclaimedCount)
	}

	// Verify segments are ordered by segment_id
	for i, seg := range segments {
		if seg.SegmentID != i {
			t.Errorf("Segment %d: expected segment_id %d, got %d", i, i, seg.SegmentID)
		}
	}
}

func TestSegmentStore_Checkpoint(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := mysql.NewStore(mysql.DefaultStoreConfig())
	consumerName := "test_consumer_checkpoint"
	ownerID := uuid.New().String()

	// Initialize and claim a segment
	tx1, _ := db.BeginTx(ctx, nil)
	err := store.InitializeSegments(ctx, tx1, consumerName, 2)
	if err != nil {
		t.Fatalf("InitializeSegments() failed: %v", err)
	}
	tx1.Commit()

	tx2, _ := db.BeginTx(ctx, nil)
	segment, err := store.ClaimSegment(ctx, tx2, consumerName, ownerID)
	if err != nil {
		t.Fatalf("ClaimSegment() failed: %v", err)
	}
	tx2.Commit()

	if segment == nil {
		t.Fatal("Expected segment to be claimed, got nil")
	}

	// Verify initial checkpoint is 0
	tx3, _ := db.BeginTx(ctx, nil)
	checkpoint, err := store.GetSegmentCheckpoint(ctx, tx3, consumerName, segment.SegmentID)
	if err != nil {
		t.Fatalf("GetSegmentCheckpoint() failed: %v", err)
	}
	tx3.Commit()

	if checkpoint != 0 {
		t.Errorf("Expected initial checkpoint 0, got %d", checkpoint)
	}

	// Update checkpoint
	tx4, _ := db.BeginTx(ctx, nil)
	err = store.UpdateSegmentCheckpoint(ctx, tx4, consumerName, segment.SegmentID, 42)
	if err != nil {
		t.Fatalf("UpdateSegmentCheckpoint() failed: %v", err)
	}
	tx4.Commit()

	// Read checkpoint back
	tx5, _ := db.BeginTx(ctx, nil)
	checkpoint, err = store.GetSegmentCheckpoint(ctx, tx5, consumerName, segment.SegmentID)
	if err != nil {
		t.Fatalf("GetSegmentCheckpoint() after update failed: %v", err)
	}
	tx5.Commit()

	if checkpoint != 42 {
		t.Errorf("Expected checkpoint 42, got %d", checkpoint)
	}

	// Update to higher value
	tx6, _ := db.BeginTx(ctx, nil)
	err = store.UpdateSegmentCheckpoint(ctx, tx6, consumerName, segment.SegmentID, 100)
	if err != nil {
		t.Fatalf("UpdateSegmentCheckpoint() to 100 failed: %v", err)
	}
	tx6.Commit()

	// Verify new checkpoint
	tx7, _ := db.BeginTx(ctx, nil)
	defer tx7.Rollback()
	checkpoint, err = store.GetSegmentCheckpoint(ctx, tx7, consumerName, segment.SegmentID)
	if err != nil {
		t.Fatalf("GetSegmentCheckpoint() after second update failed: %v", err)
	}

	if checkpoint != 100 {
		t.Errorf("Expected checkpoint 100, got %d", checkpoint)
	}
}

func TestSegmentProcessor_OneOff(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := mysql.NewStore(mysql.DefaultStoreConfig())

	// Append test events across multiple aggregates to test partitioning
	aggregates := []string{
		uuid.New().String(),
		uuid.New().String(),
		uuid.New().String(),
		uuid.New().String(),
	}

	eventCount := 20
	tx, _ := db.BeginTx(ctx, nil)
	for i := 0; i < eventCount; i++ {
		aggregateID := aggregates[i%len(aggregates)]
		event := es.Event{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      fmt.Sprintf("Event%d", i+1),
			EventVersion:   1,
			Payload:        []byte(fmt.Sprintf(`{"num":%d}`, i+1)),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		}
		_, err := store.Append(ctx, tx, es.Any(), []es.Event{event})
		if err != nil {
			t.Fatalf("Failed to append event %d: %v", i, err)
		}
	}
	//nolint:errcheck
	tx.Commit()

	// Create test consumer
	cons := newTestConsumer("test_segment_processor")

	// Configure segment processor
	config := consumer.DefaultSegmentProcessorConfig()
	config.RunMode = consumer.RunModeOneOff
	config.TotalSegments = 4
	config.BatchSize = 5
	config.HeartbeatInterval = 100 * time.Millisecond
	config.RebalanceInterval = 200 * time.Millisecond

	processor := mysql.NewSegmentProcessor(db, store, config)

	// Run processor (should exit after processing all events)
	err := processor.Run(ctx, cons)
	if err != nil {
		t.Fatalf("SegmentProcessor.Run() failed: %v", err)
	}

	// Verify events were processed
	processedCount := cons.EventCount()
	if processedCount != eventCount {
		t.Errorf("Expected %d events processed, got %d", eventCount, processedCount)
	}

	// Verify segments were initialized
	tx2, _ := db.BeginTx(ctx, nil)
	segments, err := store.GetSegments(ctx, tx2, cons.Name())
	if err != nil {
		t.Fatalf("Failed to get segments: %v", err)
	}
	tx2.Commit()

	if len(segments) != 4 {
		t.Errorf("Expected 4 segments, got %d", len(segments))
	}

	// Verify all segments are unclaimed (released after processing)
	for i, seg := range segments {
		if seg.OwnerID != nil {
			t.Errorf("Segment %d: expected to be released after processing, got owner_id %s", i, *seg.OwnerID)
		}
		if seg.Checkpoint <= 0 {
			t.Errorf("Segment %d: expected checkpoint > 0, got %d", i, seg.Checkpoint)
		}
	}

	// Verify checkpoints are reasonable (each segment processed some events)
	totalCheckpoint := int64(0)
	for _, seg := range segments {
		totalCheckpoint += seg.Checkpoint
	}

	// Note: Due to partitioning, not all segments may have the same checkpoint
	// but the total should be at least eventCount (can be higher if events were read multiple times)
	if totalCheckpoint < int64(eventCount) {
		t.Errorf("Expected total checkpoint >= %d, got %d", eventCount, totalCheckpoint)
	}
}

func TestSegmentStore_ConcurrentClaim(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := mysql.NewStore(mysql.DefaultStoreConfig())
	consumerName := "test_consumer_concurrent"

	// Initialize 4 segments
	tx1, _ := db.BeginTx(ctx, nil)
	err := store.InitializeSegments(ctx, tx1, consumerName, 4)
	if err != nil {
		t.Fatalf("InitializeSegments() failed: %v", err)
	}
	tx1.Commit()

	// Start 8 goroutines trying to claim segments concurrently
	var wg sync.WaitGroup
	claimedSegments := make(chan *int, 8)
	errors := make(chan error, 8)

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			ownerID := fmt.Sprintf("owner-%d", id)
			tx, txErr := db.BeginTx(ctx, nil)
			if txErr != nil {
				errors <- txErr
				return
			}
			defer tx.Rollback()

			segment, claimErr := store.ClaimSegment(ctx, tx, consumerName, ownerID)
			if claimErr != nil {
				errors <- claimErr
				return
			}

			if commitErr := tx.Commit(); commitErr != nil {
				errors <- commitErr
				return
			}

			if segment != nil {
				claimedSegments <- &segment.SegmentID
			} else {
				claimedSegments <- nil
			}
		}(i)
	}

	wg.Wait()
	close(claimedSegments)
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent claim error: %v", err)
	}

	// Count successful claims
	successfulClaims := 0
	nilResults := 0
	claimedIDs := make(map[int]bool)

	for segmentID := range claimedSegments {
		if segmentID == nil {
			nilResults++
		} else {
			successfulClaims++
			if claimedIDs[*segmentID] {
				t.Errorf("Segment %d was claimed by multiple owners", *segmentID)
			}
			claimedIDs[*segmentID] = true
		}
	}

	// Exactly 4 should succeed, 4 should get nil
	if successfulClaims != 4 {
		t.Errorf("Expected exactly 4 successful claims, got %d", successfulClaims)
	}
	if nilResults != 4 {
		t.Errorf("Expected exactly 4 nil results, got %d", nilResults)
	}

	// Verify all segments are claimed
	tx2, _ := db.BeginTx(ctx, nil)
	defer tx2.Rollback()

	segments, err := store.GetSegments(ctx, tx2, consumerName)
	if err != nil {
		t.Fatalf("Failed to get segments: %v", err)
	}

	for i, seg := range segments {
		if seg.OwnerID == nil {
			t.Errorf("Segment %d: expected to be claimed, got nil owner_id", i)
		}
	}
}

func TestSegmentStore_ReleaseSegment_WrongOwner(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := mysql.NewStore(mysql.DefaultStoreConfig())
	consumerName := "test_consumer_wrong_owner"
	owner1 := uuid.New().String()
	owner2 := uuid.New().String()

	// Initialize and claim segment with owner1
	tx1, _ := db.BeginTx(ctx, nil)
	err := store.InitializeSegments(ctx, tx1, consumerName, 2)
	if err != nil {
		t.Fatalf("InitializeSegments() failed: %v", err)
	}
	tx1.Commit()

	tx2, _ := db.BeginTx(ctx, nil)
	segment, err := store.ClaimSegment(ctx, tx2, consumerName, owner1)
	if err != nil {
		t.Fatalf("ClaimSegment() failed: %v", err)
	}
	tx2.Commit()

	if segment == nil {
		t.Fatal("Expected segment to be claimed, got nil")
	}

	// Try to release with owner2 (wrong owner)
	tx3, _ := db.BeginTx(ctx, nil)
	err = store.ReleaseSegment(ctx, tx3, consumerName, segment.SegmentID, owner2)
	if err != nil {
		t.Fatalf("ReleaseSegment() with wrong owner failed: %v", err)
	}
	tx3.Commit()

	// Verify segment is still claimed by owner1
	tx4, _ := db.BeginTx(ctx, nil)
	defer tx4.Rollback()

	segments, err := store.GetSegments(ctx, tx4, consumerName)
	if err != nil {
		t.Fatalf("Failed to get segments: %v", err)
	}

	claimedSegment := segments[segment.SegmentID]
	if claimedSegment.OwnerID == nil {
		t.Error("Expected segment to still be claimed")
	} else if *claimedSegment.OwnerID != owner1 {
		t.Errorf("Expected segment to be owned by owner1 (%s), got %s", owner1, *claimedSegment.OwnerID)
	}
}
