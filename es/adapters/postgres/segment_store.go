package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/store"
)

var _ store.SegmentStore = (*Store)(nil)

// ErrSegmentNotOwned indicates that a worker attempted to operate on a segment
// it no longer owns.
var ErrSegmentNotOwned = errors.New("segment not owned by worker")

// InitializeSegments implements store.SegmentStore.
func (s *Store) InitializeSegments(ctx context.Context, tx es.DBTX, consumerName string, totalSegments int) error {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "initializing segments",
			"consumer_name", consumerName,
			"total_segments", totalSegments)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (consumer_name, segment_id, total_segments, owner_id, checkpoint)
		VALUES ($1, $2, $3, NULL, 0)
		ON CONFLICT (consumer_name, segment_id) DO NOTHING
	`, s.config.SegmentsTable)

	for segmentID := 0; segmentID < totalSegments; segmentID++ {
		_, err := tx.ExecContext(ctx, query, consumerName, segmentID, totalSegments)
		if err != nil {
			return fmt.Errorf("failed to initialize segment %d: %w", segmentID, err)
		}
	}

	if s.config.Logger != nil {
		s.config.Logger.Info(ctx, "segments initialized",
			"consumer_name", consumerName,
			"total_segments", totalSegments)
	}

	return nil
}

// ClaimSegment implements store.SegmentStore.
func (s *Store) ClaimSegment(ctx context.Context, tx es.DBTX, consumerName, ownerID string) (*store.Segment, error) {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "claiming segment",
			"consumer_name", consumerName,
			"owner_id", ownerID)
	}

	query := fmt.Sprintf(`
		UPDATE %s 
		SET owner_id = $1
		WHERE (consumer_name, segment_id) = (
			SELECT consumer_name, segment_id
			FROM %s
			WHERE consumer_name = $2 AND owner_id IS NULL
			ORDER BY segment_id ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING consumer_name, segment_id, total_segments, owner_id, checkpoint
	`, s.config.SegmentsTable, s.config.SegmentsTable)

	var seg store.Segment
	var ownerIDPtr *string

	err := tx.QueryRowContext(ctx, query, ownerID, consumerName).Scan(
		&seg.ConsumerName,
		&seg.SegmentID,
		&seg.TotalSegments,
		&ownerIDPtr,
		&seg.Checkpoint,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if s.config.Logger != nil {
				s.config.Logger.Debug(ctx, "no unclaimed segments available",
					"consumer_name", consumerName,
					"owner_id", ownerID)
			}
			return nil, nil
		}
		return nil, fmt.Errorf("failed to claim segment: %w", err)
	}

	seg.OwnerID = ownerIDPtr

	if s.config.Logger != nil {
		s.config.Logger.Info(ctx, "segment claimed",
			"consumer_name", consumerName,
			"segment_id", seg.SegmentID,
			"owner_id", ownerID)
	}

	return &seg, nil
}

// ReleaseSegment implements store.SegmentStore.
func (s *Store) ReleaseSegment(ctx context.Context, tx es.DBTX, consumerName string, segmentID int, ownerID string) error {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "releasing segment",
			"consumer_name", consumerName,
			"segment_id", segmentID,
			"owner_id", ownerID)
	}

	query := fmt.Sprintf(`
		UPDATE %s 
		SET owner_id = NULL
		WHERE consumer_name = $1 AND segment_id = $2 AND owner_id = $3
	`, s.config.SegmentsTable)

	result, err := tx.ExecContext(ctx, query, consumerName, segmentID, ownerID)
	if err != nil {
		return fmt.Errorf("failed to release segment: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if s.config.Logger != nil {
		if rowsAffected > 0 {
			s.config.Logger.Info(ctx, "segment released",
				"consumer_name", consumerName,
				"segment_id", segmentID,
				"owner_id", ownerID)
		} else {
			s.config.Logger.Debug(ctx, "segment release no-op (not owned by this owner)",
				"consumer_name", consumerName,
				"segment_id", segmentID,
				"owner_id", ownerID)
		}
	}

	return nil
}

// ReclaimStaleSegments implements store.SegmentStore.
// Releases segments owned by workers not present in the active worker registry.
func (s *Store) ReclaimStaleSegments(ctx context.Context, tx es.DBTX, consumerName string, staleThreshold time.Duration) (int, error) {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "reclaiming stale segments",
			"consumer_name", consumerName,
			"stale_threshold", staleThreshold)
	}

	query := fmt.Sprintf(`
		UPDATE %s 
		SET owner_id = NULL
		WHERE consumer_name = $1 AND owner_id IS NOT NULL
		  AND owner_id NOT IN (
		    SELECT worker_id FROM %s
		    WHERE consumer_name = $1
		      AND last_heartbeat >= NOW() - make_interval(secs => $2)
		  )
	`, s.config.SegmentsTable, s.config.WorkerRegistryTable)

	result, err := tx.ExecContext(ctx, query, consumerName, staleThreshold.Seconds())
	if err != nil {
		return 0, fmt.Errorf("failed to reclaim stale segments: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	count := int(rowsAffected)

	if s.config.Logger != nil {
		if count > 0 {
			s.config.Logger.Info(ctx, "stale segments reclaimed",
				"consumer_name", consumerName,
				"count", count)
		} else {
			s.config.Logger.Debug(ctx, "no stale segments to reclaim",
				"consumer_name", consumerName)
		}
	}

	return count, nil
}

// GetSegments implements store.SegmentStore.
func (s *Store) GetSegments(ctx context.Context, tx es.DBTX, consumerName string) ([]store.Segment, error) {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "getting segments",
			"consumer_name", consumerName)
	}

	query := fmt.Sprintf(`
		SELECT consumer_name, segment_id, total_segments, owner_id, checkpoint
		FROM %s
		WHERE consumer_name = $1
		ORDER BY segment_id ASC
	`, s.config.SegmentsTable)

	rows, err := tx.QueryContext(ctx, query, consumerName)
	if err != nil {
		return nil, fmt.Errorf("failed to query segments: %w", err)
	}
	defer rows.Close()

	var segments []store.Segment
	for rows.Next() {
		var seg store.Segment
		var ownerIDPtr *string

		err := rows.Scan(
			&seg.ConsumerName,
			&seg.SegmentID,
			&seg.TotalSegments,
			&ownerIDPtr,
			&seg.Checkpoint,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan segment: %w", err)
		}

		seg.OwnerID = ownerIDPtr

		segments = append(segments, seg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "segments retrieved",
			"consumer_name", consumerName,
			"count", len(segments))
	}

	return segments, nil
}

// GetSegmentCheckpoint implements store.SegmentStore.
func (s *Store) GetSegmentCheckpoint(ctx context.Context, tx es.DBTX, consumerName string, segmentID int) (int64, error) {
	query := fmt.Sprintf(`
		SELECT checkpoint 
		FROM %s
		WHERE consumer_name = $1 AND segment_id = $2
	`, s.config.SegmentsTable)

	var checkpoint int64
	err := tx.QueryRowContext(ctx, query, consumerName, segmentID).Scan(&checkpoint)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get segment checkpoint: %w", err)
	}

	return checkpoint, nil
}

func (s *Store) getOwnedSegmentCheckpoint(
	ctx context.Context,
	tx es.DBTX,
	consumerName string,
	segmentID int,
	ownerID string,
) (int64, error) {
	query := fmt.Sprintf(`
		SELECT checkpoint 
		FROM %s
		WHERE consumer_name = $1 AND segment_id = $2 AND owner_id = $3
	`, s.config.SegmentsTable)

	var checkpoint int64
	err := tx.QueryRowContext(ctx, query, consumerName, segmentID, ownerID).Scan(&checkpoint)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrSegmentNotOwned
		}
		return 0, fmt.Errorf("failed to get owned segment checkpoint: %w", err)
	}

	return checkpoint, nil
}

// UpdateSegmentCheckpoint implements store.SegmentStore.
func (s *Store) UpdateSegmentCheckpoint(ctx context.Context, tx es.DBTX, consumerName string, segmentID int, position int64) error {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "updating segment checkpoint",
			"consumer_name", consumerName,
			"segment_id", segmentID,
			"position", position)
	}

	query := fmt.Sprintf(`
		UPDATE %s 
		SET checkpoint = $1
		WHERE consumer_name = $2 AND segment_id = $3
	`, s.config.SegmentsTable)

	_, err := tx.ExecContext(ctx, query, position, consumerName, segmentID)
	if err != nil {
		return fmt.Errorf("failed to update segment checkpoint: %w", err)
	}

	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "segment checkpoint updated",
			"consumer_name", consumerName,
			"segment_id", segmentID,
			"position", position)
	}

	return nil
}

func (s *Store) updateOwnedSegmentCheckpoint(
	ctx context.Context,
	tx es.DBTX,
	consumerName string,
	segmentID int,
	ownerID string,
	position int64,
) error {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "updating owned segment checkpoint",
			"consumer_name", consumerName,
			"segment_id", segmentID,
			"owner_id", ownerID,
			"position", position)
	}

	query := fmt.Sprintf(`
		UPDATE %s 
		SET checkpoint = $1
		WHERE consumer_name = $2 AND segment_id = $3 AND owner_id = $4
	`, s.config.SegmentsTable)

	result, err := tx.ExecContext(ctx, query, position, consumerName, segmentID, ownerID)
	if err != nil {
		return fmt.Errorf("failed to update owned segment checkpoint: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return ErrSegmentNotOwned
	}

	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "owned segment checkpoint updated",
			"consumer_name", consumerName,
			"segment_id", segmentID,
			"owner_id", ownerID,
			"position", position)
	}

	return nil
}

// --- Worker Registry ---

// RegisterWorker implements store.SegmentStore.
func (s *Store) RegisterWorker(ctx context.Context, tx es.DBTX, consumerName, workerID string) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (consumer_name, worker_id, last_heartbeat)
		VALUES ($1, $2, NOW())
		ON CONFLICT (consumer_name, worker_id) DO UPDATE
		SET last_heartbeat = NOW()
	`, s.config.WorkerRegistryTable)

	_, err := tx.ExecContext(ctx, query, consumerName, workerID)
	if err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}

	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "worker registered",
			"consumer_name", consumerName,
			"worker_id", workerID)
	}

	return nil
}

// DeregisterWorker implements store.SegmentStore.
func (s *Store) DeregisterWorker(ctx context.Context, tx es.DBTX, consumerName, workerID string) error {
	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE consumer_name = $1 AND worker_id = $2
	`, s.config.WorkerRegistryTable)

	_, err := tx.ExecContext(ctx, query, consumerName, workerID)
	if err != nil {
		return fmt.Errorf("failed to deregister worker: %w", err)
	}

	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "worker deregistered",
			"consumer_name", consumerName,
			"worker_id", workerID)
	}

	return nil
}

// CountActiveWorkers implements store.SegmentStore.
func (s *Store) CountActiveWorkers(
	ctx context.Context, tx es.DBTX, consumerName string, staleThreshold time.Duration,
) (int, error) {
	query := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s
		WHERE consumer_name = $1
		  AND last_heartbeat >= NOW() - make_interval(secs => $2)
	`, s.config.WorkerRegistryTable)

	var count int
	err := tx.QueryRowContext(ctx, query, consumerName, staleThreshold.Seconds()).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count active workers: %w", err)
	}

	return count, nil
}

// ListActiveWorkers implements store.SegmentStore.
func (s *Store) ListActiveWorkers(
	ctx context.Context, tx es.DBTX, consumerName string, staleThreshold time.Duration,
) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT worker_id FROM %s
		WHERE consumer_name = $1
		  AND last_heartbeat >= NOW() - make_interval(secs => $2)
		ORDER BY worker_id
	`, s.config.WorkerRegistryTable)

	rows, err := tx.QueryContext(ctx, query, consumerName, staleThreshold.Seconds())
	if err != nil {
		return nil, fmt.Errorf("failed to list active workers: %w", err)
	}
	defer rows.Close()

	var workers []string
	for rows.Next() {
		var workerID string
		if err := rows.Scan(&workerID); err != nil {
			return nil, fmt.Errorf("failed to scan worker ID: %w", err)
		}
		workers = append(workers, workerID)
	}

	return workers, rows.Err()
}

// PurgeStaleWorkers implements store.SegmentStore.
func (s *Store) PurgeStaleWorkers(
	ctx context.Context, tx es.DBTX, consumerName string, staleThreshold time.Duration,
) error {
	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE consumer_name = $1
		  AND last_heartbeat < NOW() - make_interval(secs => $2)
	`, s.config.WorkerRegistryTable)

	result, err := tx.ExecContext(ctx, query, consumerName, staleThreshold.Seconds())
	if err != nil {
		return fmt.Errorf("failed to purge stale workers: %w", err)
	}

	if s.config.Logger != nil {
		if rowsAffected, err := result.RowsAffected(); err == nil && rowsAffected > 0 {
			s.config.Logger.Info(ctx, "stale workers purged",
				"consumer_name", consumerName,
				"count", rowsAffected)
		}
	}

	return nil
}
