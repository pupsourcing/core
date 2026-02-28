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

// InitializeSegments implements store.SegmentStore.
func (s *Store) InitializeSegments(ctx context.Context, tx es.DBTX, consumerName string, totalSegments int) error {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "initializing segments",
			"consumer_name", consumerName,
			"total_segments", totalSegments)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (consumer_name, segment_id, total_segments, owner_id, checkpoint, last_heartbeat)
		VALUES ($1, $2, $3, NULL, 0, NULL)
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
		SET owner_id = $1, last_heartbeat = NOW()
		WHERE (consumer_name, segment_id) = (
			SELECT consumer_name, segment_id
			FROM %s
			WHERE consumer_name = $2 AND owner_id IS NULL
			ORDER BY segment_id ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING consumer_name, segment_id, total_segments, owner_id, checkpoint, last_heartbeat
	`, s.config.SegmentsTable, s.config.SegmentsTable)

	var seg store.Segment
	var ownerIDPtr *string
	var lastHeartbeat *time.Time

	err := tx.QueryRowContext(ctx, query, ownerID, consumerName).Scan(
		&seg.ConsumerName,
		&seg.SegmentID,
		&seg.TotalSegments,
		&ownerIDPtr,
		&seg.Checkpoint,
		&lastHeartbeat,
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
	seg.LastHeartbeat = lastHeartbeat

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
		SET owner_id = NULL, last_heartbeat = NULL
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

// Heartbeat implements store.SegmentStore.
func (s *Store) Heartbeat(ctx context.Context, tx es.DBTX, consumerName, ownerID string) error {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "heartbeat",
			"consumer_name", consumerName,
			"owner_id", ownerID)
	}

	query := fmt.Sprintf(`
		UPDATE %s 
		SET last_heartbeat = NOW()
		WHERE consumer_name = $1 AND owner_id = $2
	`, s.config.SegmentsTable)

	result, err := tx.ExecContext(ctx, query, consumerName, ownerID)
	if err != nil {
		return fmt.Errorf("failed to update heartbeat: %w", err)
	}

	if s.config.Logger != nil {
		if rowsAffected, err := result.RowsAffected(); err == nil {
			s.config.Logger.Debug(ctx, "heartbeat updated",
				"consumer_name", consumerName,
				"owner_id", ownerID,
				"segments_updated", rowsAffected)
		}
	}

	return nil
}

// ReclaimStaleSegments implements store.SegmentStore.
func (s *Store) ReclaimStaleSegments(ctx context.Context, tx es.DBTX, consumerName string, staleThreshold time.Duration) (int, error) {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "reclaiming stale segments",
			"consumer_name", consumerName,
			"stale_threshold", staleThreshold)
	}

	query := fmt.Sprintf(`
		UPDATE %s 
		SET owner_id = NULL, last_heartbeat = NULL
		WHERE consumer_name = $1 AND owner_id IS NOT NULL
		  AND last_heartbeat < NOW() - make_interval(secs => $2)
	`, s.config.SegmentsTable)

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
		SELECT consumer_name, segment_id, total_segments, owner_id, checkpoint, last_heartbeat
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
		var lastHeartbeat *time.Time

		err := rows.Scan(
			&seg.ConsumerName,
			&seg.SegmentID,
			&seg.TotalSegments,
			&ownerIDPtr,
			&seg.Checkpoint,
			&lastHeartbeat,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan segment: %w", err)
		}

		seg.OwnerID = ownerIDPtr
		seg.LastHeartbeat = lastHeartbeat

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
