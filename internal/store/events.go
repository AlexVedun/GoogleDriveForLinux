package store

import (
	"context"
	"fmt"
	"time"

	"google-drive-linux/internal/domain"
)

func (s *Store) RecentEvents(ctx context.Context, after time.Time, limit int) ([]domain.Event, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT event_id, object_id, operation_id, kind, code, details_json, created_at,
		COALESCE(resolved_at, '') FROM events WHERE created_at > ? ORDER BY created_at, event_id LIMIT ?`, formatTime(after), limit)
	if err != nil {
		return nil, fmt.Errorf("list recent events: %w", err)
	}
	defer rows.Close()
	events := make([]domain.Event, 0)
	for rows.Next() {
		var event domain.Event
		var eventID, objectID, operationID, kind, createdAt, resolvedAt string
		if err := rows.Scan(&eventID, &objectID, &operationID, &kind, &event.Code, &event.DetailsJSON, &createdAt, &resolvedAt); err != nil {
			return nil, fmt.Errorf("read recent event: %w", err)
		}
		event.ID, event.Kind = domain.EventID(eventID), domain.EventKind(kind)
		if objectID != "" {
			id := domain.ObjectID(objectID)
			event.ObjectID = &id
		}
		if operationID != "" {
			id := domain.OperationID(operationID)
			event.OperationID = &id
		}
		event.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
		if err != nil {
			return nil, fmt.Errorf("parse event creation time: %w", err)
		}
		if resolvedAt != "" {
			event.ResolvedAt, err = time.Parse(time.RFC3339Nano, resolvedAt)
			if err != nil {
				return nil, fmt.Errorf("parse event resolution time: %w", err)
			}
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read recent events: %w", err)
	}
	return events, nil
}
