package indexer

import (
	"context"
	"errors"
	"fmt"

	"google-drive-linux/internal/driveapi"
	"google-drive-linux/internal/store"
)

// Poller applies the Drive changes feed with one persisted in-flight cursor.
// A single Poll call is allowed at a time for each instance.
type Poller struct {
	drive   driveapi.API
	store   *store.Store
	indexer *Indexer
	gate    chan struct{}
}

func NewPoller(drive driveapi.API, metadata *store.Store) (*Poller, error) {
	indexer, err := New(drive, metadata)
	if err != nil {
		return nil, err
	}
	return &Poller{drive: drive, store: metadata, indexer: indexer, gate: make(chan struct{}, 1)}, nil
}

// Poll reads every currently available changes page. Each page's changes and
// continuation cursor are committed atomically. Interrupted polls resume from
// the last page cursor on the next call.
func (p *Poller) Poll(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context must not be nil")
	}
	select {
	case p.gate <- struct{}{}:
		defer func() { <-p.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	status, err := p.store.InitialIndexStatus(ctx)
	if err != nil {
		return err
	}
	if status.State != "ready" {
		return p.indexer.Run(ctx)
	}
	pageToken, err := p.store.BeginChangePoll(ctx)
	if err != nil {
		return err
	}
	for {
		page, err := p.drive.ListChanges(ctx, pageToken)
		if err != nil {
			if errors.Is(err, driveapi.ErrInvalidPageToken) {
				if cancelErr := p.store.CancelChangePoll(ctx); cancelErr != nil {
					return errors.Join(fmt.Errorf("discard rejected Drive changes token: %w", err), cancelErr)
				}
				return p.indexer.Reindex(ctx)
			}
			return fmt.Errorf("list incremental Drive changes: %w", err)
		}
		changes, err := remoteChanges(page.Changes)
		if err != nil {
			return err
		}
		if err := p.store.CommitChangePage(ctx, pageToken, changes, page.NextPageToken, page.NewStartPageToken); err != nil {
			return err
		}
		status, err := p.store.InitialIndexStatus(ctx)
		if err != nil {
			return err
		}
		if status.State == "indexing" {
			return p.indexer.Run(ctx)
		}
		if status.State != "ready" {
			return fmt.Errorf("incremental poll entered unexpected index state %q", status.State)
		}
		if page.NextPageToken == "" {
			return nil
		}
		pageToken = page.NextPageToken
	}
}
