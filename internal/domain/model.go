package domain

import "time"

type ObjectID string
type DriveFileID string
type Inode uint64
type ContentVersionID string
type OperationID string
type EventID string

const RootObjectID ObjectID = "root"

const DriveOperationProperty = "gdlOperationId"

type Object struct {
	ID               ObjectID
	DriveFileID      *DriveFileID
	Inode            Inode
	MIMEType         string
	Size             int64
	DriveVersion     string
	ModifiedAt       time.Time
	WebViewLink      string
	CapabilitiesJSON string
	Deleted          bool
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type DirectoryEntry struct {
	ParentID    ObjectID
	ObjectID    ObjectID
	RemoteName  string
	DisplayName string
}

type ContentVersionKind string

const (
	ContentCached   ContentVersionKind = "cached"
	ContentPending  ContentVersionKind = "pending"
	ContentConflict ContentVersionKind = "conflict"
)

type ContentVersion struct {
	ID           ContentVersionID
	ObjectID     ObjectID
	Kind         ContentVersionKind
	LocalPath    string
	Size         int64
	SHA256       string
	DriveVersion string
	Complete     bool
	Current      bool
	CreatedAt    time.Time
}

type OperationKind string

const (
	OperationCreate OperationKind = "create"
	OperationWrite  OperationKind = "write"
	OperationMkdir  OperationKind = "mkdir"
	OperationRename OperationKind = "rename"
	OperationTrash  OperationKind = "trash"
)

type OperationState string

const (
	OperationPrepared       OperationState = "prepared"
	OperationQueued         OperationState = "queued"
	OperationUploading      OperationState = "uploading"
	OperationCommitted      OperationState = "committed"
	OperationConflict       OperationState = "conflict"
	OperationRetryableError OperationState = "retryable-error"
	OperationPermanentError OperationState = "permanent-error"
	OperationNeedsReconcile OperationState = "needs-reconcile"
)

type Operation struct {
	ID                  OperationID
	ObjectID            ObjectID
	Kind                OperationKind
	State               OperationState
	IdempotencyKey      string
	BaseDriveVersion    string
	ContentVersionID    *ContentVersionID
	PayloadJSON         string
	ResumableSessionURL string
	ResumableOffset     int64
	AttemptCount        int
	NextAttemptAt       time.Time
	LastErrorCode       string
	LastErrorMessage    string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type Account struct {
	GoogleUserID string
	Email        string
	DisplayName  string
	AuthState    string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type SyncState struct {
	ChangeToken string
	IndexState  string
	UpdatedAt   time.Time
}

type EventKind string

const (
	EventInfo     EventKind = "info"
	EventError    EventKind = "error"
	EventConflict EventKind = "conflict"
)

type Event struct {
	ID          EventID      `json:"id"`
	ObjectID    *ObjectID    `json:"object_id,omitempty"`
	OperationID *OperationID `json:"operation_id,omitempty"`
	Kind        EventKind    `json:"kind"`
	Code        string       `json:"code"`
	DetailsJSON string       `json:"details_json"`
	CreatedAt   time.Time    `json:"created_at"`
	ResolvedAt  time.Time    `json:"resolved_at,omitempty"`
}
