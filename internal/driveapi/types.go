package driveapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"time"
)

type API interface {
	ListFiles(ctx context.Context, options FileListOptions) (FileList, error)
	GetFile(ctx context.Context, fileID string) (File, error)
	CreateFile(ctx context.Context, metadata FileMetadata) (File, error)
	UpdateFile(ctx context.Context, fileID string, patch FilePatch, addParents, removeParents []string) (File, error)
	DownloadFile(ctx context.Context, fileID string) (Download, error)
	TrashFile(ctx context.Context, fileID string) (File, error)
	MoveRename(ctx context.Context, fileID, newName, addParentID, removeParentID string) (File, error)
	GetStartPageToken(ctx context.Context) (string, error)
	ListChanges(ctx context.Context, pageToken string) (ChangeList, error)
	GenerateIDs(ctx context.Context, count int) ([]string, error)
	StartResumableUpload(ctx context.Context, upload UploadRequest) (string, error)
	UploadChunk(ctx context.Context, sessionURL string, chunk UploadChunkRequest) (UploadProgress, error)
	QueryUploadStatus(ctx context.Context, sessionURL string, total int64) (UploadProgress, error)
}

const (
	defaultFileFields = "id,name,mimeType,parents,size,version,modifiedTime,webViewLink,md5Checksum,trashed,driveId,appProperties,capabilities(canEdit,canTrash,canDownload,canRename,canMoveItemWithinDrive,canAddChildren,canRemoveChildren)"
	fileListFields    = "nextPageToken,files(" + defaultFileFields + ")"
	changeListFields  = "nextPageToken,newStartPageToken,changes(fileId,removed,time,file(" + defaultFileFields + "))"
)

type Capabilities struct {
	CanEdit                bool `json:"canEdit"`
	CanTrash               bool `json:"canTrash"`
	CanDownload            bool `json:"canDownload"`
	CanRename              bool `json:"canRename"`
	CanMoveItemWithinDrive bool `json:"canMoveItemWithinDrive"`
	CanAddChildren         bool `json:"canAddChildren"`
	CanRemoveChildren      bool `json:"canRemoveChildren"`
}

type File struct {
	ID            string
	Name          string
	MIMEType      string
	Parents       []string
	Size          int64
	Version       string
	ModifiedTime  time.Time
	WebViewLink   string
	MD5Checksum   string
	Trashed       bool
	DriveID       string
	AppProperties map[string]string
	Capabilities  Capabilities
}

func (f *File) UnmarshalJSON(data []byte) error {
	var raw struct {
		ID            string            `json:"id"`
		Name          string            `json:"name"`
		MIMEType      string            `json:"mimeType"`
		Parents       []string          `json:"parents"`
		Size          json.RawMessage   `json:"size"`
		Version       string            `json:"version"`
		ModifiedTime  time.Time         `json:"modifiedTime"`
		WebViewLink   string            `json:"webViewLink"`
		MD5Checksum   string            `json:"md5Checksum"`
		Trashed       bool              `json:"trashed"`
		DriveID       string            `json:"driveId"`
		AppProperties map[string]string `json:"appProperties"`
		Capabilities  Capabilities      `json:"capabilities"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	var size int64
	if len(raw.Size) != 0 && !bytes.Equal(raw.Size, []byte("null")) {
		value := raw.Size
		if len(value) > 0 && value[0] == '"' {
			var encoded string
			if err := json.Unmarshal(value, &encoded); err != nil {
				return err
			}
			value = []byte(encoded)
		}
		parsed, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil || parsed < 0 {
			return errors.New("invalid Drive file size")
		}
		size = parsed
	}
	*f = File{
		ID: raw.ID, Name: raw.Name, MIMEType: raw.MIMEType, Parents: raw.Parents,
		Size: size, Version: raw.Version, ModifiedTime: raw.ModifiedTime,
		WebViewLink: raw.WebViewLink, MD5Checksum: raw.MD5Checksum, Trashed: raw.Trashed,
		DriveID: raw.DriveID, AppProperties: raw.AppProperties, Capabilities: raw.Capabilities,
	}
	return nil
}

type FileMetadata struct {
	ID            string            `json:"id,omitempty"`
	Name          string            `json:"name,omitempty"`
	MIMEType      string            `json:"mimeType,omitempty"`
	Parents       []string          `json:"parents,omitempty"`
	AppProperties map[string]string `json:"appProperties,omitempty"`
}

type FilePatch struct {
	Name          *string           `json:"name,omitempty"`
	Trashed       *bool             `json:"trashed,omitempty"`
	AppProperties map[string]string `json:"appProperties,omitempty"`
}

type FileListOptions struct {
	Query     string
	PageToken string
	PageSize  int
}

type FileList struct {
	Files         []File `json:"files"`
	NextPageToken string `json:"nextPageToken"`
}

type Change struct {
	FileID  string    `json:"fileId"`
	Removed bool      `json:"removed"`
	Time    time.Time `json:"time"`
	File    *File     `json:"file"`
}

type ChangeList struct {
	Changes           []Change `json:"changes"`
	NextPageToken     string   `json:"nextPageToken"`
	NewStartPageToken string   `json:"newStartPageToken"`
}

type Download struct {
	Body          io.ReadCloser
	ContentType   string
	ContentLength int64
}

type UploadRequest struct {
	FileID   string
	Metadata FileMetadata
	Size     int64
	MIMEType string
}

type UploadChunkRequest struct {
	Start    int64
	End      int64
	Total    int64
	MIMEType string
	Body     io.Reader
	Empty    bool
}

type UploadProgress struct {
	Complete   bool
	NextOffset int64
	File       *File
}
