package ipc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

const ProtocolVersion = 1

type Command string

const (
	CommandStatus          Command = "status"
	CommandQueueList       Command = "queue.list"
	CommandEventsList      Command = "events.list"
	CommandConflictsList   Command = "conflicts.list"
	CommandConflictResolve Command = "conflicts.resolve"
	CommandSettingsGet     Command = "settings.get"
	CommandSettingsSet     Command = "settings.set"
	CommandPinsList        Command = "pins.list"
	CommandPin             Command = "pins.pin"
	CommandUnpin           Command = "pins.unpin"
	CommandSignIn          Command = "account.sign_in"
	CommandMount           Command = "mount"
	CommandUnmount         Command = "unmount"
	CommandSignOut         Command = "account.sign_out"
	CommandQuit            Command = "app.quit"
)

var ErrUnsupportedVersion = errors.New("unsupported IPC protocol version")

type Request struct {
	Version int             `json:"version"`
	ID      string          `json:"id"`
	Command Command         `json:"command"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type Response struct {
	Version   int             `json:"version"`
	ID        string          `json:"id"`
	OK        bool            `json:"ok"`
	ErrorCode string          `json:"error_code,omitempty"`
	Error     string          `json:"error,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Shutdown  bool            `json:"shutdown,omitempty"`
}

type Event struct {
	Version  int             `json:"version"`
	Sequence uint64          `json:"sequence"`
	Type     string          `json:"type"`
	Payload  json.RawMessage `json:"payload,omitempty"`
}

type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Code == "" {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

type Handler interface {
	Handle(context.Context, Request) (payload any, shutdown bool, err error)
}
