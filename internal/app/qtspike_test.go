package app

import (
	"strings"
	"testing"
	"time"

	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/drivecontrol"
	"google-drive-linux/internal/store"
)

func TestAutoMountWaitsForAuthenticatedReadyRuntime(t *testing.T) {
	status := drivecontrol.Status{
		AccountState: "signed-in", NetworkState: "online", IndexState: "ready", Database: "/tmp/metadata.db",
	}
	settings := drivecontrol.Settings{Mountpoint: "/tmp/drive"}
	if !shouldAutoMount(status, settings) {
		t.Fatal("configured, authenticated, ready Drive was not selected for automatic mount")
	}
	for _, change := range []func(*drivecontrol.Status){
		func(s *drivecontrol.Status) { s.Mounted = true },
		func(s *drivecontrol.Status) { s.AccountState = "signed-out" },
		func(s *drivecontrol.Status) { s.NetworkState = "not-connected" },
		func(s *drivecontrol.Status) { s.IndexState = "indexing" },
	} {
		unready := status
		change(&unready)
		if shouldAutoMount(unready, settings) {
			t.Fatalf("automatic mount selected with unready status: %+v", unready)
		}
	}
	if shouldAutoMount(status, drivecontrol.Settings{}) {
		t.Fatal("automatic mount selected without a saved mountpoint")
	}
}

func TestConflictActionsMatchOperationKind(t *testing.T) {
	assertActions := func(name string, record store.ConflictRecord, want ...string) {
		t.Helper()
		options := conflictActionsFor(record)
		if len(options) != len(want) {
			t.Fatalf("%s: got %d actions (%#v), want %v", name, len(options), options, want)
		}
		for i, option := range options {
			if option.Action != want[i] || option.Label == "" || option.Tooltip == "" {
				t.Fatalf("%s: option[%d] = %#v, want action %q with label and tooltip", name, i, option, want[i])
			}
		}
	}

	assertActions("write", store.ConflictRecord{LocalOperationKind: domain.OperationWrite}, "keep-both", "keep-remote")
	assertActions("write fallback by code", store.ConflictRecord{Code: "remote_write_conflict"}, "keep-both", "keep-remote")
	assertActions("rename", store.ConflictRecord{LocalOperationKind: domain.OperationRename}, "keep-local", "keep-remote")
	assertActions("trash", store.ConflictRecord{LocalOperationKind: domain.OperationTrash}, "keep-local", "keep-remote")
	assertActions("generic fallback", store.ConflictRecord{Code: "other_conflict"}, "keep-local", "keep-remote")
}

func TestConflictCardFormattingTabTitleAndSignature(t *testing.T) {
	renameConflict := store.ConflictRecord{
		EventID:            "evt-1",
		OperationID:        "op-1",
		ObjectID:           "local:1",
		Code:               "remote_namespace_conflict",
		LocalOperationKind: domain.OperationRename,
		LocalIntent:        "rename or move to zshrc",
		RemoteName:         ".goutputstream-5US5X3",
		RemoteVersion:      "2",
		CreatedAt:          time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC),
	}
	if got := conflictCardTitle(renameConflict); got != "Rename / move conflict: .goutputstream-5US5X3" {
		t.Fatalf("rename card title = %q", got)
	}
	renameDetails := conflictCardDetails(renameConflict)
	if strings.Contains(renameDetails, "Local copy:") {
		t.Fatalf("rename details should omit empty Local copy line: %q", renameDetails)
	}
	if !strings.Contains(renameDetails, "Local intent: rename or move to zshrc") ||
		!strings.Contains(renameDetails, "Drive: .goutputstream-5US5X3 (version 2)") ||
		!strings.Contains(renameDetails, "Event: evt-1") {
		t.Fatalf("rename details missing expected info: %q", renameDetails)
	}

	writeConflict := store.ConflictRecord{
		EventID:            "evt-2",
		OperationID:        "op-2",
		ObjectID:           "drive:file-2",
		Code:               "remote_write_conflict",
		LocalOperationKind: domain.OperationWrite,
		LocalIntent:        "write",
		LocalCopyName:      "notes (local conflict 1234).txt",
		RemoteName:         "notes.txt",
		RemoteVersion:      "5",
		CreatedAt:          time.Date(2026, 11, 22, 10, 5, 0, 0, time.UTC),
	}
	if got := conflictCardTitle(writeConflict); got != "Content conflict: notes.txt" {
		t.Fatalf("write card title = %q", got)
	}
	writeDetails := conflictCardDetails(writeConflict)
	if !strings.Contains(writeDetails, "Local copy: notes (local conflict 1234).txt") {
		t.Fatalf("write details missing Local copy line: %q", writeDetails)
	}

	if got := conflictTabTitle(0); got != "Conflicts" {
		t.Fatalf("conflictTabTitle(0) = %q, want Conflicts", got)
	}
	if got := conflictTabTitle(3); got != "Conflicts (3)" {
		t.Fatalf("conflictTabTitle(3) = %q, want Conflicts (3)", got)
	}

	list := []store.ConflictRecord{renameConflict, writeConflict}
	sigBase := conflictViewSignature(list, nil, nil)
	if sigBase != conflictViewSignature(list, nil, map[domain.EventID]string{}) {
		t.Fatal("conflictViewSignature was not deterministic for identical state")
	}
	if sigBase == conflictViewSignature(list, nil, map[domain.EventID]string{"evt-1": "keep-local"}) {
		t.Fatal("conflictViewSignature ignored pending resolution state")
	}
	if sigBase == conflictViewSignature(list, []string{"conflicts: timeout"}, nil) {
		t.Fatal("conflictViewSignature ignored errorsFound")
	}
	modified := list
	modified[0].RemoteVersion = "3"
	if sigBase == conflictViewSignature(modified, nil, nil) {
		t.Fatal("conflictViewSignature ignored conflict record changes")
	}
}
