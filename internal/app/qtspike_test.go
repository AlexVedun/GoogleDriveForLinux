package app

import (
	"testing"

	"google-drive-linux/internal/drivecontrol"
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
