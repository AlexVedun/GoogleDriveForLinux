package app

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	qt "github.com/mappu/miqt/qt6"
	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/drivecontrol"
	"google-drive-linux/internal/ipc"
	"google-drive-linux/internal/store"
)

type appSnapshot struct {
	Status    drivecontrol.Status
	Queue     []drivecontrol.QueueItem
	Conflicts []store.ConflictRecord
	Pins      []store.OfflinePin
	Events    []domain.Event
	Settings  drivecontrol.Settings
	Errors    []string
}

// RunQtApp keeps the GUI as a reconnectable client of the per-user drivefs
// service. Long-running actions and IPC polling stay off the Qt GUI thread.
func RunQtApp(smoke bool, socketPath string) int {
	var client *ipc.Client
	var startupError string
	database := ""
	if !smoke {
		var err error
		if socketPath == "" {
			socketPath, err = ipc.DefaultSocketPath()
			if err != nil {
				startupError = err.Error()
			}
		}
		client, err = ConnectOrStartDriveFS(socketPath)
		if err != nil {
			startupError = err.Error()
		}
		database, err = drivecontrol.DefaultDatabasePath()
		if err != nil && startupError == "" {
			startupError = err.Error()
		}
	}

	// Qt requires QApplication and every GUI object to stay on one OS thread.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	application := qt.NewQApplication([]string{"driveapp"})
	_ = application
	qt.QGuiApplication_SetDesktopFileName("google-drive-linux")
	appIcon, appIconFromSVG := loadAppIcon()
	qt.QGuiApplication_SetWindowIcon(appIcon)

	initialStatus := "drivefs service is running"
	if startupError != "" {
		initialStatus = "drivefs unavailable: " + startupError
	}
	var snapshot atomic.Value
	snapshot.Store(appSnapshot{})
	var actionMessage atomic.Value
	actionMessage.Store("")
	var conflictMessage atomic.Value
	conflictMessage.Store("")
	refreshNow := make(chan struct{}, 1)
	triggerRefresh := func() {
		select {
		case refreshNow <- struct{}{}:
		default:
		}
	}
	var clientMu sync.RWMutex
	currentClient := client
	setClient := func(next *ipc.Client) {
		clientMu.Lock()
		currentClient = next
		clientMu.Unlock()
	}
	getClient := func() *ipc.Client {
		clientMu.RLock()
		defer clientMu.RUnlock()
		return currentClient
	}
	callAsync := func(command ipc.Command, payload any, label string) {
		activeClient := getClient()
		if activeClient == nil {
			actionMessage.Store("drivefs is disconnected; restart driveapp to reconnect")
			return
		}
		actionMessage.Store(label + "…")
		go func() {
			_, err := activeClient.Call(context.Background(), command, payload, nil)
			if err != nil {
				actionMessage.Store(label + " failed: " + err.Error())
				return
			}
			actionMessage.Store(label + " requested")
			triggerRefresh()
		}()
	}
	var resolvingMu sync.Mutex
	resolvingConflicts := make(map[domain.EventID]string)
	resolveConflictAsync := func(eventID domain.EventID, action, label string) {
		activeClient := getClient()
		if activeClient == nil {
			msg := "drivefs is disconnected; restart driveapp to reconnect"
			conflictMessage.Store(msg)
			actionMessage.Store(msg)
			return
		}
		resolvingMu.Lock()
		if resolvingConflicts[eventID] != "" {
			resolvingMu.Unlock()
			return
		}
		resolvingConflicts[eventID] = action
		resolvingMu.Unlock()
		msg := fmt.Sprintf("Applying %q…", label)
		conflictMessage.Store(msg)
		actionMessage.Store(msg)
		go func() {
			_, err := activeClient.Call(context.Background(), ipc.CommandConflictResolve,
				map[string]string{"event_id": string(eventID), "action": action}, nil)
			if err != nil {
				resolvingMu.Lock()
				delete(resolvingConflicts, eventID)
				resolvingMu.Unlock()
				errMsg := "Resolve conflict failed: " + err.Error()
				conflictMessage.Store(errMsg)
				actionMessage.Store(errMsg)
				return
			}
			doneMsg := fmt.Sprintf("Resolved conflict (%s)", label)
			conflictMessage.Store(doneMsg)
			actionMessage.Store(doneMsg)
			triggerRefresh()
		}()
	}

	window := qt.NewQWidget2()
	window.SetWindowTitle("Google Drive for Linux")
	window.SetWindowIcon(appIcon)
	window.Resize(760, 560)
	tabs := qt.NewQTabWidget2()

	statusPage := qt.NewQWidget2()
	statusLayout := qt.NewQVBoxLayout(statusPage)
	statusLabel := qt.NewQLabel3(initialStatus)
	statusLabel.SetWordWrap(true)
	statusLayout.AddWidget(statusLabel.QWidget)
	accountLabel := qt.NewQLabel3("Google account: checking…")
	statusLayout.AddWidget(accountLabel.QWidget)
	networkLabel := qt.NewQLabel3("Drive sync: not connected")
	statusLayout.AddWidget(networkLabel.QWidget)
	indexLabel := qt.NewQLabel3("Drive index: not initialized")
	statusLayout.AddWidget(indexLabel.QWidget)
	storageLabel := qt.NewQLabel3("Storage use: checking…")
	statusLayout.AddWidget(storageLabel.QWidget)
	queueStatus := qt.NewQLabel3("This build exposes an indexed FUSE namespace. File content and synchronization availability are shown separately.")
	queueStatus.SetWordWrap(true)
	statusLayout.AddWidget(queueStatus.QWidget)
	accountButtons := qt.NewQWidget2()
	accountLayout := qt.NewQHBoxLayout(accountButtons)
	signIn := qt.NewQPushButton3("Sign in with Google")
	signIn.OnClicked(func() { callAsync(ipc.CommandSignIn, nil, "Sign in") })
	accountLayout.AddWidget(signIn.QWidget)
	signOut := qt.NewQPushButton3("Sign out")
	signOut.OnClicked(func() { callAsync(ipc.CommandSignOut, nil, "Sign out") })
	accountLayout.AddWidget(signOut.QWidget)
	statusLayout.AddWidget(accountButtons)
	mountPointInput := qt.NewQLineEdit2()
	mountPointInput.SetPlaceholderText("Choose an empty folder for the Drive mount")
	statusLayout.AddWidget(mountPointInput.QWidget)
	mountButtons := qt.NewQWidget2()
	mountLayout := qt.NewQHBoxLayout(mountButtons)
	browseMount := qt.NewQPushButton3("Browse…")
	browseMount.OnClicked(func() {
		selected := qt.QFileDialog_GetExistingDirectory2(window, "Choose an empty mount point")
		if selected != "" {
			mountPointInput.SetText(selected)
		}
	})
	mountLayout.AddWidget(browseMount.QWidget)
	mount := qt.NewQPushButton3("Mount Drive")
	mount.OnClicked(func() {
		point := strings.TrimSpace(mountPointInput.Text())
		if point == "" {
			actionMessage.Store("Choose a mount point first")
			return
		}
		callAsync(ipc.CommandMount, drivecontrol.MountRequest{Database: database, Mountpoint: point}, "Mount")
	})
	mountLayout.AddWidget(mount.QWidget)
	unmount := qt.NewQPushButton3("Unmount")
	unmount.OnClicked(func() { callAsync(ipc.CommandUnmount, nil, "Unmount") })
	mountLayout.AddWidget(unmount.QWidget)
	statusLayout.AddWidget(mountButtons)
	quitButton := qt.NewQPushButton3("Quit and unmount")
	statusLayout.AddWidget(quitButton.QWidget)
	messageLabel := qt.NewQLabel3("")
	messageLabel.SetWordWrap(true)
	statusLayout.AddWidget(messageLabel.QWidget)
	statusLayout.AddStretch()
	tabs.AddTab(statusPage, "Status")

	queuePage := qt.NewQWidget2()
	queueLayout := qt.NewQVBoxLayout(queuePage)
	queueView := readOnlyTextView()
	queueLayout.AddWidget(queueView.QWidget)
	queueHint := qt.NewQLabel3("Queued writes are stored durably on this computer. They move to committed only after Drive confirms the upload.")
	queueHint.SetWordWrap(true)
	queueLayout.AddWidget(queueHint.QWidget)
	tabs.AddTab(queuePage, "Queue")

	conflictPage := qt.NewQWidget2()
	conflictLayout := qt.NewQVBoxLayout(conflictPage)
	conflictScroll := qt.NewQScrollArea2()
	conflictScroll.SetWidgetResizable(true)
	conflictScroll.SetFrameShape(qt.QFrame__NoFrame)
	conflictScroll.SetWidget(buildConflictListWidget(nil, nil, nil, resolveConflictAsync))
	conflictLayout.AddWidget(conflictScroll.QWidget)
	conflictMessageLabel := qt.NewQLabel3("")
	conflictMessageLabel.SetWordWrap(true)
	conflictLayout.AddWidget(conflictMessageLabel.QWidget)
	conflictHint := qt.NewQLabel3("Choose how to resolve each conflict directly on its card. Content conflicts keep both copies or the Drive version; rename/delete conflicts apply the local change or keep the Drive state.")
	conflictHint.SetWordWrap(true)
	conflictLayout.AddWidget(conflictHint.QWidget)
	conflictTabIndex := tabs.AddTab(conflictPage, "Conflicts")

	pinsPage := qt.NewQWidget2()
	pinsLayout := qt.NewQVBoxLayout(pinsPage)
	pinsView := readOnlyTextView()
	pinsLayout.AddWidget(pinsView.QWidget)
	pinID := qt.NewQLineEdit2()
	pinID.SetPlaceholderText("Indexed Drive object ID")
	pinsLayout.AddWidget(pinID.QWidget)
	recursivePin := qt.NewQCheckBox3("Include all files in this folder")
	pinsLayout.AddWidget(recursivePin.QWidget)
	pinButtons := qt.NewQWidget2()
	pinButtonLayout := qt.NewQHBoxLayout(pinButtons)
	pin := qt.NewQPushButton3("Pin for offline")
	pin.OnClicked(func() {
		id := strings.TrimSpace(pinID.Text())
		if id == "" {
			actionMessage.Store("Enter an indexed Drive object ID")
			return
		}
		callAsync(ipc.CommandPin, map[string]any{"object_id": id, "recursive": recursivePin.IsChecked()}, "Save offline pin")
	})
	pinButtonLayout.AddWidget(pin.QWidget)
	unpin := qt.NewQPushButton3("Remove pin")
	unpin.OnClicked(func() {
		id := strings.TrimSpace(pinID.Text())
		if id == "" {
			actionMessage.Store("Enter an indexed Drive object ID")
			return
		}
		callAsync(ipc.CommandUnpin, map[string]string{"object_id": id}, "Remove offline pin")
	})
	pinButtonLayout.AddWidget(unpin.QWidget)
	pinsLayout.AddWidget(pinButtons)
	pinHint := qt.NewQLabel3("Pinned files are downloaded and verified in the background. Check the ready count and byte progress before relying on offline access.")
	pinHint.SetWordWrap(true)
	pinsLayout.AddWidget(pinHint.QWidget)
	tabs.AddTab(pinsPage, "Offline")

	settingsPage := qt.NewQWidget2()
	settingsLayout := qt.NewQVBoxLayout(settingsPage)
	settingsLayout.AddWidget(qt.NewQLabel3("Google OAuth desktop client ID").QWidget)
	clientIDInput := qt.NewQLineEdit2()
	clientIDInput.SetPlaceholderText("123456789-abc.apps.googleusercontent.com")
	settingsLayout.AddWidget(clientIDInput.QWidget)
	settingsLayout.AddWidget(qt.NewQLabel3("OAuth client secret (optional)").QWidget)
	clientSecretInput := qt.NewQLineEdit2()
	clientSecretInput.SetEchoMode(qt.QLineEdit__Password)
	settingsLayout.AddWidget(clientSecretInput.QWidget)
	clientIDHint := qt.NewQLabel3("Create a Desktop app OAuth client in Google Cloud. Save its client ID here, then use Sign in with Google. The ID is saved in private application settings; access tokens remain in the system keyring.")
	clientIDHint.SetWordWrap(true)
	settingsLayout.AddWidget(clientIDHint.QWidget)
	settingsLayout.AddWidget(qt.NewQLabel3("Evictable cache limit (MiB)").QWidget)
	cacheLimit := qt.NewQSpinBox2()
	cacheLimit.SetRange(int(drivecontrol.MinCacheLimitBytes>>20), int(drivecontrol.MaxCacheLimitBytes>>20))
	cacheLimit.SetSuffix(" MiB")
	cacheLimit.SetValue(int(drivecontrol.DefaultCacheLimitBytes >> 20))
	settingsLayout.AddWidget(cacheLimit.QWidget)
	saveSettings := qt.NewQPushButton3("Save settings")
	saveSettings.OnClicked(func() {
		callAsync(ipc.CommandSettingsSet, map[string]any{"cache_limit_bytes": int64(cacheLimit.Value()) << 20, "oauth_client_id": strings.TrimSpace(clientIDInput.Text()), "oauth_client_secret": strings.TrimSpace(clientSecretInput.Text())}, "Save settings")
	})
	settingsLayout.AddWidget(saveSettings.QWidget)
	settingsHint := qt.NewQLabel3("The limit applies to evictable downloads when signed in. Pending edits and pinned files are protected and may exceed it; the 10% free-space floor is enforced separately.")
	settingsHint.SetWordWrap(true)
	settingsLayout.AddWidget(settingsHint.QWidget)
	settingsLayout.AddStretch()
	tabs.AddTab(settingsPage, "Settings")

	eventsPage := qt.NewQWidget2()
	eventsLayout := qt.NewQVBoxLayout(eventsPage)
	eventsView := readOnlyTextView()
	eventsLayout.AddWidget(eventsView.QWidget)
	tabs.AddTab(eventsPage, "Events")
	windowLayout := qt.NewQVBoxLayout(window)
	windowLayout.AddWidget(tabs.QWidget)

	tray := qt.NewQSystemTrayIcon2(appIcon)
	tray.SetToolTip("Google Drive for Linux")
	menu := qt.NewQMenu2()
	showAction := qt.NewQAction2("Open window")
	showAction.OnTriggered(func() {
		window.Show()
		window.Raise()
		window.ActivateWindow()
	})
	menu.AddAction(showAction)
	menu.AddSeparator()
	quitComplete := make(chan error, 1)
	requestQuit := func() {
		activeClient := getClient()
		if activeClient == nil {
			qt.QCoreApplication_Quit()
			return
		}
		go func() {
			_, err := activeClient.Call(context.Background(), ipc.CommandQuit, nil, nil)
			quitComplete <- err
		}()
	}
	quitButton.OnClicked(requestQuit)
	quitAction := qt.NewQAction2("Quit and unmount")
	quitAction.OnTriggered(requestQuit)
	menu.AddAction(quitAction)
	tray.SetContextMenu(menu)
	tray.OnActivated(func(reason qt.QSystemTrayIcon__ActivationReason) {
		window.Show()
		window.Raise()
		window.ActivateWindow()
	})
	tray.Show()
	trayAvailable := qt.QSystemTrayIcon_IsSystemTrayAvailable()
	qt.QGuiApplication_SetQuitOnLastWindowClosed(!trayAvailable)
	window.OnCloseEvent(func(super func(*qt.QCloseEvent), event *qt.QCloseEvent) {
		if trayAvailable && !smoke {
			event.SetAccepted(false)
			window.Hide()
			return
		}
		if !trayAvailable && !smoke {
			event.SetAccepted(false)
			requestQuit()
			return
		}
		super(event)
	})

	var pollingStatus atomic.Value
	pollingStatus.Store(initialStatus)
	if smoke {
		go func() {
			time.Sleep(50 * time.Millisecond)
			pollingStatus.Store("Qt UI updated from a background task")
		}()
	} else {
		go pollDriveFS(socketPath, getClient, setClient, &snapshot, &pollingStatus, refreshNow)
	}
	updated := false
	settingsLoaded := false
	autoMountAttempted := false
	lastConflictSignature := conflictViewSignature(nil, nil, nil)
	lastConflictTabTitle := "Conflicts"
	poll := qt.NewQTimer()
	poll.SetInterval(150)
	poll.OnTimeout(func() {
		statusText := pollingStatus.Load().(string)
		if statusLabel.Text() != statusText {
			statusLabel.SetText(statusText)
			updated = true
		}
		message := actionMessage.Load().(string)
		if messageLabel.Text() != message {
			messageLabel.SetText(message)
		}
		conflictMsg := conflictMessage.Load().(string)
		if conflictMessageLabel.Text() != conflictMsg {
			conflictMessageLabel.SetText(conflictMsg)
		}
		state := snapshot.Load().(appSnapshot)
		if !autoMountAttempted && state.Status.Mounted {
			autoMountAttempted = true
		}
		if !autoMountAttempted && shouldAutoMount(state.Status, state.Settings) {
			autoMountAttempted = true
			callAsync(ipc.CommandMount, drivecontrol.MountRequest{
				Database: state.Status.Database, Mountpoint: state.Settings.Mountpoint,
			}, "Auto mount")
		}
		if state.Status.AccountState != "" {
			accountText := "Google account: " + state.Status.AccountState
			if state.Status.AccountError != "" {
				accountText += " — " + state.Status.AccountError
			}
			accountLabel.SetText(accountText)
		}
		if state.Status.NetworkState != "" {
			networkLabel.SetText("Drive sync: " + state.Status.NetworkState)
		}
		indexLabel.SetText(fmt.Sprintf("Drive index: %s (%s)", fallback(state.Status.IndexState, "not initialized"), state.Status.IndexPhase))
		storageLabel.SetText(fmt.Sprintf("Cache: %s of %s; protected data: %s (over limit by %s); free: %s of %s",
			formatBytes(state.Status.CachedBytes), formatBytes(state.Status.CacheLimitBytes), formatBytes(state.Status.ProtectedBytes),
			formatBytes(state.Status.ProtectedOverageBytes), formatByteCount(state.Status.DiskFreeBytes), formatByteCount(state.Status.DiskTotalBytes)))
		if state.Status.Mountpoint != "" && mountPointInput.Text() == "" {
			mountPointInput.SetText(state.Status.Mountpoint)
		}
		if !settingsLoaded && state.Settings.CacheLimitBytes > 0 {
			cacheLimit.SetValue(int(state.Settings.CacheLimitBytes >> 20))
			clientIDInput.SetText(state.Settings.OAuthClientID)
			clientSecretInput.SetText(state.Settings.OAuthClientSecret)
			settingsLoaded = true
		}
		queueView.SetPlainText(renderQueue(state.Queue, state.Errors, state.Status.SyncError))
		activeConflicts := make(map[domain.EventID]struct{}, len(state.Conflicts))
		for _, conflict := range state.Conflicts {
			activeConflicts[conflict.EventID] = struct{}{}
		}
		resolvingMu.Lock()
		for eventID := range resolvingConflicts {
			if _, ok := activeConflicts[eventID]; !ok {
				delete(resolvingConflicts, eventID)
			}
		}
		pendingCopy := make(map[domain.EventID]string, len(resolvingConflicts))
		for eventID, action := range resolvingConflicts {
			pendingCopy[eventID] = action
		}
		resolvingMu.Unlock()
		if sig := conflictViewSignature(state.Conflicts, state.Errors, pendingCopy); sig != lastConflictSignature {
			var scrollPos int
			if bar := conflictScroll.VerticalScrollBar(); bar != nil {
				scrollPos = bar.Value()
			}
			previous := conflictScroll.TakeWidget()
			conflictScroll.SetWidget(buildConflictListWidget(state.Conflicts, state.Errors, pendingCopy, resolveConflictAsync))
			if previous != nil {
				previous.Hide()
				previous.SetParent(nil)
				previous.DeleteLater()
			}
			if bar := conflictScroll.VerticalScrollBar(); bar != nil {
				bar.SetValue(scrollPos)
			}
			lastConflictSignature = sig
		}
		if wantTitle := conflictTabTitle(len(state.Conflicts)); wantTitle != lastConflictTabTitle {
			tabs.SetTabText(conflictTabIndex, wantTitle)
			lastConflictTabTitle = wantTitle
		}
		pinsView.SetPlainText(renderPins(state.Pins, state.Errors, state.Status))
		eventsView.SetPlainText(renderEvents(state.Events, state.Errors))
		tray.SetToolTip("Google Drive for Linux — " + fallback(state.Status.AccountState, "service unavailable"))
		select {
		case quitErr := <-quitComplete:
			if quitErr != nil {
				messageLabel.SetText("Quit failed: " + quitErr.Error())
			} else {
				qt.QCoreApplication_Quit()
			}
		default:
		}
		if smoke && updated {
			qt.QCoreApplication_Quit()
		}
	})
	poll.Start(150)
	window.Show()
	if smoke {
		watchdog := qt.NewQTimer()
		watchdog.SetSingleShot(true)
		watchdog.OnTimeout(qt.QCoreApplication_Quit)
		watchdog.Start(2000)
	}
	if !trayAvailable {
		fmt.Println("Qt system tray is unavailable; the main window remains available")
	}
	qt.QApplication_Exec()
	if smoke && (appIcon == nil || appIcon.IsNull()) {
		fmt.Println("application icon could not be loaded")
		return 1
	}
	if smoke && !appIconFromSVG {
		fmt.Println("warning: embedded SVG icon could not be rasterized; using fallback icon")
	}
	if smoke && !updated {
		fmt.Println("background state was not delivered through the Qt event loop")
		return 1
	}
	return 0
}

func shouldAutoMount(status drivecontrol.Status, settings drivecontrol.Settings) bool {
	return !status.Mounted && status.AccountState == "signed-in" &&
		status.NetworkState != "not-connected" && status.IndexState == "ready" &&
		status.Database != "" && settings.Mountpoint != ""
}

func pollDriveFS(socketPath string, getClient func() *ipc.Client, setClient func(*ipc.Client), destination *atomic.Value, statusText *atomic.Value, refreshNow <-chan struct{}) {
	for {
		client := getClient()
		if client == nil {
			var err error
			client, err = ipc.NewClient(socketPath, 8*time.Second)
			if err != nil {
				statusText.Store("drivefs disconnected; restart driveapp to reconnect")
				waitPollInterval(refreshNow)
				continue
			}
		}
		state, errorsFound, err := fetchSnapshot(client)
		if err != nil {
			statusText.Store("drivefs disconnected: " + err.Error())
			setClient(nil)
			waitPollInterval(refreshNow)
			continue
		}
		setClient(client)
		destination.Store(state)
		statusText.Store(renderStatus(state.Status, errorsFound))
		waitPollInterval(refreshNow)
	}
}

func waitPollInterval(refreshNow <-chan struct{}) {
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-refreshNow:
	case <-timer.C:
	}
}

func fetchSnapshot(client *ipc.Client) (appSnapshot, []string, error) {
	var state appSnapshot
	_, err := client.Call(context.Background(), ipc.CommandStatus, nil, &state.Status)
	if err != nil {
		return state, nil, err
	}
	var errorsFound []string
	requests := []struct {
		command ipc.Command
		out     any
		name    string
	}{
		{ipc.CommandQueueList, &state.Queue, "queue"},
		{ipc.CommandConflictsList, &state.Conflicts, "conflicts"},
		{ipc.CommandPinsList, &state.Pins, "offline pins"},
		{ipc.CommandEventsList, &state.Events, "events"},
		{ipc.CommandSettingsGet, &state.Settings, "settings"},
	}
	for _, request := range requests {
		if _, err := client.Call(context.Background(), request.command, nil, request.out); err != nil {
			errorsFound = append(errorsFound, request.name+": "+err.Error())
		}
	}
	state.Errors = errorsFound
	return state, errorsFound, nil
}

func readOnlyTextView() *qt.QTextEdit {
	view := qt.NewQTextEdit2()
	view.SetReadOnly(true)
	return view
}

func renderStatus(status drivecontrol.Status, errorsFound []string) string {
	state := status.NetworkState
	if status.NetworkError != "" {
		state += " — " + status.NetworkError
	}
	if status.Mounted {
		return fmt.Sprintf("Mounted at %s · Drive %s · index %s · %d queued operations", status.Mountpoint, fallback(state, "unknown"), fallback(status.IndexState, "unknown"), status.QueuePending)
	}
	if status.AccountState == "" {
		return "drivefs service is running"
	}
	text := fmt.Sprintf("Not mounted · account %s · Drive %s · index %s", status.AccountState, fallback(state, "unknown"), fallback(status.IndexState, "not initialized"))
	if len(errorsFound) > 0 {
		text += " · " + strings.Join(errorsFound, "; ")
	}
	return text
}

func renderQueue(items []drivecontrol.QueueItem, errorsFound []string, syncError string) string {
	var output strings.Builder
	if syncError != "" {
		fmt.Fprintf(&output, "Sync worker: %s\n\n", syncError)
	}
	if len(items) == 0 {
		output.WriteString("No pending operations.\n")
	}
	for _, item := range items {
		fmt.Fprintf(&output, "%s  %s  %s  attempt %d  %s", item.ID, item.Kind, item.State, item.AttemptCount, item.ObjectID)
		if item.ResumableBytes > 0 {
			fmt.Fprintf(&output, "  acknowledged %s", formatBytes(item.ResumableBytes))
		}
		if item.ErrorCode != "" || item.ErrorMessage != "" {
			fmt.Fprintf(&output, "\n  Error: %s %s", item.ErrorCode, item.ErrorMessage)
		}
		output.WriteByte('\n')
	}
	appendErrors(&output, errorsFound)
	return output.String()
}

func renderPins(pins []store.OfflinePin, errorsFound []string, status drivecontrol.Status) string {
	var output strings.Builder
	fmt.Fprintf(&output, "Prefetch: %s · %d of %d files · %s of %s\n",
		fallback(status.PinState, "idle"), status.PinFilesDone, status.PinFilesTotal,
		formatBytes(status.PinBytesDone), formatBytes(status.PinBytesTotal))
	if status.PinCurrentName != "" {
		fmt.Fprintf(&output, "Current file: %s (%s)\n", status.PinCurrentName, fallback(status.PinCurrentState, "downloading"))
	}
	if status.PinError != "" {
		fmt.Fprintf(&output, "Last pin error: %s\n", status.PinError)
	}
	output.WriteString("Saved offline pins:\n\n")
	if len(pins) == 0 {
		output.WriteString("No offline pins.\n")
	}
	for _, pin := range pins {
		fmt.Fprintf(&output, "%s  recursive=%t  saved=%s\n", pin.ObjectID, pin.Recursive, pin.PinnedAt.Format(time.RFC3339))
	}
	appendErrors(&output, errorsFound)
	return output.String()
}

func renderEvents(events []domain.Event, errorsFound []string) string {
	var output strings.Builder
	if len(events) == 0 {
		output.WriteString("No recorded events.\n")
	}
	for _, event := range events {
		fmt.Fprintf(&output, "%s  %s  %s  %s\n%s\n\n", event.CreatedAt.Format(time.RFC3339), event.Kind, event.Code, event.ID, event.DetailsJSON)
	}
	appendErrors(&output, errorsFound)
	return output.String()
}

func appendErrors(output *strings.Builder, errorsFound []string) {
	if len(errorsFound) == 0 {
		return
	}
	output.WriteString("\nUnavailable sections:\n")
	for _, item := range errorsFound {
		fmt.Fprintf(output, "• %s\n", item)
	}
}

func formatBytes(value int64) string {
	return formatByteCount(uint64(max(value, 0)))
}

func formatByteCount(value uint64) string {
	const unit = uint64(1 << 20)
	return fmt.Sprintf("%.1f MiB", float64(value)/float64(unit))
}

func fallback(value, defaultValue string) string {
	if value == "" {
		return defaultValue
	}
	return value
}
