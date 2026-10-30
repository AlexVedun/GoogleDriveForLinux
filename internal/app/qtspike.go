package app

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	qt "github.com/mappu/miqt/qt6"
	"google-drive-linux/internal/drivecontrol"
	"google-drive-linux/internal/ipc"
)

// RunQtApp keeps the GUI as a reconnectable client of the per-user drivefs
// service. Mount and quit requests are sent away from the Qt GUI thread.
func RunQtApp(smoke bool, socketPath string) int {
	var client *ipc.Client
	var startupError string
	var database string
	if !smoke {
		var err error
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

	initialStatus := "drivefs service is running"
	if startupError != "" {
		initialStatus = "drivefs unavailable: " + startupError
	}
	var backgroundStatus atomic.Value
	backgroundStatus.Store(initialStatus)
	quitComplete := make(chan error, 1)
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

	window := qt.NewQMainWindow2()
	window.SetWindowTitle("Google Drive for Linux")
	window.Resize(560, 240)
	centralWidget := qt.NewQWidget2()
	layout := qt.NewQVBoxLayout(centralWidget)
	statusLabel := qt.NewQLabel3(initialStatus)
	layout.AddWidget(statusLabel.QWidget)
	queueStatus := qt.NewQLabel3("This build exposes a read-only indexed namespace; writable sync is not connected to drivefs yet.")
	layout.AddWidget(queueStatus.QWidget)
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
	chooseFolder := qt.NewQPushButton3("Choose mount point…")
	chooseFolder.OnClicked(func() {
		selected := qt.QFileDialog_GetExistingDirectory2(window.QWidget, "Choose an empty mount point")
		if selected == "" {
			return
		}
		activeClient := getClient()
		if activeClient == nil {
			backgroundStatus.Store("drivefs service is unavailable")
			return
		}
		backgroundStatus.Store("Mounting indexed Drive namespace at " + selected + "…")
		go func() {
			var current drivecontrol.Status
			_, err := activeClient.Call(context.Background(), ipc.CommandMount,
				drivecontrol.MountRequest{Database: database, Mountpoint: selected}, &current)
			if err != nil {
				backgroundStatus.Store("Mount failed: " + err.Error())
				return
			}
			backgroundStatus.Store(renderStatus(current))
		}()
	})
	layout.AddWidget(chooseFolder.QWidget)
	unmount := qt.NewQPushButton3("Unmount Drive")
	unmount.OnClicked(func() {
		activeClient := getClient()
		if activeClient == nil {
			return
		}
		go func() {
			var current drivecontrol.Status
			_, err := activeClient.Call(context.Background(), ipc.CommandUnmount, nil, &current)
			if err != nil {
				backgroundStatus.Store("Unmount failed: " + err.Error())
				return
			}
			backgroundStatus.Store(renderStatus(current))
		}()
	})
	layout.AddWidget(unmount.QWidget)
	quitButton := qt.NewQPushButton3("Quit and unmount")
	quitButton.OnClicked(requestQuit)
	layout.AddWidget(quitButton.QWidget)
	window.SetCentralWidget(centralWidget)

	icon := qt.QApplication_Style().StandardIcon(qt.QStyle__SP_DriveNetIcon, nil, nil)
	tray := qt.NewQSystemTrayIcon2(icon)
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
	window.QWidget.OnCloseEvent(func(super func(*qt.QCloseEvent), event *qt.QCloseEvent) {
		if trayAvailable && !smoke {
			event.SetAccepted(false)
			window.Hide()
			return
		}
		super(event)
	})

	if smoke {
		go func() {
			time.Sleep(50 * time.Millisecond)
			backgroundStatus.Store("Background status delivered on the Qt GUI thread")
		}()
	} else {
		go func() {
			for {
				activeClient := getClient()
				if activeClient == nil {
					reconnected, err := ipc.NewClient(socketPath, 3*time.Second)
					if err != nil {
						backgroundStatus.Store("drivefs disconnected; restart driveapp to reconnect")
						time.Sleep(2 * time.Second)
						continue
					}
					if _, err := reconnected.Call(context.Background(), ipc.CommandStatus, nil, nil); err != nil {
						backgroundStatus.Store("drivefs disconnected; restart driveapp to reconnect")
						time.Sleep(2 * time.Second)
						continue
					}
					setClient(reconnected)
					activeClient = reconnected
				}
				var current drivecontrol.Status
				if _, err := activeClient.Call(context.Background(), ipc.CommandStatus, nil, &current); err != nil {
					backgroundStatus.Store("drivefs disconnected: " + err.Error())
					setClient(nil)
				} else {
					backgroundStatus.Store(renderStatus(current))
				}
				time.Sleep(2 * time.Second)
			}
		}()
	}

	updated := false
	poll := qt.NewQTimer()
	poll.SetInterval(100)
	poll.OnTimeout(func() {
		text := backgroundStatus.Load().(string)
		if statusLabel.Text() != text {
			statusLabel.SetText(text)
			updated = true
			if smoke {
				qt.QCoreApplication_Quit()
			}
		}
		select {
		case quitErr := <-quitComplete:
			if quitErr != nil {
				statusLabel.SetText("Quit failed: " + quitErr.Error())
			} else {
				qt.QCoreApplication_Quit()
			}
		default:
		}
	})
	poll.Start(100)
	window.Show()
	if smoke {
		watchdog := qt.NewQTimer()
		watchdog.SetSingleShot(true)
		watchdog.OnTimeout(qt.QCoreApplication_Quit)
		watchdog.Start(2000)
	}
	if !trayAvailable {
		fmt.Println("Qt system tray is unavailable; window remains available in the task switcher")
	}
	qt.QApplication_Exec()
	if smoke && !updated {
		fmt.Println("background state was not delivered through the Qt event loop")
		return 1
	}
	return 0
}

func renderStatus(status drivecontrol.Status) string {
	if status.Mounted {
		return fmt.Sprintf("Mounted at %s; index %s (%s); %d operations waiting", status.Mountpoint,
			status.IndexState, status.IndexPhase, status.QueuePending)
	}
	if status.IndexState != "" {
		return fmt.Sprintf("Not mounted; index %s (%s); %d operations waiting", status.IndexState,
			status.IndexPhase, status.QueuePending)
	}
	return "drivefs service is running; the Drive index is not open"
}
