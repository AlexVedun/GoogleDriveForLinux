package app

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	qt "github.com/mappu/miqt/qt6"
)

// RunQtSpike starts a small Qt Widgets window and a system tray entry to prove
// that MIQT, Qt 6, and GUI-thread dispatch work together on the target system.
func RunQtSpike(smoke bool) int {
	// Qt requires the QApplication and all GUI objects to stay on one OS thread.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	application := qt.NewQApplication([]string{"driveapp"})
	_ = application // Keep QApplication alive for the whole event loop.
	qt.QGuiApplication_SetQuitOnLastWindowClosed(false)

	window := qt.NewQMainWindow2()
	window.SetWindowTitle("Google Drive for Linux — Qt spike")
	window.Resize(480, 180)
	centralWidget := qt.NewQWidget2()
	layout := qt.NewQVBoxLayout(centralWidget)
	statusLabel := qt.NewQLabel3("Starting Qt UI prototype…")
	layout.AddWidget(statusLabel.QWidget)
	queueStatus := qt.NewQLabel3("After a local save, the status will read: “Saved locally; waiting to upload to Google Drive.”")
	layout.AddWidget(queueStatus.QWidget)
	chooseFolder := qt.NewQPushButton3("Choose a folder")
	chooseFolder.OnClicked(func() {
		selected := qt.QFileDialog_GetExistingDirectory2(window.QWidget, "Choose a folder")
		if selected != "" {
			statusLabel.SetText("Selected folder: " + selected)
		}
	})
	layout.AddWidget(chooseFolder.QWidget)
	window.SetCentralWidget(centralWidget)

	icon := qt.QApplication_Style().StandardIcon(qt.QStyle__SP_DriveNetIcon, nil, nil)
	tray := qt.NewQSystemTrayIcon2(icon)
	tray.SetToolTip("Google Drive for Linux — Qt prototype")
	menu := qt.NewQMenu2()
	showAction := qt.NewQAction2("Open window")
	showAction.OnTriggered(func() {
		window.Show()
		window.Raise()
		window.ActivateWindow()
	})
	menu.AddAction(showAction)
	menu.AddSeparator()
	quitAction := qt.NewQAction2("Quit prototype")
	quitAction.OnTriggered(qt.QCoreApplication_Quit)
	menu.AddAction(quitAction)
	tray.SetContextMenu(menu)
	tray.OnActivated(func(reason qt.QSystemTrayIcon__ActivationReason) {
		window.Show()
		window.Raise()
		window.ActivateWindow()
	})
	tray.Show()

	var backgroundStatus atomic.Value
	backgroundStatus.Store("Waiting for background status…")
	go func() {
		time.Sleep(50 * time.Millisecond)
		backgroundStatus.Store("Background status delivered on the Qt GUI thread")
	}()

	updated := false
	poll := qt.NewQTimer()
	poll.SetInterval(20)
	poll.OnTimeout(func() {
		text := backgroundStatus.Load().(string)
		if statusLabel.Text() == text {
			return
		}
		statusLabel.SetText(text)
		updated = true
		if smoke {
			qt.QCoreApplication_Quit()
		}
	})
	poll.Start(20)

	window.Show()
	if smoke {
		watchdog := qt.NewQTimer()
		watchdog.SetSingleShot(true)
		watchdog.OnTimeout(qt.QCoreApplication_Quit)
		watchdog.Start(2000)
	}

	if !qt.QSystemTrayIcon_IsSystemTrayAvailable() {
		fmt.Println("Qt system tray is unavailable; window remains available")
	}
	qt.QApplication_Exec()
	if smoke && !updated {
		fmt.Println("background state was not delivered through the Qt event loop")
		return 1
	}
	return 0
}
