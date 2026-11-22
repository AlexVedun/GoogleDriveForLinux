package app

import (
	"fmt"
	"strconv"
	"strings"

	qt "github.com/mappu/miqt/qt6"
	"google-drive-linux/internal/domain"
	"google-drive-linux/internal/store"
)

type conflictActionOption struct {
	Action  string
	Label   string
	Tooltip string
}

func conflictActionsFor(conflict store.ConflictRecord) []conflictActionOption {
	switch {
	case conflict.LocalOperationKind == domain.OperationWrite ||
		(conflict.LocalOperationKind == "" && conflict.Code == "remote_write_conflict"):
		return []conflictActionOption{
			{
				Action:  "keep-both",
				Label:   "Keep both",
				Tooltip: "Keep both the Drive file and the local conflict copy",
			},
			{
				Action:  "keep-remote",
				Label:   "Keep Drive version",
				Tooltip: "Keep the Drive file and move the local conflict copy to Drive trash",
			},
		}
	case conflict.LocalOperationKind == domain.OperationRename:
		return []conflictActionOption{
			{
				Action:  "keep-local",
				Label:   "Apply local rename",
				Tooltip: "Retry the local rename or move against the latest Drive version",
			},
			{
				Action:  "keep-remote",
				Label:   "Keep Drive name",
				Tooltip: "Discard the pending local rename and keep the current Drive path",
			},
		}
	case conflict.LocalOperationKind == domain.OperationTrash:
		return []conflictActionOption{
			{
				Action:  "keep-local",
				Label:   "Apply local delete",
				Tooltip: "Retry moving the file to Drive trash against the latest Drive version",
			},
			{
				Action:  "keep-remote",
				Label:   "Keep Drive file",
				Tooltip: "Discard the pending local delete and keep the Drive file",
			},
		}
	default:
		return []conflictActionOption{
			{
				Action:  "keep-local",
				Label:   "Keep local change",
				Tooltip: "Retry the local operation against the latest Drive version",
			},
			{
				Action:  "keep-remote",
				Label:   "Keep Drive version",
				Tooltip: "Discard the pending local change and keep the current Drive state",
			},
		}
	}
}

func conflictCardTitle(conflict store.ConflictRecord) string {
	var prefix string
	switch {
	case conflict.LocalOperationKind == domain.OperationWrite ||
		(conflict.LocalOperationKind == "" && conflict.Code == "remote_write_conflict"):
		prefix = "Content conflict"
	case conflict.LocalOperationKind == domain.OperationRename:
		prefix = "Rename / move conflict"
	case conflict.LocalOperationKind == domain.OperationTrash:
		prefix = "Delete conflict"
	default:
		prefix = "Sync conflict"
	}
	if conflict.RemoteName != "" {
		return prefix + ": " + conflict.RemoteName
	}
	return prefix
}

func conflictCardDetails(conflict store.ConflictRecord) string {
	lines := make([]string, 0, 4)
	if conflict.LocalIntent != "" {
		lines = append(lines, "Local intent: "+conflict.LocalIntent)
	}
	lines = append(lines, fmt.Sprintf("Drive: %s (version %s)",
		fallback(conflict.RemoteName, "unknown"), fallback(conflict.RemoteVersion, "unknown")))
	if conflict.LocalCopyName != "" {
		lines = append(lines, "Local copy: "+conflict.LocalCopyName)
	}
	lines = append(lines, fmt.Sprintf("Code: %s · Operation: %s · Event: %s",
		fallback(conflict.Code, "conflict"), conflict.OperationID, conflict.EventID))
	return strings.Join(lines, "\n")
}

func conflictTabTitle(count int) string {
	if count <= 0 {
		return "Conflicts"
	}
	return fmt.Sprintf("Conflicts (%d)", count)
}

func conflictViewSignature(conflicts []store.ConflictRecord, errorsFound []string, pending map[domain.EventID]string) string {
	var b strings.Builder
	b.WriteString(strconv.Itoa(len(conflicts)))
	for _, conflict := range conflicts {
		b.WriteByte('\x1e')
		b.WriteString(string(conflict.EventID))
		b.WriteByte('\x1f')
		b.WriteString(string(conflict.OperationID))
		b.WriteByte('\x1f')
		b.WriteString(string(conflict.ObjectID))
		b.WriteByte('\x1f')
		b.WriteString(conflict.Code)
		b.WriteByte('\x1f')
		b.WriteString(string(conflict.LocalOperationKind))
		b.WriteByte('\x1f')
		b.WriteString(conflict.LocalIntent)
		b.WriteByte('\x1f')
		b.WriteString(string(conflict.LocalCopyObjectID))
		b.WriteByte('\x1f')
		b.WriteString(conflict.LocalCopyName)
		b.WriteByte('\x1f')
		b.WriteString(conflict.RemoteName)
		b.WriteByte('\x1f')
		b.WriteString(conflict.RemoteVersion)
		b.WriteByte('\x1f')
		b.WriteString(strconv.FormatInt(conflict.CreatedAt.UnixNano(), 10))
		b.WriteByte('\x1f')
		b.WriteString(pending[conflict.EventID])
	}
	b.WriteByte('\x1d')
	b.WriteString(strconv.Itoa(len(errorsFound)))
	for _, errText := range errorsFound {
		b.WriteByte('\x1f')
		b.WriteString(errText)
	}
	return b.String()
}

func buildConflictListWidget(conflicts []store.ConflictRecord, errorsFound []string, pending map[domain.EventID]string,
	onResolve func(eventID domain.EventID, action, label string)) *qt.QWidget {
	container := qt.NewQWidget2()
	layout := qt.NewQVBoxLayout(container)
	layout.SetSpacing(8)
	layout.SetContentsMargins(4, 4, 4, 4)

	if len(conflicts) == 0 {
		empty := qt.NewQLabel3("No unresolved conflicts.")
		empty.SetWordWrap(true)
		layout.AddWidget(empty.QWidget)
	}

	for _, conflict := range conflicts {
		card := qt.NewQFrame2()
		card.SetFrameShape(qt.QFrame__StyledPanel)
		card.SetFrameShadow(qt.QFrame__Raised)
		cardLayout := qt.NewQVBoxLayout(card.QWidget)
		cardLayout.SetSpacing(6)
		cardLayout.SetContentsMargins(10, 8, 10, 8)

		title := qt.NewQLabel3(conflictCardTitle(conflict))
		title.SetWordWrap(true)
		title.SetStyleSheet("font-weight: bold;")
		title.SetTextInteractionFlags(qt.TextSelectableByMouse)
		cardLayout.AddWidget(title.QWidget)

		details := qt.NewQLabel3(conflictCardDetails(conflict))
		details.SetWordWrap(true)
		details.SetTextInteractionFlags(qt.TextSelectableByMouse)
		cardLayout.AddWidget(details.QWidget)

		buttonRow := qt.NewQWidget2()
		buttonLayout := qt.NewQHBoxLayout(buttonRow)
		buttonLayout.SetContentsMargins(0, 2, 0, 0)
		buttonLayout.SetSpacing(8)
		inFlight := pending[conflict.EventID]
		for _, option := range conflictActionsFor(conflict) {
			eventID, action, label := conflict.EventID, option.Action, option.Label
			text := option.Label
			if inFlight == option.Action {
				text += "…"
			}
			button := qt.NewQPushButton3(text)
			button.SetToolTip(option.Tooltip)
			if inFlight != "" {
				button.SetEnabled(false)
			}
			button.OnClicked(func() {
				if onResolve != nil {
					onResolve(eventID, action, label)
				}
			})
			buttonLayout.AddWidget(button.QWidget)
		}
		buttonLayout.AddStretch()
		cardLayout.AddWidget(buttonRow)
		layout.AddWidget(card.QWidget)
	}

	if len(errorsFound) > 0 {
		var output strings.Builder
		appendErrors(&output, errorsFound)
		errLabel := qt.NewQLabel3(strings.TrimLeft(output.String(), "\n"))
		errLabel.SetWordWrap(true)
		errLabel.SetTextInteractionFlags(qt.TextSelectableByMouse)
		layout.AddWidget(errLabel.QWidget)
	}

	layout.AddStretch()
	return container
}
