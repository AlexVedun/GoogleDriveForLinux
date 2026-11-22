package app

import (
	qt "github.com/mappu/miqt/qt6"
	"google-drive-linux/assets"
)

var appIconSizes = []int{16, 22, 24, 32, 48, 64, 128, 256}

// iconFromSVG rasterizes svg across standard desktop and tray icon extents so
// both window chrome and system tray backends receive crisp HiDPI pixmaps.
func iconFromSVG(svg []byte, sizes []int) *qt.QIcon {
	if len(svg) == 0 {
		return nil
	}

	icon := qt.NewQIcon()
	loaded := false
	for _, size := range sizes {
		buf := qt.NewQBuffer()
		buf.SetData(svg)
		if !buf.Open(qt.QIODeviceBase__ReadOnly) {
			buf.Delete()
			continue
		}

		reader := qt.NewQImageReader4(buf.QIODevice, []byte("svg"))
		reader.SetScaledSize(qt.NewQSize2(size, size))
		img := reader.Read()
		reader.Delete()
		buf.Close()
		buf.Delete()

		if img == nil || img.IsNull() {
			continue
		}
		pm := qt.QPixmap_FromImage(img)
		if pm == nil || pm.IsNull() {
			continue
		}
		icon.AddPixmap(pm)
		loaded = true
	}

	if !loaded {
		icon.Delete()
		return nil
	}
	return icon
}

// loadAppIcon loads the embedded application SVG icon, falling back to the
// desktop icon theme or standard style drive icon if SVG rendering is unavailable.
func loadAppIcon() (*qt.QIcon, bool) {
	if icon := iconFromSVG(assets.AppIconSVG, appIconSizes); icon != nil && !icon.IsNull() {
		return icon, true
	}
	if themeIcon := qt.QIcon_FromTheme("google-drive-linux"); themeIcon != nil && !themeIcon.IsNull() {
		return themeIcon, false
	}
	return qt.QApplication_Style().StandardIcon(qt.QStyle__SP_DriveNetIcon, nil, nil), false
}
