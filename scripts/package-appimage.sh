#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

"$root_dir/scripts/fetch-appimage-tools.sh"
tools_dir="$root_dir/.tools"
app_dir="$root_dir/AppDir"
dist_dir="$root_dir/dist"
rm -rf "$app_dir"
mkdir -p "$app_dir/usr/bin" "$app_dir/usr/share/icons/hicolor/scalable/apps"

export GOCACHE="${GOCACHE:-/tmp/google-drive-linux-gocache}"
export GOMODCACHE="${GOMODCACHE:-/tmp/google-drive-linux-gomodcache}"
make build
install -m 0755 bin/driveapp "$app_dir/usr/bin/driveapp"
install -m 0755 bin/drivefs "$app_dir/usr/bin/drivefs"
install -m 0644 assets/google-drive-linux.desktop "$app_dir/google-drive-linux.desktop"
install -m 0644 assets/google-drive-linux.svg \
    "$app_dir/usr/share/icons/hicolor/scalable/apps/google-drive-linux.svg"

cat > "$app_dir/AppRun" <<'APP_RUN'
#!/usr/bin/env sh
HERE="$(dirname "$(readlink -f "$0")")"
export PATH="$HERE/usr/bin:$PATH"
export LD_LIBRARY_PATH="$HERE/usr/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export QT_QPA_PLATFORM_PLUGIN_PATH="$HERE/usr/plugins/platforms"
exec "$HERE/usr/bin/driveapp" "$@"
APP_RUN
chmod 0755 "$app_dir/AppRun"

export EXTRA_PLATFORM_PLUGINS='libqwayland.so;libqoffscreen.so'
export APPIMAGE_EXTRACT_AND_RUN=1
export LDAI_RUNTIME_FILE="$tools_dir/appimage-runtime-x86_64"
export NO_STRIP=1 # bundled linuxdeploy strip is too old for Fedora 44 RELR ELF sections
"$tools_dir/linuxdeploy-x86_64.AppImage" --appimage-extract-and-run \
    --appdir "$app_dir" \
    --executable "$app_dir/usr/bin/driveapp" \
    --executable "$app_dir/usr/bin/drivefs" \
    --desktop-file "$app_dir/google-drive-linux.desktop" \
    --icon-file "$app_dir/usr/share/icons/hicolor/scalable/apps/google-drive-linux.svg" \
    --plugin qt

# linuxdeploy-plugin-qt writes a qt.conf that crashes this MIQT binary when
# used from an extracted AppImage. AppRun sets the bundle paths explicitly.
rm -f "$app_dir/usr/bin/qt.conf"
"$tools_dir/linuxdeploy-plugin-appimage-x86_64.AppImage" --appimage-extract-and-run \
    --appdir "$app_dir"

mkdir -p "$dist_dir"
mv -f "$root_dir"/Google_Drive_for_Linux-x86_64.AppImage "$dist_dir/Google_Drive_for_Linux-x86_64.AppImage"
