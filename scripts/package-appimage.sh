#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

"$root_dir/scripts/fetch-appimage-tools.sh"
tools_dir="$root_dir/.tools"
app_dir="$root_dir/AppDir"
dist_dir="$root_dir/dist"
rm -rf "$app_dir"
mkdir -p \
    "$app_dir/usr/bin" \
    "$app_dir/usr/share/icons/hicolor/scalable/apps" \
    "$app_dir/usr/share/doc/google-drive-linux/licenses"

export GOCACHE="${GOCACHE:-/tmp/google-drive-linux-gocache}"
export GOMODCACHE="${GOMODCACHE:-/tmp/google-drive-linux-gomodcache}"
make build
install -m 0755 bin/driveapp "$app_dir/usr/bin/driveapp"
install -m 0755 bin/drivefs "$app_dir/usr/bin/drivefs"
install -m 0644 assets/google-drive-linux.desktop "$app_dir/google-drive-linux.desktop"
install -m 0644 assets/google-drive-linux.svg \
    "$app_dir/usr/share/icons/hicolor/scalable/apps/google-drive-linux.svg"
install -m 0644 THIRD_PARTY_NOTICES.md \
    "$app_dir/usr/share/doc/google-drive-linux/THIRD_PARTY_NOTICES.md"
install -m 0644 licenses/*.txt \
    "$app_dir/usr/share/doc/google-drive-linux/licenses/"

cat > "$app_dir/AppRun" <<'APP_RUN'
#!/usr/bin/env sh
HERE="$(dirname "$(readlink -f "$0")")"
export PATH="$HERE/usr/bin:$PATH"
export LD_LIBRARY_PATH="$HERE/usr/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export QT_PLUGIN_PATH="$HERE/usr/plugins"
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

# linuxdeploy rewrites ELF runpaths, and its patched Fedora 44 libraries can
# retain a DT_INIT address that no longer points at the relocated .init code.
# AppRun already resolves shared libraries through LD_LIBRARY_PATH, so restore
# their original bytes after linuxdeploy has finished collecting dependencies.
for bundled in "$app_dir"/usr/lib/*; do
    [[ -f "$bundled" ]] || continue
    original="/usr/lib64/${bundled##*/}"
    [[ -f "$original" ]] || { echo "Missing original library: $original" >&2; exit 1; }
    cp -L -- "$original" "$bundled"
done
while IFS= read -r -d '' bundled; do
    relative="${bundled#"$app_dir/usr/plugins/"}"
    original="/usr/lib64/qt6/plugins/$relative"
    [[ -f "$original" ]] || { echo "Missing original Qt plugin: $original" >&2; exit 1; }
    cp -L -- "$original" "$bundled"
done < <(find "$app_dir/usr/plugins" -type f -name '*.so' -print0)
install -m 0755 bin/driveapp "$app_dir/usr/bin/driveapp"
install -m 0755 bin/drivefs "$app_dir/usr/bin/drivefs"

# Keep Qt's plugin lookup inside the AppImage. The relative paths are resolved
# from usr/bin, where driveapp and this qt.conf are installed.
cat > "$app_dir/usr/bin/qt.conf" <<'QT_CONF'
[Paths]
Prefix=..
Plugins=plugins
QT_CONF
"$root_dir/scripts/bundle-appimage-rpm-licenses.sh" "$app_dir"
"$tools_dir/linuxdeploy-plugin-appimage-x86_64.AppImage" --appimage-extract-and-run \
    --appdir "$app_dir"

mkdir -p "$dist_dir"
mv -f "$root_dir"/Google_Drive_for_Linux-x86_64.AppImage "$dist_dir/Google_Drive_for_Linux-x86_64.AppImage"
