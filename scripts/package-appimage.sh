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

if [[ -z "${QMAKE:-}" ]]; then
    for candidate in qmake6 qmake-qt6 qmake; do
        if QMAKE="$(command -v "$candidate" 2>/dev/null)"; then
            break
        fi
    done
fi
if [[ -z "${QMAKE:-}" || ! -x "$QMAKE" ]]; then
    echo "Unable to locate qmake6/qmake; install Qt 6 development tools or set QMAKE." >&2
    exit 1
fi
export QMAKE
qt_plugins_dir="$("$QMAKE" -query QT_INSTALL_PLUGINS)"
qt_libs_dir="$("$QMAKE" -query QT_INSTALL_LIBS)"

if [[ -z "${EXTRA_PLATFORM_PLUGINS:-}" ]]; then
    extra_plugins=()
    for plugin in libqwayland.so libqwayland-egl.so libqwayland-generic.so libqoffscreen.so; do
        if [[ -f "$qt_plugins_dir/platforms/$plugin" ]]; then
            extra_plugins+=("$plugin")
        fi
    done
    EXTRA_PLATFORM_PLUGINS="$(IFS=';'; echo "${extra_plugins[*]-}")"
fi
export EXTRA_PLATFORM_PLUGINS
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

# linuxdeploy rewrites ELF runpaths, and its patched system libraries can
# retain a DT_INIT address that no longer points at the relocated .init code.
# AppRun already resolves shared libraries through LD_LIBRARY_PATH, so restore
# their original bytes after linuxdeploy has finished collecting dependencies.
declare -A system_libs=()
while read -r line; do
    [[ "$line" == *"x86-64"* ]] || continue
    soname="${line%% *}"
    resolved="${line##* => }"
    if [[ -n "$soname" && "$resolved" == /* && -z "${system_libs[$soname]:-}" ]]; then
        system_libs["$soname"]="$resolved"
    fi
done < <(PATH="/usr/sbin:/sbin:$PATH" ldconfig -p 2>/dev/null || true)

find_system_library() {
    local name="$1"
    local candidate
    for candidate in \
        "$qt_libs_dir/$name" \
        "${system_libs[$name]:-}" \
        "/usr/lib64/$name" \
        "/usr/lib/x86_64-linux-gnu/$name" \
        "/lib/x86_64-linux-gnu/$name" \
        "/usr/lib/$name"; do
        if [[ -n "$candidate" && -e "$candidate" ]]; then
            printf '%s\n' "$candidate"
            return 0
        fi
    done
    candidate="$(find /usr/lib /usr/lib64 /lib -maxdepth 4 \( -type f -o -type l \) -name "$name" -print -quit 2>/dev/null || true)"
    if [[ -n "$candidate" && -e "$candidate" ]]; then
        printf '%s\n' "$candidate"
        return 0
    fi
    return 1
}

for bundled in "$app_dir"/usr/lib/*; do
    [[ -f "$bundled" || -L "$bundled" ]] || continue
    original="$(find_system_library "${bundled##*/}")" || {
        echo "Missing original library: ${bundled##*/}" >&2
        exit 1
    }
    cp -L -- "$original" "$bundled"
done
while IFS= read -r -d '' bundled; do
    relative="${bundled#"$app_dir/usr/plugins/"}"
    original="$qt_plugins_dir/$relative"
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

mkdir -p "$dist_dir"
if [[ -n "${VERSION:-}" ]]; then
    output_name="GoogleDriveForLinux-${VERSION}-x86_64.AppImage"
    export VERSION
else
    output_name="GoogleDriveForLinux-x86_64.AppImage"
fi
export LDAI_OUTPUT="$dist_dir/$output_name"
"$tools_dir/linuxdeploy-plugin-appimage-x86_64.AppImage" --appimage-extract-and-run \
    --appdir "$app_dir"

if [[ ! -f "$dist_dir/$output_name" ]]; then
    if [[ -f "$root_dir/$output_name" ]]; then
        mv -f "$root_dir/$output_name" "$dist_dir/$output_name"
    elif [[ -n "${VERSION:-}" && -f "$root_dir/Google_Drive_for_Linux-${VERSION}-x86_64.AppImage" ]]; then
        mv -f "$root_dir/Google_Drive_for_Linux-${VERSION}-x86_64.AppImage" "$dist_dir/$output_name"
    elif [[ -f "$root_dir/Google_Drive_for_Linux-x86_64.AppImage" ]]; then
        mv -f "$root_dir/Google_Drive_for_Linux-x86_64.AppImage" "$dist_dir/$output_name"
    else
        echo "Expected AppImage was not generated: $dist_dir/$output_name" >&2
        exit 1
    fi
fi
