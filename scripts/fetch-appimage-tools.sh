#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tools_dir="$root_dir/.tools"
mkdir -p "$tools_dir"

# Official linuxdeploy continuous artifacts, pinned by SHA-256 so an updated
# continuous release cannot silently change the toolchain used for a build.
fetch() {
    local name="$1" sha256="$2" url="$3" destination="$tools_dir/$1"
    if [[ -f "$destination" ]] && echo "$sha256  $destination" | sha256sum --check --status; then
        return
    fi
    local temporary="$destination.part"
    rm -f "$temporary"
    curl --fail --location --retry 3 --output "$temporary" "$url"
    echo "$sha256  $temporary" | sha256sum --check --status || {
        rm -f "$temporary"
        echo "SHA-256 verification failed for $name" >&2
        return 1
    }
    chmod 0755 "$temporary"
    mv "$temporary" "$destination"
}

fetch linuxdeploy-x86_64.AppImage \
    8aea8da0f7f7039d2a2cecb14657d752a222a5e1d3825caeef186c82f751cdd1 \
    https://github.com/linuxdeploy/linuxdeploy/releases/download/continuous/linuxdeploy-x86_64.AppImage
fetch linuxdeploy-plugin-qt-x86_64.AppImage \
    cfc1055b2b9dbc08412b579f20990b7b41a17b61beaa5847dc9477c96c9e9617 \
    https://github.com/linuxdeploy/linuxdeploy-plugin-qt/releases/download/continuous/linuxdeploy-plugin-qt-x86_64.AppImage
fetch linuxdeploy-plugin-appimage-x86_64.AppImage \
    49d6a17160675a6bd1781699aae6bdf7692d98552e02a3671d2183d10547842e \
    https://github.com/linuxdeploy/linuxdeploy-plugin-appimage/releases/download/continuous/linuxdeploy-plugin-appimage-x86_64.AppImage
fetch appimage-runtime-x86_64 \
    2fca8b443c92510f1483a883f60061ad09b46b978b2631c807cd873a47ec260d \
    https://github.com/AppImage/type2-runtime/releases/download/20251108/runtime-x86_64
