#!/usr/bin/env bash
set -euo pipefail

app_dir="${1:?usage: bundle-appimage-rpm-licenses.sh APPDIR}"
doc_dir="$app_dir/usr/share/doc/google-drive-linux"
license_dir="$doc_dir/licenses/system"
report="$doc_dir/licenses/system-components.tsv"

if ! command -v rpm >/dev/null 2>&1; then
    echo "RPM metadata unavailable; bundle a platform-specific license inventory before distribution." >&2
    exit 0
fi

mkdir -p "$license_dir"
printf 'package\tlicense-expression\n' > "$report"
declare -A packages=()
declare -A libraries=()
while IFS= read -r line; do
    soname="$(awk '{print $1}' <<< "$line")"
    path="$(awk '{print $NF}' <<< "$line")"
    if [[ "$path" == /* ]]; then
        libraries["$soname"]+=" $path"
    fi
done < <(ldconfig -p 2>/dev/null | tail -n +2)

plugin_roots=()
for candidate in /usr/lib64/qt6/plugins /usr/lib/qt6/plugins /usr/lib/x86_64-linux-gnu/qt6/plugins; do
    [[ -d "$candidate" ]] && plugin_roots+=("$candidate")
done

unmapped=0
while IFS= read -r bundled_file; do
    bundled_name="$(basename "$bundled_file")"
    candidates=()
    if [[ "$bundled_file" == "$app_dir/usr/lib/"* ]]; then
        read -r -a candidates <<< "${libraries[$bundled_name]:-}"
    else
        for plugin_root in "${plugin_roots[@]}"; do
            source_file="$(find "$plugin_root" -type f -name "$bundled_name" -print -quit)"
            [[ -n "$source_file" ]] && candidates+=("$source_file")
        done
    fi

    owner=""
    for source_file in "${candidates[@]}"; do
        if owner="$(rpm -qf --qf '%{NAME}\t%{LICENSE}' "$source_file" 2>/dev/null)"; then
            break
        fi
        owner=""
    done
    if [[ -z "$owner" ]]; then
        printf 'No installed RPM owner found for bundled file: %s\n' "$bundled_file" >&2
        unmapped=1
        continue
    fi
    package="${owner%%$'\t'*}"
    license="${owner#*$'\t'}"
    packages["$package"]="$license"
done < <(find "$app_dir/usr/lib" "$app_dir/usr/plugins" -type f \( -name '*.so' -o -name '*.so.*' \) -print | sort)

if (( unmapped != 0 )); then
    echo "Could not inventory every bundled shared library/plugin; refusing an incomplete RPM license report." >&2
    exit 1
fi

for package in "${!packages[@]}"; do
    printf '%s\t%s\n' "$package" "${packages[$package]}" >> "$report"
    while IFS= read -r source_file; do
        [[ "$source_file" == /usr/share/licenses/* && -f "$source_file" ]] || continue
        relative="${source_file#/usr/share/licenses/}"
        destination="$license_dir/$relative"
        mkdir -p "$(dirname "$destination")"
        install -m 0644 "$source_file" "$destination"
    done < <(rpm -ql "$package")
done
{ head -n 1 "$report"; tail -n +2 "$report" | sort; } > "$report.tmp"
mv "$report.tmp" "$report"
printf 'Bundled license texts and RPM metadata for %d system packages.\n' "${#packages[@]}"
