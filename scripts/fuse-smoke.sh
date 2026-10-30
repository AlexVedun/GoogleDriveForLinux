#!/usr/bin/env bash
set -euo pipefail

binary=${1:-./bin/drivefs}
if [[ ! -x "$binary" ]]; then
	printf 'drivefs binary not found or not executable: %s\n' "$binary" >&2
	exit 2
fi
if [[ ! -e /dev/fuse ]]; then
	printf '/dev/fuse is unavailable; run this integration check on a host with user FUSE access\n' >&2
	exit 77
fi
for command in fusermount3 mountpoint cmp sync; do
	if ! command -v "$command" >/dev/null 2>&1; then
		printf 'required command not found: %s\n' "$command" >&2
		exit 2
	fi
done

tmp=$(mktemp -d)
source_dir="$tmp/source"
mount_dir="$tmp/mount"
mkdir "$source_dir" "$mount_dir"
printf 'FUSE loopback smoke test\n' > "$source_dir/read-me.txt"

pid=''
cleanup() {
	if mountpoint -q "$mount_dir"; then
		fusermount3 -u "$mount_dir" || true
	fi
	if [[ -n "$pid" ]]; then
		kill "$pid" 2>/dev/null || true
		wait "$pid" 2>/dev/null || true
	fi
	rm -rf "$tmp"
}
trap cleanup EXIT

"$binary" --source "$source_dir" --mountpoint "$mount_dir" &
pid=$!
mounted=false
for _ in $(seq 1 100); do
	if mountpoint -q "$mount_dir"; then
		mounted=true
		break
	fi
	if ! kill -0 "$pid" 2>/dev/null; then
		wait "$pid" || true
		break
	fi
	sleep 0.1
done
if [[ "$mounted" != true ]]; then
	printf 'drivefs did not mount within 10 seconds\n' >&2
	exit 1
fi

cmp "$source_dir/read-me.txt" "$mount_dir/read-me.txt"
printf 'created through FUSE\n' > "$mount_dir/new.txt"
sync -f "$mount_dir/new.txt"
cmp "$source_dir/new.txt" "$mount_dir/new.txt"
mv "$mount_dir/read-me.txt" "$mount_dir/renamed.txt"
cmp "$source_dir/renamed.txt" "$mount_dir/renamed.txt"
mkdir "$mount_dir/empty-dir"
rmdir "$mount_dir/empty-dir"
rm "$mount_dir/new.txt"

fusermount3 -u "$mount_dir"
wait "$pid"
pid=''
printf 'FUSE loopback smoke test passed\n'
