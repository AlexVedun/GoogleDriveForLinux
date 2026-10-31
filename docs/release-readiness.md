# Release readiness

**Status: not ready for public distribution.** The local implementation and
an x86_64 AppImage candidate exist, but several external acceptance and release
gates have not been completed.

## Validation completed locally

- `go test ./... -count=1` passed on Fedora 44 x86_64. It needed a short
  `TMPDIR` because the IPC tests use Unix socket paths subject to the kernel's
  path-length limit.
- `go vet ./...` passed.
- The full `go test -race ./...` gate was not completed. The parallel run hit
  the `/tmp` quota; a serial retry started another multi-minute generated
  MIQT/Qt C++ build and was stopped. Focused race suites for the store, cache,
  FUSE, indexer, worker, IPC, and controller passed in earlier steps.
- Earlier step checks passed for crash recovery and durability, fake Drive
  indexing and sync, queue reconciliation, conflicts, cache/pins, IPC
  lifecycle, and offscreen Qt. See [`docs/progress.md`](progress.md) for the
  commands and limits recorded for each step.
- `make appimage` produced an x86_64 AppImage containing `driveapp`,
  `drivefs`, Qt platform plugins, desktop metadata, and license notices. The
  image extracts with `--appimage-extract`; ordinary AppImage Qt smoke launches
  pass in this desktop session with Wayland, X11 (`xcb`), and offscreen after
  fixing linuxdeploy's ELF initialization corruption. Extracted offscreen Qt launch,
  bundled `drivefs --version`, and a per-user service start/quit smoke test
  passed. The smoke also confirmed the app reports Secret Service failure
  without falling back to a token file.
- The image was built on Fedora 44 and bundled Qt plus host-discovered plugins
  and shared libraries. Its RPM inventory lists 55 bundled system packages
  and carries 81 corresponding license files.

## Acceptance matrix

| Scenario | Evidence and status |
|---|---|
| Offline write, durable `fsync`, process kill, restart, and sync | Local store and worker recovery boundaries are tested separately with a fake Drive; not yet verified through a mounted filesystem and real Drive account. **Partial** |
| Remote edit while local work is pending | Fake Drive conflict tests preserve local and remote versions and exercise resolutions. The real `files.update` race is not atomic. **Local tests passed; residual API race remains** |
| Interrupted large resumable upload | Fake Drive worker tests cover accepted chunks, lost responses, restart, and authoritative offset reconciliation. **Local tests passed; no real Drive upload tested** |
| Editor temporary file followed by rename | FUSE-node/store tests cover local atomic replacement and queue order; live file-manager test not run. **Local tests passed** |
| Folder pin larger than 1 GiB | Quota accounting and overage use simulated metadata; a physical >1 GiB download was not performed. **Partial** |
| 10% free-space floor | Reservation and rejection behavior are tested with controlled filesystem capacity. **Local tests passed** |
| Thousands of objects and duplicate names | Large-index and stable-name behaviors have local tests; no live large Drive was used. **Local tests passed** |
| HTTP 401, 403, 404, 429, and 5xx | Fake-server tests verify error classes, safe retries, and non-repetition of ambiguous mutations. **Local tests passed** |
| GNOME and KDE, X11 and Wayland | Qt startup smoke passes in the current desktop session with Wayland and X11 (`xcb`). Full GUI and tray workflows across GNOME and KDE remain untested. **Partial** |
| Ordinary and extracted AppImage with OAuth, FUSE, Secret Service, and clean exit | Ordinary AppImage Qt startup smoke and extraction pass. Secret Service absence and IPC shutdown were checked without real credentials. Live OAuth, FUSE mount, tray, and desktop exit were not tested. **Partial** |

## Remaining public-release gates

1. Run the mounted FUSE matrix against a test Google account, including edits,
   deletion, conflict, restart, sign-out, and queue recovery.
2. Verify tray behavior, folder selection, mount/unmount, app close, and clean
   exit under GNOME and KDE on both X11 and Wayland where supported.
3. Decide how Google Docs, Sheets, and Slides appear in the mount and verify
   that representation and its launch prompts in GNOME Files and KDE Dolphin.
4. Select the oldest supported Linux/glibc baseline and rebuild/test there and
   on a newer system. The present Fedora 44 build does not establish that
   baseline.
5. Complete the component licensing review. The generated RPM manifest is
   useful evidence, but the project has no declared source license yet; the
   Qt LGPL relinking/source obligations and any bundled GPL/patent obligations
   still need review. Non-RPM builds need an equivalent dependency inventory.
6. Complete Google's OAuth verification for the full Drive scope unless the
   app's actual distribution/use qualifies for an exception. Confirm whether
   the final data flow triggers a security assessment. Google notes that
   restricted-scope verification can take several weeks.
7. Rebuild the final AppImage after all release decisions, then publish its
   hash and run the full acceptance matrix on the selected systems.

The current AppImage is a development candidate, not a release artifact. Its
SHA-256 is recorded in [`docs/progress.md`](progress.md); it must be rebuilt
and retested after the remaining release decisions and external acceptance
checks.
