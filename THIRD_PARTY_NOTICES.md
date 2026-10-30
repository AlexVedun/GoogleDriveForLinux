# Third-party software notices

The AppImage bundles the following open-source components. Copyright and
license texts shipped with the application are in
`usr/share/doc/google-drive-linux/licenses/` (in the AppImage or extracted
directory). On RPM-based build hosts, `licenses/system-components.tsv` records
the RPM package and license expression for each bundled shared library and Qt
plugin; the corresponding package license files are copied alongside it.

| Component | License | Bundled use |
|---|---|---|
| go-fuse/v2 | BSD 3-Clause | FUSE protocol implementation |
| MIQT | MIT | Go bindings to Qt |
| go-sqlite3 | MIT; SQLite amalgamation is public domain | SQLite storage |
| go-keyring | MIT | Secret Service integration |
| godbus/dbus | BSD 2-Clause | D-Bus client |
| golang.org/x/oauth2 | BSD 3-Clause | OAuth client |
| golang.org/x/sys | BSD 3-Clause | Linux system calls |
| Qt 6 shared libraries and platform plugins | LGPL 3.0 or GPL 3.0 with Qt exception, subject to the terms selected by the Qt build | GUI, D-Bus, X11, Wayland, and offscreen platform support |
| Additional shared libraries and Qt plugins found on the packaging host | See the generated RPM inventory for RPM builds | Transitive Qt dependencies and optional platform/image plugins |
| AppImage type-2 runtime | MIT; see its notice for embedded components | AppImage launcher/runtime |

Qt is dynamically linked. The corresponding LGPL text is included. RPM builds
also include the exact package metadata and license files for the bundled Qt
plugins and transitive shared libraries. Non-RPM builds need an equivalent
inventory. Before public distribution, review the resulting inventory for
copyleft and patent-license obligations, provide any required corresponding
source or Qt relinking materials, and confirm the applicable Qt licensing
option. This notice and package metadata are not a substitute for that release
review.

The application requests the Google Drive `drive` OAuth scope. Google treats
this as a restricted scope; a public service or binary release may require
Google's OAuth verification and, depending on how data is handled, a security
assessment. See the [scope list](https://developers.google.com/identity/protocols/oauth2/scopes),
[Drive API authorization guidance](https://developers.google.com/workspace/drive/api/guides/api-specific-auth),
and [restricted-scope verification requirements](https://developers.google.com/identity/protocols/oauth2/production-readiness/restricted-scope-verification).
