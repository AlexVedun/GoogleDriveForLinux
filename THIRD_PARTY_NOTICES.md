# Third-party software notices

The AppImage bundles the following open-source components. Copyright and
license texts shipped with the application are in
`usr/share/doc/google-drive-linux/licenses/` (in the AppImage or extracted
directory).

| Component | License | Bundled use |
|---|---|---|
| go-fuse/v2 | BSD 3-Clause | FUSE protocol implementation |
| MIQT | MIT | Go bindings to Qt |
| go-sqlite3 | MIT; SQLite amalgamation is public domain | SQLite storage |
| go-keyring | MIT | Secret Service integration |
| godbus/dbus | BSD 2-Clause | D-Bus client |
| golang.org/x/oauth2 | BSD 3-Clause | OAuth client |
| golang.org/x/sys | BSD 3-Clause | Linux system calls |
| Qt 6 shared libraries and platform plugins | LGPL 3.0, subject to the terms selected by the Qt build | GUI, D-Bus, X11, Wayland, and offscreen platform support |
| AppImage type-2 runtime | MIT; see its notice for embedded components | AppImage launcher/runtime |

Qt is dynamically linked. The corresponding LGPL text is included. The exact
license metadata of bundled Qt libraries and any transitive shared-library
dependencies depends on the Linux distribution used for packaging. Before a
public binary release, audit the final AppDir against the package metadata of
that build environment, include any additional required notices and source or
relinking materials, and confirm the applicable Qt licensing option. This
notice is not a substitute for that release audit.

The application requests the Google Drive `drive` OAuth scope. Google treats
this as a restricted scope; a public service or binary release may require
Google's OAuth verification and, depending on how data is handled, a security
assessment. See the [scope list](https://developers.google.com/identity/protocols/oauth2/scopes),
[Drive API authorization guidance](https://developers.google.com/workspace/drive/api/guides/api-specific-auth),
and [restricted-scope verification requirements](https://developers.google.com/identity/protocols/oauth2/production-readiness/restricted-scope-verification).
