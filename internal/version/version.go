package version

// Version is the application version shared by driveapp and drivefs.
// Release builds override it via -ldflags "-X google-drive-linux/internal/version.Version=...".
var Version = "0.0.0-dev"
