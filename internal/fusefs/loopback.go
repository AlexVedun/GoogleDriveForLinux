//go:build linux

package fusefs

import (
	"fmt"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// MountLoopback mounts a local directory as a disposable FUSE protocol spike.
// It is not the Google Drive filesystem implementation.
func MountLoopback(source, mountpoint string) (*fuse.Server, error) {
	resolvedSource, resolvedMount, err := ValidatePaths(source, mountpoint)
	if err != nil {
		return nil, err
	}
	root, err := fs.NewLoopbackRoot(resolvedSource)
	if err != nil {
		return nil, fmt.Errorf("create loopback filesystem: %w", err)
	}
	options := &fs.Options{
		EntryTimeout: nil,
		AttrTimeout:  nil,
		MountOptions: fuse.MountOptions{
			FsName:        "drive-fuse-spike",
			Name:          "drive-fuse-spike",
			DisableXAttrs: true,
		},
	}
	server, err := fs.Mount(resolvedMount, root, options)
	if err != nil {
		return nil, fmt.Errorf("mount %q: %w", resolvedMount, err)
	}
	return server, nil
}
