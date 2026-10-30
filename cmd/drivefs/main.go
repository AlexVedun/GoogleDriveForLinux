package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"

	"google-drive-linux/internal/drivecontrol"
	"google-drive-linux/internal/fusefs"
	"google-drive-linux/internal/ipc"
	"google-drive-linux/internal/store"
)

const version = "0.0.0-dev"

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	ipcServer := flag.Bool("ipc-server", false, "run as the per-user drivefs service")
	socket := flag.String("socket", "", "Unix socket path for the per-user service")
	database := flag.String("database", "", "SQLite metadata database for the indexed Drive namespace")
	source := flag.String("source", "", "local source directory for the loopback FUSE spike")
	mountpoint := flag.String("mountpoint", "", "existing empty directory to mount on")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}
	if *ipcServer {
		if *database != "" || *source != "" || *mountpoint != "" {
			fmt.Fprintln(os.Stderr, "--ipc-server cannot be combined with direct mount flags")
			os.Exit(2)
		}
		if err := runIPCServer(*socket); err != nil {
			if errors.Is(err, ipc.ErrAlreadyRunning) {
				return
			}
			log.Fatalf("drivefs service: %v", err)
		}
		return
	}

	if *mountpoint == "" || (*database == "" && *source == "") || (*database != "" && *source != "") {
		flag.Usage()
		os.Exit(2)
	}

	var server *fuse.Server
	var metadata *store.Store
	var err error
	if *database != "" {
		metadata, err = store.Open(*database)
		if err != nil {
			log.Fatalf("open metadata database: %v", err)
		}
		server, err = fusefs.MountIndexed(metadata, *mountpoint)
	} else {
		server, err = fusefs.MountLoopback(*source, *mountpoint)
	}
	if err != nil {
		if metadata != nil {
			_ = metadata.Close()
		}
		log.Fatalf("start FUSE mount: %v", err)
	}
	if metadata != nil {
		defer metadata.Close()
		log.Printf("read-only indexed Drive namespace mounted at %s", *mountpoint)
	} else {
		log.Printf("FUSE loopback spike mounted at %s; this is a local-only prototype", *mountpoint)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signals
		if err := server.Unmount(); err != nil {
			log.Printf("unmount: %v", err)
		}
	}()
	server.Wait()
}

func runIPCServer(socketPath string) error {
	if socketPath == "" {
		var err error
		socketPath, err = ipc.DefaultSocketPath()
		if err != nil {
			return err
		}
	}
	controller := drivecontrol.New(func(metadata *store.Store, mountpoint string) (drivecontrol.MountHandle, error) {
		server, err := fusefs.MountIndexed(metadata, mountpoint)
		if err != nil {
			return drivecontrol.MountHandle{}, err
		}
		done := make(chan struct{})
		go func() {
			server.Wait()
			close(done)
		}()
		return drivecontrol.MountHandle{Unmount: server.Unmount, Done: done}, nil
	})
	server, err := ipc.Listen(filepath.Clean(socketPath), controller)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("drivefs IPC service listening on %s", socketPath)
	serveErr := server.Serve(ctx)
	if err := controller.Stop(); err != nil {
		return errors.Join(serveErr, fmt.Errorf("stop drivefs controller: %w", err))
	}
	return serveErr
}
