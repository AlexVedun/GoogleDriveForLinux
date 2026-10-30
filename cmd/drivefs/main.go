package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"

	"google-drive-linux/internal/fusefs"
	"google-drive-linux/internal/store"
)

const version = "0.0.0-dev"

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	database := flag.String("database", "", "SQLite metadata database for the indexed Drive namespace")
	source := flag.String("source", "", "local source directory for the loopback FUSE spike")
	mountpoint := flag.String("mountpoint", "", "existing empty directory to mount on")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
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
