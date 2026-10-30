package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"google-drive-linux/internal/fusefs"
)

const version = "0.0.0-dev"

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	source := flag.String("source", "", "local source directory for the FUSE spike")
	mountpoint := flag.String("mountpoint", "", "existing empty directory to mount on")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	if *source == "" || *mountpoint == "" {
		flag.Usage()
		os.Exit(2)
	}

	server, err := fusefs.MountLoopback(*source, *mountpoint)
	if err != nil {
		log.Fatalf("start FUSE spike: %v", err)
	}
	log.Printf("FUSE loopback mounted at %s; this is a local-only prototype", *mountpoint)

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
