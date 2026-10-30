package main

import (
	"flag"
	"fmt"
	"os"

	"google-drive-linux/internal/app"
)

const version = "0.0.0-dev"

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	smoke := flag.Bool("smoke", false, "run a short Qt event-loop and GUI-thread dispatch check")
	socket := flag.String("socket", "", "Unix socket path for the per-user drivefs service")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	if flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	os.Exit(app.RunQtApp(*smoke, *socket))
}
