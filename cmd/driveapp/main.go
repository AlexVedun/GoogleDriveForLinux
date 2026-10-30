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
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	if flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	os.Exit(app.RunQtSpike(*smoke))
}
