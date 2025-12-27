package main

import (
	"flag"
	"fmt"
	"os"
)

var (
	version   = "dev"
	buildTime = "unknown"
)

func main() {
	var (
		showVersion = flag.Bool("version", false, "Print version and exit")
		configPath  = flag.String("config", "", "Path to configuration file")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("drayd version %s (built %s)\n", version, buildTime)
		os.Exit(0)
	}

	if *configPath != "" {
		fmt.Printf("Using config file: %s\n", *configPath)
	}

	fmt.Println("Dray broker starting...")
	fmt.Println("Not yet implemented. See task_list.json for implementation tasks.")
	os.Exit(0)
}
