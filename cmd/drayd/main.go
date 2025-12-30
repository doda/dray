package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dray-io/dray/internal/config"
	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/server"
	"github.com/google/uuid"
)

var (
	version   = "dev"
	buildTime = "unknown"
	gitCommit = "unknown"
)

func main() {
	// Handle version flag before subcommand parsing
	if len(os.Args) > 1 && (os.Args[1] == "--version" || os.Args[1] == "-version") {
		fmt.Printf("drayd version %s (built %s)\n", version, buildTime)
		os.Exit(0)
	}

	// Check for subcommand
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	subcommand := os.Args[1]
	switch subcommand {
	case "broker":
		runBroker(os.Args[2:])
	case "compactor":
		runCompactor(os.Args[2:])
	case "admin":
		runAdmin(os.Args[2:])
	case "version":
		fmt.Printf("drayd version %s (built %s, commit %s)\n", version, buildTime, gitCommit)
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", subcommand)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Usage: drayd <command> [options]

Commands:
  broker      Start the Kafka protocol server (broker mode)
  compactor   Start the compaction worker
  admin       Administrative commands (topics, groups, configs, status)
  version     Print version information

Run 'drayd <command> --help' for more information on a command.`)
}

func runBroker(args []string) {
	fs := flag.NewFlagSet("broker", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to configuration file")
	listenAddr := fs.String("listen", "", "Override listen address (e.g., :9092)")
	healthAddr := fs.String("health-addr", "", "Override health endpoint address (e.g., :9090)")
	brokerID := fs.String("broker-id", "", "Override broker ID (default: auto-generated UUID)")
	nodeID := fs.Int("node-id", -1, "Override Kafka node ID (default: 1)")
	zoneID := fs.String("zone-id", "", "Override availability zone ID")
	clusterID := fs.String("cluster-id", "", "Override cluster ID (default: from config)")

	fs.Usage = func() {
		fmt.Println(`Usage: drayd broker [options]

Start the Dray broker (Kafka protocol server).

Options:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	// Load configuration
	var cfg *config.Config
	var err error
	if *configPath != "" {
		cfg, err = config.LoadFromPath(*configPath)
	} else {
		cfg, err = config.Load()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Apply CLI overrides
	if *listenAddr != "" {
		cfg.Broker.ListenAddr = *listenAddr
	}
	if *healthAddr != "" {
		cfg.Observability.MetricsAddr = *healthAddr
	}
	if *zoneID != "" {
		cfg.Broker.ZoneID = *zoneID
	}
	if *clusterID != "" {
		cfg.ClusterID = *clusterID
	}

	// Set up logger
	logger := logging.New(logging.Config{
		Level:  logging.ParseLevel(cfg.Observability.LogLevel),
		Format: logging.ParseFormat(cfg.Observability.LogFormat),
	})

	// Build broker options
	brokerOpts := BrokerOptions{
		Config:    cfg,
		Logger:    logger,
		Version:   version,
		GitCommit: gitCommit,
		BuildTime: buildTime,
	}

	// Set broker ID
	if *brokerID != "" {
		brokerOpts.BrokerID = *brokerID
	} else {
		brokerOpts.BrokerID = uuid.New().String()
	}

	// Set node ID
	if *nodeID >= 0 {
		brokerOpts.NodeID = int32(*nodeID)
	} else {
		brokerOpts.NodeID = 1
	}

	// Set cluster ID
	brokerOpts.ClusterID = cfg.ClusterID

	// Create and run broker
	broker, err := NewBroker(brokerOpts)
	if err != nil {
		logger.Errorf("failed to create broker", map[string]any{"error": err.Error()})
		os.Exit(1)
	}

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start the broker
	errCh := make(chan error, 1)
	go func() {
		errCh <- broker.Start(ctx)
	}()

	// Wait for shutdown signal or error
	select {
	case sig := <-sigCh:
		logger.Infof("received shutdown signal", map[string]any{"signal": sig.String()})
	case err := <-errCh:
		if err != nil && err != server.ErrServerClosed {
			logger.Errorf("broker error", map[string]any{"error": err.Error()})
			os.Exit(1)
		}
	}

	// Graceful shutdown
	logger.Info("initiating graceful shutdown")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := broker.Shutdown(shutdownCtx); err != nil {
		logger.Errorf("shutdown error", map[string]any{"error": err.Error()})
		os.Exit(1)
	}

	logger.Info("broker shutdown complete")
}

func runCompactor(args []string) {
	fs := flag.NewFlagSet("compactor", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to configuration file")
	healthAddr := fs.String("health-addr", "", "Override health endpoint address (e.g., :9090)")
	compactorID := fs.String("compactor-id", "", "Override compactor ID (default: auto-generated UUID)")

	fs.Usage = func() {
		fmt.Println(`Usage: drayd compactor [options]

Start the Dray compaction worker.

The compactor scans streams for WAL entries eligible for compaction,
converts them to Parquet format, and updates the offset index atomically.

Options:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	// Load configuration
	var cfg *config.Config
	var err error
	if *configPath != "" {
		cfg, err = config.LoadFromPath(*configPath)
	} else {
		cfg, err = config.Load()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Apply CLI overrides
	if *healthAddr != "" {
		cfg.Observability.MetricsAddr = *healthAddr
	}

	// Set up logger
	logger := logging.New(logging.Config{
		Level:  logging.ParseLevel(cfg.Observability.LogLevel),
		Format: logging.ParseFormat(cfg.Observability.LogFormat),
	})

	// Build compactor options
	compactorOpts := CompactorOptions{
		Config:    cfg,
		Logger:    logger,
		Version:   version,
		GitCommit: gitCommit,
		BuildTime: buildTime,
	}

	// Set compactor ID
	if *compactorID != "" {
		compactorOpts.CompactorID = *compactorID
	} else {
		compactorOpts.CompactorID = uuid.New().String()
	}

	// Create and run compactor
	compactor, err := NewCompactor(compactorOpts)
	if err != nil {
		logger.Errorf("failed to create compactor", map[string]any{"error": err.Error()})
		os.Exit(1)
	}

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start the compactor
	errCh := make(chan error, 1)
	go func() {
		errCh <- compactor.Start(ctx)
	}()

	// Wait for shutdown signal or error
	select {
	case sig := <-sigCh:
		logger.Infof("received shutdown signal", map[string]any{"signal": sig.String()})
	case err := <-errCh:
		if err != nil {
			logger.Errorf("compactor error", map[string]any{"error": err.Error()})
			os.Exit(1)
		}
	}

	// Graceful shutdown
	logger.Info("initiating graceful shutdown")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := compactor.Shutdown(shutdownCtx); err != nil {
		logger.Errorf("shutdown error", map[string]any{"error": err.Error()})
		os.Exit(1)
	}

	logger.Info("compactor shutdown complete")
}
