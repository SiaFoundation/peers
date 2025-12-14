package main

import (
	"context"
	"flag"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	eapi "go.sia.tech/explored/api"
	"go.sia.tech/peerd/api"
	"go.sia.tech/peerd/geoip"
	"go.sia.tech/peerd/peers"
	"go.sia.tech/peerd/persist/sqlite"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// initLog initializes the logger with the specified settings.
func initLog(showColors bool, logLevel zap.AtomicLevel) *zap.Logger {
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.RFC3339TimeEncoder
	cfg.EncodeDuration = zapcore.StringDurationEncoder

	if showColors {
		cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		cfg.EncodeLevel = zapcore.CapitalLevelEncoder
	}

	cfg.StacktraceKey = ""
	cfg.CallerKey = ""
	encoder := zapcore.NewConsoleEncoder(cfg)
	core := zapcore.NewCore(encoder, zapcore.Lock(os.Stdout), logLevel)
	log := zap.New(core, zap.AddCaller())

	zap.RedirectStdLog(log)
	return log
}

func main() {
	var (
		networkName  string
		dir          string
		level        zap.AtomicLevel
		scanInterval time.Duration
		scanThreads  int
		explorerURL  string
	)

	flag.StringVar(&networkName, "network", "mainnet", "the network to use (mainnet, zen)")
	flag.StringVar(&dir, "dir", ".", "the directory to store data")
	flag.StringVar(&explorerURL, "explorer", "", "the URL of the explorer API to use (default is network specific)")
	flag.TextVar(&level, "log.level", zap.NewAtomicLevelAt(zap.InfoLevel), "the log level")
	flag.DurationVar(&scanInterval, "scan.interval", 3*time.Hour, "the interval between successful scans")
	flag.IntVar(&scanThreads, "scan.threads", runtime.NumCPU(), "the number of threads to use for scanning")

	flag.Parse()

	log := initLog(runtime.GOOS != "windows", level)

	var network *consensus.Network
	var genesis types.Block
	var bootstrapPeers []string
	switch networkName {
	case "mainnet":
		bootstrapPeers = syncer.MainnetBootstrapPeers
		network, genesis = chain.Mainnet()
		if explorerURL == "" {
			explorerURL = "https://api.siascan.com"
		}
	case "zen":
		bootstrapPeers = syncer.ZenBootstrapPeers
		network, genesis = chain.TestnetZen()
		if explorerURL == "" {
			explorerURL = "https://api.siascan.com/zen"
		}
	}
	genesisID := genesis.ID()
	genesisState, _ := consensus.ApplyBlock(network.GenesisState(), genesis, consensus.V1BlockSupplement{Transactions: make([]consensus.V1TransactionSupplement, len(genesis.Transactions))}, time.Time{})
	log.Debug("using network", zap.String("name", networkName), zap.Stringer("genesisID", genesisID), zap.Stringer("genesisState", genesisState.Index))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Panic("failed to create data directory", zap.Error(err))
	}

	store, err := sqlite.OpenDatabase(filepath.Join(dir, "peerd.sqlite3"), log.Named("sqlite3"))
	if err != nil {
		log.Panic("failed to open database", zap.Error(err))
	}
	defer store.Close()

	explorer := eapi.NewClient(explorerURL, "")

	locator, err := geoip.NewMaxMindLocator("")
	if err != nil {
		log.Panic("failed to create geoip locator", zap.Error(err))
	}
	defer locator.Close()

	peers, err := peers.NewManager(explorer, locator, genesisState, genesisID, bootstrapPeers, store,
		peers.WithLogger(log.Named("peers")),
		peers.WithScanInterval(scanInterval),
		peers.WithScanThreads(scanThreads))
	if err != nil {
		log.Panic("failed to create peer manager", zap.Error(err))
	}
	defer peers.Close()

	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal("failed to listen", zap.Error(err))
	}
	defer l.Close()

	srv := &http.Server{
		ReadTimeout: 10 * time.Second,
		Handler:     api.NewHandler(peers),
	}
	defer srv.Close()

	go func() {
		if err := srv.Serve(l); err != nil && err != http.ErrServerClosed {
			log.Fatal("failed to serve", zap.Error(err))
		}
	}()

	<-ctx.Done()
}
