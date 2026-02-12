package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/KernelPryanic/golog"
	"github.com/chinese-room-solutions/mass/internal/agent"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/gui"
	"github.com/chinese-room-solutions/mass/internal/installer"
	"github.com/chinese-room-solutions/mass/internal/llm"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/tlsutil"
	"github.com/chinese-room-solutions/mass/internal/web"
	"github.com/rs/zerolog"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"gopkg.in/natefinch/lumberjack.v2"
)

func main() {
	headless := flag.Bool("headless", false, "Don't open the webview window or browser")
	flag.Parse()

	// In headless mode, attach to the parent console (or allocate one)
	// so stderr output is visible. In GUI mode the exe is built with
	// -H windowsgui so no console is created.
	if *headless {
		attachOrAllocConsole()
	}

	cfgPath, err := config.DefaultPath()
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal: getting config path:", err)
		os.Exit(1)
	}

	// Set up log file with rotation in the config directory.
	logsDir := config.LogsDir(cfgPath)
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		fmt.Fprintln(os.Stderr, "fatal: creating logs directory:", err)
		os.Exit(1)
	}
	logFile := &lumberjack.Logger{
		Filename:   filepath.Join(logsDir, "mass.log"),
		MaxSize:    2, // megabytes
		MaxBackups: 3,
	}

	sysLog := web.NewSystemLogBuffer(1000)

	// Console-formatted output goes to the syslog buffer (web UI)
	// and to stderr in headless mode. JSON goes to the log file.
	var consoleWriters []io.Writer
	if *headless {
		consoleWriters = []io.Writer{os.Stderr, sysLog}
	} else {
		consoleWriters = []io.Writer{sysLog}
	}
	consoleOut := zerolog.ConsoleWriter{
		Out:        io.MultiWriter(consoleWriters...),
		TimeFormat: zerolog.TimeFieldFormat,
	}
	logger := golog.New(false, io.MultiWriter(consoleOut, logFile))

	cfg, firstRun, err := config.Load(cfgPath)
	if err != nil {
		logger.Fatal().Err(err).Msg("loading config")
	}

	// Write defaults on first run so the config file exists on disk.
	if firstRun {
		if err := config.Save(cfg, cfgPath); err != nil {
			logger.Error().Err(err).Msg("writing default config")
		}
	}

	// Apply logger settings from config.
	zerolog.SetGlobalLevel(zerolog.Level(cfg.Logger.Level))

	// Ensure data directory exists.
	dataDir, err := cfg.EffectiveDataDir()
	if err != nil {
		logger.Fatal().Err(err).Msg("resolving data directory")
	}
	if mkErr := os.MkdirAll(dataDir, 0755); mkErr != nil {
		logger.Fatal().Err(mkErr).Str("dir", dataDir).Msg("creating data directory")
	}

	saveFn := func() {
		if err := config.Save(cfg, cfgPath); err != nil {
			logger.Error().Err(err).Msg("saving config")
		}
	}

	// Create agent registry with built-in local agent.
	bencher := &llm.Bencher{}
	localAgent := agent.NewLocalAgent(llm.NewLlamaLoader(), bencher)
	agents := agent.NewRegistry()
	if err := agents.Register(localAgent); err != nil {
		logger.Fatal().Err(err).Msg("registering local agent")
	}

	orch := scheduler.New(cfg, saveFn, logger, agents)

	// Register saved modules from disk metadata (no subprocess started).
	for i := range cfg.Modules {
		if cfg.Modules[i].Command == "" {
			logger.Warn().Str("module", cfg.Modules[i].Name).Msg("skipping module with empty command")
			continue
		}
		if err := orch.Register(&cfg.Modules[i]); err != nil {
			logger.Warn().Err(err).Str("module", cfg.Modules[i].Name).Msg("failed to register module")
		}
	}

	// Auto-start modules based on config.
	for i := range cfg.Modules {
		if cfg.Modules[i].AutoStart {
			if err := orch.Start(cfg.Modules[i].Name); err != nil {
				logger.Warn().Err(err).Str("module", cfg.Modules[i].Name).Msg("failed to auto-start module")
			}
		} else if cfg.Modules[i].EffectiveLaunchMode() == config.LaunchModeOnDemand {
			logger.Info().Str("module", cfg.Modules[i].Name).Msg("on-demand module ready")
		}
	}

	// Create module installer.
	inst := installer.NewInstaller("", config.ModuleInstallDir(dataDir), logger)

	// Open persistent store.
	dbPath := filepath.Join(dataDir, "mass.db")
	appStore, err := store.Open(dbPath)
	if err != nil {
		logger.Fatal().Err(err).Msg("opening database")
	}

	// Initialize two-level queue subsystem (global + per-device queues).
	orch.InitQueue(appStore.DB(), appStore)

	// Resolve the auth token hash.
	// Priority: MASS_AUTH_TOKEN env var > legacy config.yml > SQLite DB.
	var authHash []byte
	if envToken := os.Getenv("MASS_AUTH_TOKEN"); envToken != "" {
		authHash, err = bcrypt.GenerateFromPassword([]byte(envToken), bcrypt.DefaultCost)
		if err != nil {
			logger.Fatal().Err(err).Msg("hashing env auth token")
		}
	} else if cfg.AuthToken != "" {
		// Migrate legacy plain-text token from config.yml to bcrypt hash in DB.
		authHash, err = bcrypt.GenerateFromPassword([]byte(cfg.AuthToken), bcrypt.DefaultCost)
		if err != nil {
			logger.Fatal().Err(err).Msg("hashing legacy auth token")
		}
		if err := appStore.SetSetting("auth_token", string(authHash)); err != nil {
			logger.Fatal().Err(err).Msg("storing migrated auth token")
		}
		cfg.AuthToken = ""
		saveFn()
		logger.Info().Msg("migrated auth token from config.yml to database")
	} else {
		// Read bcrypt hash from DB.
		stored, err := appStore.GetSetting("auth_token")
		if err != nil {
			logger.Fatal().Err(err).Msg("reading auth token from database")
		}
		if stored != "" {
			authHash = []byte(stored)
		}
	}

	// Create session store for browser cookie sessions.
	sessions := web.NewSessionStore(30 * 24 * time.Hour)

	// Create web handler.
	handler, err := web.NewHandler(cfg, orch, inst, saveFn, logger, appStore, authHash, sessions, sysLog, agents)
	if err != nil {
		logger.Fatal().Err(err).Msg("creating web handler")
	}

	// Wrap with auth middleware (reads live authHash from handler on each request).
	authedHandler := handler.AuthMiddleware(handler)

	addr := cfg.EffectiveListenAddr()
	useTLS := cfg.TLS.Enabled

	var srv *http.Server
	if useTLS {
		tlsCfg, err := tlsutil.ServerTLSConfig(cfg.TLS)
		if err != nil {
			logger.Error().Err(err).Msg("TLS misconfigured, falling back to plaintext")
			useTLS = false
		} else {
			srv = &http.Server{
				Addr:      addr,
				Handler:   authedHandler,
				TLSConfig: tlsCfg,
			}
		}
	}
	if !useTLS {
		srv = &http.Server{
			Addr:    addr,
			Handler: h2c.NewHandler(authedHandler, &http2.Server{}),
		}
	}

	// Graceful shutdown (idempotent via sync.Once).
	done := make(chan struct{})
	var shutdownOnce sync.Once
	shutdown := func() {
		shutdownOnce.Do(func() {
			logger.Info().Msg("shutting down")
			orch.ShutdownAll()
			if err := appStore.Close(); err != nil {
				logger.Error().Err(err).Msg("closing database")
			}
			if err := srv.Shutdown(context.Background()); err != nil {
				logger.Error().Err(err).Msg("shutting down HTTP server")
			}
			close(done)
		})
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		shutdown()
	}()

	// Start server in background.
	go func() {
		var listenErr error
		if useTLS {
			listenErr = srv.ListenAndServeTLS("", "")
		} else {
			listenErr = srv.ListenAndServe()
		}
		if listenErr != nil && listenErr != http.ErrServerClosed {
			logger.Fatal().Err(listenErr).Msg("HTTP server error")
		}
	}()

	scheme := "http"
	if useTLS {
		scheme = "https"
	}
	url := scheme + "://localhost" + addr
	logger.Info().Str("url", url).Msg("MASS web UI starting")

	if *headless {
		// Block until shutdown completes (triggered by signal handler).
		<-done
		return
	}

	// Open a native webview window on the main thread.
	// The HTTP server runs in the background; browser access still works.
	wv := gui.New("MASS", url, 1440, 900, cfg.Theme != "light")
	if wv == nil {
		logger.Warn().Msg("could not create webview window, running headless")
		fmt.Fprintln(os.Stderr, "warning: webview unavailable (missing WebView2 runtime?), running in headless mode")
		fmt.Fprintln(os.Stderr, "access the UI at", url)
		<-done
		return
	}
	handler.SetOnThemeChange(wv.SetDarkMode)
	defer wv.Destroy()
	wv.Run()
	shutdown()
}
