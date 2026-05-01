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
	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/downloads"
	"github.com/chinese-room-solutions/mass/internal/gui"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/runtimes"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/tlsutil"
	"github.com/chinese-room-solutions/mass/internal/web"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/rs/zerolog"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"gopkg.in/natefinch/lumberjack.v2"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

func init() {
	// Respect container memory limits (cgroup) with system fallback.
	_, _ = memlimit.SetGoMemLimitWithOpts(
		memlimit.WithProvider(
			memlimit.ApplyFallback(memlimit.FromCgroup, memlimit.FromSystem),
		),
	)
}

func main() {
	headless := flag.Bool("headless", false, "Don't open the webview window or browser")
	showVersion := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("mass", version)
		return
	}

	if *headless {
		attachOrAllocConsole()
	}

	cfgDir, err := config.DefaultDir()
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal: getting config dir:", err)
		os.Exit(1)
	}

	logsDir := config.LogsDir(cfgDir)
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		fmt.Fprintln(os.Stderr, "fatal: creating logs directory:", err)
		os.Exit(1)
	}
	logFile := &lumberjack.Logger{
		Filename:   filepath.Join(logsDir, "mass.log"),
		MaxSize:    2,
		MaxBackups: 3,
	}

	sysLog := web.NewSystemLogBuffer(1000)

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

	cfg, firstRun, err := config.Load(cfgDir)
	if err != nil {
		logger.Fatal().Err(err).Msg("loading config")
	}
	if firstRun {
		if err := config.Save(cfg, cfgDir); err != nil {
			logger.Error().Err(err).Msg("writing default config")
		}
	}

	zerolog.SetGlobalLevel(zerolog.Level(cfg.Logger.Level))

	logger.Info().Str("version", version).Msg("mass starting")
	cfgLogger := golog.WithCensoredSecretFields(logger.With(), "config", cfg).Logger()
	cfgLogger.Debug().Msg("loaded config")

	dataDir, err := cfg.EffectiveDataDir()
	if err != nil {
		logger.Fatal().Err(err).Msg("resolving data directory")
	}
	if mkErr := os.MkdirAll(dataDir, 0755); mkErr != nil {
		logger.Fatal().Err(mkErr).Str("dir", dataDir).Msg("creating data directory")
	}

	saveFn := func() {
		if err := config.Save(cfg, cfgDir); err != nil {
			logger.Error().Err(err).Msg("saving config")
		}
	}

	dbPath := filepath.Join(dataDir, "mass.db")
	st, err := store.Open(dbPath)
	if err != nil {
		logger.Fatal().Err(err).Msg("opening database")
	}

	// MASS is a coordinator. Workers register dynamically; the fleet starts
	// empty. Inference traffic fails until at least one worker connects and
	// at least one matching runtime gateway is installed.
	workers := worker.NewFleet()

	rtMgr, err := runtimes.NewManager(dataDir, st, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("opening runtimes manager")
	}
	rtMgr.SetLogLevel(zerolog.GlobalLevel().String())

	orch := scheduler.New(cfg, saveFn, logger, workers)
	rtMgr.SetScheduler(orch)

	// Models are owned by individual runtime gateways now — each one walks
	// its own {dataDir}/models/<runtime_name>/ subdir, parses metadata,
	// renders its own list HTML, and handles deletes via the HTTP-over-gRPC
	// proxy. MASS just aggregates the per-runtime fragments into the
	// Models tab.

	// Downloads: persistent in-flight + paused HTTP fetches into the
	// shared models dir. Recover() restores rows from the previous
	// session as paused — operator clicks Resume to pick them up.
	dlMgr := downloads.NewManager(st, config.ModelsDir(dataDir), logger)
	dlMgr.Recover()

	queuePool := queue.NewPool(st.DB())
	results := queue.NewResultStore(st.DB())
	orch.InitQueue(queuePool, results)

	// Worker hub: workers connect here and are gated on having a matching
	// installed runtime kind.
	massURL := "http://localhost" + cfg.EffectiveListenAddr()
	hub := worker.NewHub(workers, massURL, config.ModelsDir(dataDir), nil, rtMgr.IsInstalled, logger)
	// Resync per-device enable whitelist on every worker reconnect (workers
	// are stateless; MASS holds the persisted operator intent).
	hub.SetEnabledDevicesProvider(func(workerID string, advertised []string) []string {
		rows, err := st.ListWorkerDevicesEnabled(workerID)
		if err != nil {
			return advertised
		}
		state := make(map[string]bool, len(rows))
		for _, r := range rows {
			state[r.DeviceID] = r.Enabled
		}
		out := make([]string, 0, len(advertised))
		for _, id := range advertised {
			v, ok := state[id]
			if !ok || v {
				out = append(out, id)
			}
		}
		return out
	})

	// Per-worker disable: the scheduler skips a worker only when every
	// advertised device is explicitly disabled. Devices without a
	// persisted row default to enabled (mirrors the EnabledDevices
	// provider above) — toggling one device off must not implicitly
	// disable the others.
	orch.SetWorkerEnabledFn(func(workerID string) bool {
		w := workers.Get(workerID)
		if w == nil {
			return true // unknown worker: don't filter (race with disconnect)
		}
		devices := w.Devices()
		if len(devices) == 0 {
			return true // pre-heartbeat race: assume enabled
		}
		rows, err := st.ListWorkerDevicesEnabled(workerID)
		if err != nil {
			return true
		}
		state := make(map[string]bool, len(rows))
		for _, r := range rows {
			state[r.DeviceID] = r.Enabled
		}
		for _, d := range devices {
			v, ok := state[d.ID]
			if !ok || v {
				return true
			}
		}
		return false
	})

	cleanupCtx, cleanupCancel := context.WithCancel(context.Background())
	defer cleanupCancel()
	orch.StartResultCleanup(cleanupCtx)
	orch.StartIdleEviction(cleanupCtx)

	// Resolve auth token hash. Priority: env > legacy config.yml > DB.
	var authHash []byte
	if envToken := os.Getenv("MASS_AUTH_TOKEN"); envToken != "" {
		authHash, err = bcrypt.GenerateFromPassword([]byte(envToken), bcrypt.DefaultCost)
		if err != nil {
			logger.Fatal().Err(err).Msg("hashing env auth token")
		}
	} else if cfg.AuthToken != "" {
		authHash, err = bcrypt.GenerateFromPassword([]byte(cfg.AuthToken), bcrypt.DefaultCost)
		if err != nil {
			logger.Fatal().Err(err).Msg("hashing legacy auth token")
		}
		if err := st.SetSetting("auth_token", string(authHash)); err != nil {
			logger.Fatal().Err(err).Msg("storing migrated auth token")
		}
		cfg.AuthToken = ""
		saveFn()
		logger.Info().Msg("migrated auth token from config.yml to database")
	} else {
		stored, err := st.GetSetting("auth_token")
		if err != nil {
			logger.Fatal().Err(err).Msg("reading auth token from database")
		}
		if stored != "" {
			authHash = []byte(stored)
		}
	}

	sessions := web.NewSessionStore(30 * 24 * time.Hour)

	handler, err := web.NewHandler(web.HandlerOptions{
		Config:    cfg,
		Scheduler: orch,
		Runtimes:  rtMgr,
		Downloads: dlMgr,
		Store:     st,
		SaveFn:    saveFn,
		Logger:    logger,
		AuthHash:  authHash,
		Sessions:  sessions,
		SysLog:    sysLog,
		Workers:   workers,
		WorkerHub: hub,
		ConfigDir: cfgDir,
		LogsDir:   logsDir,
		DataDir:   dataDir,
	})
	if err != nil {
		logger.Fatal().Err(err).Msg("creating web handler")
	}

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

	done := make(chan struct{})
	var shutdownOnce sync.Once
	shutdown := func() {
		shutdownOnce.Do(func() {
			logger.Info().Msg("shutting down")
			cleanupCancel()
			rtMgr.Shutdown()
			orch.ShutdownAll()
			if err := st.Close(); err != nil {
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

	// Launch any runtimes flagged auto-start. Done in the background so
	// gateway handshake latency doesn't block the dashboard from coming
	// up — operators see the UI immediately and gateways pop in as they
	// finish initializing.
	go rtMgr.AutoStartAll(cleanupCtx)

	if *headless {
		<-done
		return
	}

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
