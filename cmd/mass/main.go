package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/KernelPryanic/golog"
	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/chinese-room-solutions/mass-sdk/uikit"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/downloads"
	"github.com/chinese-room-solutions/mass/internal/modelscan"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/runtimes"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/tlsutil"
	"github.com/chinese-room-solutions/mass/internal/web"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/rs/zerolog"
	"golang.org/x/crypto/bcrypt"
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
	// Subcommand dispatch: a first non-flag argument (e.g. `mass status`)
	// runs the management CLI and exits. `-headless`/`-version` fall through
	// to the server path below.
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		os.Exit(runCLI(os.Args[1:]))
	}

	headless := flag.Bool("headless", false, "Don't open the webview window or browser")
	showVersion := flag.Bool("version", false, "Print version and exit")
	// `mass --help` lands here (leading dash skips the CLI dispatch above), so
	// stdlib usage alone would hide the management verbs. Show both.
	flag.Usage = func() {
		usage()
		fmt.Fprintln(os.Stderr, "\nServer flags (bare `mass` starts the app):")
		flag.PrintDefaults()
	}
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

	// Load pluggable themes from the shared themes dir (seeding the SDK's
	// example on first run). A bad theme file must not stop the app, so a
	// load error is only a warning.
	if err := uikit.LoadThemes(); err != nil {
		logger.Warn().Err(err).Msg("loading pluggable themes")
	}
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

	dialect, dsn := cfg.EffectiveDB(dataDir)
	st, err := store.Open(store.Dialect(dialect), dsn)
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

	orch := scheduler.New(cfg, logger, workers)
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
	// Gateway-initiated installs (HF picker, etc.) call back into the
	// downloads manager via MassScheduler.DownloadFiles. Wire it before
	// Start() launches gateways or the callback fails Unavailable.
	rtMgr.SetDownloads(dlMgr)

	queuePool := queue.NewPool(st.DB(), st.Dialect())
	results := queue.NewResultStore(st.DB(), st.Dialect())
	orch.InitQueue(queuePool, results, st)
	// The models root resolves the store-relative keys per-model
	// benchmark rows are recorded under, so the scheduler can tell a
	// measurement apart from one taken on a since-changed file.
	orch.SetModelsDir(config.ModelsDir(dataDir))

	// Worker hub: workers connect here and are gated on having a matching
	// installed runtime kind.
	massURL := config.LocalURL("http", cfg.EffectiveListenAddr())
	// Canonical-set provider for worker cache reconciliation: the store-
	// relative keys still on disk. Memoized ~20s so the per-heartbeat
	// reconcile loop doesn't stat the tree on every tick; on a walk error it
	// returns empty, which reconcile treats as "unknown" and skips.
	canonScan := modelscan.New(config.ModelsDir(dataDir), 20*time.Second, logger)
	// Enroller owns the join-token + per-worker-credential lifecycle, shared by
	// the hub (enroll/authenticate) and the control plane (mint join tokens).
	enroller := worker.NewEnroller(st)
	hub := worker.NewHub(workers, enroller, massURL, config.ModelsDir(dataDir), canonScan.Set, rtMgr.IsInstalled, logger)
	// Compat handshake: reject a worker whose declared compatible range
	// excludes the installed runtime's version. The runtimes manager is the
	// authority on the installed version (the join key into that range).
	hub.SetRuntimeVersionFn(func(runtimeName string) (string, bool) {
		mf, err := rtMgr.Get(runtimeName)
		if err != nil {
			return "", false
		}
		return mf.Version, true
	})
	// Resync per-device enable whitelist on every worker reconnect (workers
	// are stateless; MASS holds the persisted operator intent).
	hub.SetEnabledDevicesProvider(func(workerID string, advertised []string) worker.EnabledDevices {
		rows, err := st.ListWorkerDevicesEnabled(workerID)
		if err != nil {
			// Persisted intent unreadable: fail open, matching the
			// no-rows bootstrap default.
			return worker.EnabledDevices{All: true}
		}
		state := make(map[string]bool, len(rows))
		for _, r := range rows {
			state[r.DeviceID] = r.Enabled
		}
		return worker.ComputeEnabledDevices(advertised, state)
	})

	// Worker auth: the hub does its own credential auth per the join-token
	// enrollment contract (join token to enroll, per-worker secret in steady
	// state). The shared operator token is not accepted. SetAuthDisabledFn is
	// wired once the handler exists (it owns the live auth-hash state).

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

	// Per-device enable check: a (worker, device) pair is enabled unless an
	// explicit persisted row says otherwise. Mirrors the EnabledDevices
	// provider above so the dispatcher and the device whitelist agree.
	orch.SetDeviceEnabledFn(func(workerID, deviceID string) bool {
		rows, err := st.ListWorkerDevicesEnabled(workerID)
		if err != nil {
			return true
		}
		for _, r := range rows {
			if r.DeviceID == deviceID {
				return r.Enabled
			}
		}
		return true
	})

	// Materialise per-device queues whenever a worker connects, drain them
	// back to global on disconnect. The fleet exposes connect/update/remove
	// via one callback; we branch on the kind.
	workers.AddChangeCallback(func(evt worker.FleetChangeEvent) {
		switch evt.Kind {
		case worker.FleetChangeAdded:
			wi := workers.Get(evt.WorkerID)
			if sw, ok := wi.(*worker.StreamWorker); ok {
				orch.OnWorkerConnected(sw)
			}
		case worker.FleetChangeRemoved:
			orch.OnWorkerDisconnected(evt.WorkerID)
		}
	})

	cleanupCtx, cleanupCancel := context.WithCancel(context.Background())
	defer cleanupCancel()
	// Bench orchestration: what to measure comes from each gateway's
	// catalogue, the request to measure it with from AuthorBenchPayload.
	// A finished download re-benches that model across the fleet so the
	// first job for it doesn't pay for the measurement.
	orch.SetBenchProviders(rtMgr.BenchModels(config.ModelsDir(dataDir)), rtMgr.AuthorBenchPayload())
	dlMgr.SetOnComplete(orch.OnModelDownloaded)
	orch.StartBenchOrchestrator(cleanupCtx)

	orch.Start(cleanupCtx)
	orch.StartResultCleanup(cleanupCtx)
	orch.StartIdleEviction(cleanupCtx)

	// Resolve auth token hash. Priority: env > DB.
	var authHash []byte
	if envToken := os.Getenv("MASS_AUTH_TOKEN"); envToken != "" {
		authHash, err = bcrypt.GenerateFromPassword([]byte(envToken), bcrypt.DefaultCost)
		if err != nil {
			logger.Fatal().Err(err).Msg("hashing env auth token")
		}
	} else {
		stored, err := st.GetSetting("auth_token")
		if err != nil {
			logger.Fatal().Err(err).Msg("reading auth token from database")
		}
		if stored != "" {
			authHash = []byte(stored)
		}
	}

	// With no token configured, AuthMiddleware allows every request. That is
	// acceptable only when the server is unreachable from the network —
	// refuse to expose an unauthenticated dashboard + API on a non-loopback
	// bind.
	if len(authHash) == 0 && !config.IsLoopbackAddr(cfg.EffectiveListenAddr()) {
		logger.Fatal().Str("listen_addr", cfg.EffectiveListenAddr()).Msg(
			"refusing to start: no auth token is configured while the listen address is reachable from the network; " +
				"set the MASS_AUTH_TOKEN environment variable (or set a token in Settings while bound to loopback), " +
				"or set listen_addr to a loopback address such as \"127.0.0.1:3455\" in config.yml")
	}

	sessions := web.NewSessionStore(30 * 24 * time.Hour)
	// Hourly expired-session sweep; exits when cleanupCancel fires at shutdown.
	go sessions.Janitor(cleanupCtx)

	handler, err := web.NewHandler(web.HandlerOptions{
		Version:   version,
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
		Enroller:  enroller,
		ConfigDir: cfgDir,
		LogsDir:   logsDir,
		DataDir:   dataDir,
	})
	if err != nil {
		logger.Fatal().Err(err).Msg("creating web handler")
	}

	// The hub mirrors the dashboard's auth state: with no operator token
	// configured, workers may enroll without a join token.
	hub.SetAuthDisabledFn(handler.AuthDisabled)

	authedHandler := handler.MetricsMiddleware(handler.AuthMiddleware(handler))

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
				// No ReadTimeout/WriteTimeout: SSE and gRPC streams are
				// long-lived by design. These two only bound slow-header
				// clients and idle keep-alive connections.
				ReadHeaderTimeout: 10 * time.Second,
				IdleTimeout:       120 * time.Second,
			}
		}
	}
	if !useTLS {
		// Serve HTTP/1.1 (dashboard + SSE) and unencrypted HTTP/2 (plain
		// gRPC from runtime gateways and workers) on the same port. The
		// native Protocols field replaces the deprecated h2c handler wrapper
		// and, unlike it, keeps http.Flusher available on HTTP/1.1 requests.
		var protocols http.Protocols
		protocols.SetHTTP1(true)
		protocols.SetUnencryptedHTTP2(true)
		srv = &http.Server{
			Addr:      addr,
			Handler:   authedHandler,
			Protocols: &protocols,
			// No ReadTimeout/WriteTimeout: SSE and gRPC streams are
			// long-lived by design. These two only bound slow-header
			// clients and idle keep-alive connections.
			ReadHeaderTimeout: 10 * time.Second,
			IdleTimeout:       120 * time.Second,
		}
	}

	// Give every request a context derived from srvCtx. The SSE handlers
	// (dashboard, logs, model installs) block in a select on r.Context().Done(),
	// which without this only fires when the *client* disconnects. Cancelling
	// srvCtx at shutdown fires it for all in-flight streams so they return at
	// once and srv.Shutdown doesn't stall waiting for never-ending streams.
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	srv.BaseContext = func(net.Listener) context.Context { return srvCtx }

	done := make(chan struct{})
	var shutdownOnce sync.Once
	shutdown := func() {
		shutdownOnce.Do(func() {
			logger.Info().Msg("shutting down")
			cleanupCancel()
			// Drain HTTP before tearing anything else down — in-flight
			// requests still touch the runtime manager and the database.
			// Cancel in-flight request contexts first so the long-lived SSE
			// streams return at once; otherwise srv.Shutdown waits for them.
			// The timeout then only guards against a genuinely stuck
			// connection.
			srvCancel()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := srv.Shutdown(shutdownCtx); err != nil {
				logger.Error().Err(err).Msg("shutting down HTTP server")
			}
			rtMgr.Shutdown()
			if err := st.Close(); err != nil {
				logger.Error().Err(err).Msg("closing database")
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
	url := config.LocalURL(scheme, addr)
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

	runGUI(logger, cfg.Theme, url, handler, done, shutdown)
}
