package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/tlsutil"
	"github.com/chinese-room-solutions/mass/internal/web"
	"github.com/rs/zerolog"
)

// The launcher: how the GUI and the CLI find, replace, and start the local
// daemon. There is no port file — MASS binds the configured listen address
// (default 127.0.0.1:3455), so discovery is probing that address's
// /internal/daemon/ping. A daemon spawned here runs `mass serve
// --idle-timeout` detached and retires itself once its clients are gone.

const (
	// spawnedIdleTimeout is how long a launcher-spawned daemon stays up after
	// its last client traffic before retiring itself, so an on-demand
	// instance doesn't linger. An attached GUI window holds its control
	// channel open the whole time; an agent working the CLI keeps resetting
	// it; the next call after expiry just respawns it.
	spawnedIdleTimeout = 2 * time.Minute

	// launchTimeout bounds how long a caller waits for a freshly spawned
	// daemon to answer its ping; opening the database and binding the
	// listener take at most a few seconds, so this is generous.
	launchTimeout = 30 * time.Second
	launchPoll    = 100 * time.Millisecond

	// daemonProbeTimeout bounds a ping against the configured address. The
	// daemon answers it without touching the database, so a slow answer means
	// a wedged process, not a busy one.
	daemonProbeTimeout = 2 * time.Second
	// daemonStopTimeout bounds the wait for a daemon asked to retire; its
	// teardown drains HTTP and stops the runtime gateways first.
	daemonStopTimeout = 15 * time.Second

	// detachedEnv marks a daemon spawned by spawnDetached, so runServe knows
	// not to grab the launching terminal's console on Windows.
	detachedEnv = "MASS_SERVE_DETACHED"

	// spawnLogName is the file a detached daemon's stdout/stderr append to,
	// under the logs dir. Without it the child would inherit the launching
	// CLI's streams and write log noise into a script's output long after the
	// verb returned.
	spawnLogName = "daemon-spawn.log"
)

// daemonEndpoint is how a launcher reaches the local daemon.
type daemonEndpoint struct {
	base   string // e.g. "http://127.0.0.1:3455", no trailing slash.
	token  string // operator token for the shutdown request ("" = none).
	client *http.Client
}

// localDaemonEndpoint resolves the daemon endpoint from the local config: the
// configured listen address, with the scheme (and trust anchor) following its
// TLS setting. The launcher manages only the daemon on its own host, so the
// MASS_ADDR env var deliberately plays no part here.
func localDaemonEndpoint(token string) (daemonEndpoint, error) {
	dir, err := config.DefaultDir()
	if err != nil {
		return daemonEndpoint{}, fmt.Errorf("getting config dir: %w", err)
	}
	cfg, _, err := config.Load(dir)
	if err != nil {
		return daemonEndpoint{}, fmt.Errorf("loading config: %w", err)
	}
	scheme, client := "http", http.DefaultClient
	if cfg.TLS.Enabled {
		scheme = "https"
		// The server's own cert PEM doubles as the trust anchor, so a
		// self-signed setup verifies without extra configuration.
		tlsCfg, err := tlsutil.ClientTLSConfig(cfg.TLS.CertFile)
		if err != nil {
			return daemonEndpoint{}, fmt.Errorf("building TLS client config: %w", err)
		}
		client = &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg}}
	}
	return daemonEndpoint{
		base:   config.LocalURL(scheme, cfg.EffectiveListenAddr()),
		token:  token,
		client: client,
	}, nil
}

// daemonState classifies what answers at the configured address.
type daemonState int

const (
	daemonDown    daemonState = iota // nothing is listening.
	daemonAlive                      // a MASS daemon answered the ping.
	daemonForeign                    // something answered, but not a MASS daemon.
)

// probeDaemon asks what is running at ep. A dial failure is daemonDown; an
// answer that isn't a MASS ping is daemonForeign (the accompanying error says
// what was seen — the launcher must not spawn onto an occupied address).
func probeDaemon(ctx context.Context, ep daemonEndpoint) (web.DaemonPing, daemonState, error) {
	ctx, cancel := context.WithTimeout(ctx, daemonProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ep.base+"/internal/daemon/ping", nil)
	if err != nil {
		return web.DaemonPing{}, daemonForeign, fmt.Errorf("building the ping request: %w", err)
	}
	resp, err := ep.client.Do(req)
	if err != nil {
		if isDialError(err) {
			return web.DaemonPing{}, daemonDown, nil
		}
		return web.DaemonPing{}, daemonForeign, fmt.Errorf("pinging %s: %w", ep.base, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return web.DaemonPing{}, daemonForeign,
			fmt.Errorf("%s answers with status %d on the daemon ping — not a MASS daemon, or an incompatible build", ep.base, resp.StatusCode)
	}
	var ping web.DaemonPing
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&ping); err != nil {
		return web.DaemonPing{}, daemonForeign,
			fmt.Errorf("%s answered the daemon ping with something that isn't one: %w", ep.base, err)
	}
	return ping, daemonAlive, nil
}

// isDialError reports whether err is a failure to reach the address at all
// (nothing listening), as opposed to a failure once connected. Checked via
// *net.OpError rather than errno sentinels, which differ per platform.
func isDialError(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

// ensureDaemon makes sure a live MASS daemon of this build answers at ep,
// spawning or replacing one when needed. It is the single entry point both
// clients use — the CLI before a verb, the GUI for the window it attaches:
//
//   - nothing listening: spawn a detached on-demand daemon and wait for it;
//   - alive, same build: attach;
//   - alive, different build, on-demand: ask it to stop, wait for the address
//     to free, spawn a fresh one — an upgraded binary must not drive
//     yesterday's process. If it won't stop (no valid token), attach anyway:
//     a running daemon beats none, and its page is self-consistent;
//   - alive, different build, NOT on-demand: attach with a warning — an
//     operator-managed `mass serve` (a service, a server) is never restarted
//     from under its workers by a mere client;
//   - something else on the address: fail with what was seen.
func ensureDaemon(ctx context.Context, ep daemonEndpoint, logger zerolog.Logger) error {
	ping, state, err := probeDaemon(ctx, ep)
	switch state {
	case daemonAlive:
		if ping.Version == version {
			return nil
		}
		if !ping.OnDemand {
			logger.Warn().Str("daemon", ping.Version).Str("client", version).
				Msg("the running daemon is a different build; it is operator-managed, so attaching to it as is")
			return nil
		}
		logger.Info().Str("daemon", ping.Version).Str("client", version).
			Msg("the running on-demand daemon is a different build; replacing it")
		if err := requestDaemonShutdown(ctx, ep); err != nil {
			logger.Warn().Err(err).Msg("asking the outdated daemon to stop; attaching to it anyway")
			return nil
		}
		if err := waitForDaemonExit(ctx, ep, daemonStopTimeout); err != nil {
			return fmt.Errorf("the outdated daemon did not release %s: %w", ep.base, err)
		}
		return spawnAndWait(ctx, ep)
	case daemonDown:
		return spawnAndWait(ctx, ep)
	default:
		return err
	}
}

// requestDaemonShutdown asks the daemon at ep to retire gracefully. It
// returns once the daemon has accepted; the stop itself runs after the
// response. The endpoint honors operator auth, so the token rides along.
func requestDaemonShutdown(ctx context.Context, ep daemonEndpoint) error {
	ctx, cancel := context.WithTimeout(ctx, daemonProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.base+"/internal/daemon/shutdown", nil)
	if err != nil {
		return fmt.Errorf("building the shutdown request: %w", err)
	}
	if ep.token != "" {
		req.Header.Set("Authorization", "Bearer "+ep.token)
	}
	resp, err := ep.client.Do(req)
	if err != nil {
		return fmt.Errorf("asking the daemon to stop: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusSeeOther {
		return fmt.Errorf("the daemon requires the operator token to stop; set MASS_AUTH_TOKEN")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("asking the daemon to stop: status %d", resp.StatusCode)
	}
	return nil
}

// spawnAndWait starts a detached on-demand daemon and waits for its ping.
func spawnAndWait(ctx context.Context, ep daemonEndpoint) error {
	if err := spawnDetached("serve", "--idle-timeout", spawnedIdleTimeout.String()); err != nil {
		return fmt.Errorf("launching the mass daemon: %w", err)
	}
	if err := waitForDaemon(ctx, ep, launchTimeout); err != nil {
		return fmt.Errorf("mass daemon: %w", err)
	}
	return nil
}

// spawnDetached starts this executable with args as a detached child that
// owns its own lifecycle (we don't wait on or reap it). Its stdout/stderr go
// to the spawn log rather than to ours, so the child can't write into a
// script's output after this process exits.
func spawnDetached(args ...string) error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locating executable: %w", err)
	}
	cmd := exec.Command(exe, args...)
	cmd.Env = append(os.Environ(), detachedEnv+"=1")
	if logFile, err := openSpawnLog(); err != nil {
		return err
	} else if logFile != nil {
		defer func() { _ = logFile.Close() }() // the child holds its own descriptor.
		cmd.Stdout = logFile
		cmd.Stderr = logFile
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("launching mass process: %w", err)
	}
	return cmd.Process.Release()
}

// openSpawnLog opens the detached daemon's stdio log for appending. A nil
// file with a nil error means there is nowhere to write it (no config dir),
// in which case the child gets the OS default and the launch still proceeds.
func openSpawnLog() (*os.File, error) {
	dir, err := config.DefaultDir()
	if err != nil {
		return nil, nil //nolint:nilnil // no config dir: launch anyway, without redirection.
	}
	logsDir := config.LogsDir(dir)
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return nil, nil //nolint:nilnil // same: don't block the launch on the log.
	}
	f, err := os.OpenFile(filepath.Join(logsDir, spawnLogName), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening the daemon spawn log: %w", err)
	}
	return f, nil
}

// waitForDaemon polls ep until a MASS daemon answers the ping. It gives up
// when ctx is cancelled, after timeout, or when something that isn't MASS
// answers (the spawn lost the address race, or the port was taken).
func waitForDaemon(ctx context.Context, ep daemonEndpoint, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	tick := time.NewTicker(launchPoll)
	defer tick.Stop()
	for {
		_, state, err := probeDaemon(ctx, ep)
		switch state {
		case daemonAlive:
			return nil
		case daemonForeign:
			if ctx.Err() == nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting up to %s for the daemon at %s: %w", timeout, ep.base, ctx.Err())
		case <-tick.C:
		}
	}
}

// waitForDaemonExit blocks until nothing answers at ep any more — the daemon
// asked to stop has released the address — so the spawn that follows doesn't
// lose the bind race to the corpse.
func waitForDaemonExit(ctx context.Context, ep daemonEndpoint, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	tick := time.NewTicker(launchPoll)
	defer tick.Stop()
	for {
		if _, state, _ := probeDaemon(ctx, ep); state == daemonDown {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting up to %s for the daemon at %s to retire: %w", timeout, ep.base, ctx.Err())
		case <-tick.C:
		}
	}
}
