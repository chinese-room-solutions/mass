package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
)

// cmdWorkers dispatches the worker subcommands.
func cmdWorkers(args []string) int {
	subs := []string{"list", "enable", "disable", "device", "benchmark", "join-command", "install-local"}
	if len(args) == 0 {
		return subUsage("workers", subs)
	}
	switch args[0] {
	case "list":
		return workersList(args[1:])
	case "enable":
		return workersSetEnabled(args[1:], true)
	case "disable":
		return workersSetEnabled(args[1:], false)
	case "device":
		return workersDevice(args[1:])
	case "benchmark":
		return workersBenchmark(args[1:])
	case "join-command":
		return workersJoinCommand(args[1:])
	case "install-local":
		return workersInstallLocal(args[1:])
	default:
		return subUsage("workers", subs)
	}
}

// workersJoinCommand mints a real join token (CreateJoinToken, operator-
// authenticated with the CLI's own bearer) and prints, per OS, a pair of
// commands: one to download the worker installer from /setup/worker-bin, and
// one to run it. Running it launches the installer's interactive wizard on the
// target machine — the operator picks scope/dirs/options; the installer's
// --mass-url and, when present, --token are passed only as prefilled defaults.
// The address embedded in the commands defaults to the API address the CLI
// talks to; --mass-url overrides it (the LAN/DNS/proxy address workers reach).
// --ttl sets the token lifetime (0/unset = server default). --json emits the
// structured pairs for automation. The runtime name selects which worker
// package the installer endpoint serves: when --runtime is omitted and exactly
// one runtime is installed it is used; when several are installed it's required.
func workersJoinCommand(args []string) int {
	fs := flag.NewFlagSet("workers join-command", flag.ContinueOnError)
	runtimeName := fs.String("runtime", "", "runtime name the worker joins (required when >1 runtime is installed)")
	workerPkg := fs.String("worker", "", "worker package to install (required when the runtime has >1 worker package)")
	backend := fs.String("backend", "", "worker backend to install (e.g. cuda, vulkan; required when >1 backend is available)")
	massURL := fs.String("mass-url", "", "address the worker machines use to reach this MASS, embedded in the commands (default: the API address)")
	ttl := fs.Duration("ttl", 0, "join-token lifetime (e.g. 30m, 24h); 0 uses the server default (1h)")
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, args)
	if !ok {
		return code
	}
	defer cancel()

	name := *runtimeName
	if name == "" {
		resp, err := newClient(c).ListRuntimes(ctx, connect.NewRequest(&rpc.ListRuntimesRequest{}))
		if err != nil {
			return fail(c, err)
		}
		switch len(resp.Msg.Runtimes) {
		case 0:
			return failUsage("no runtimes installed; install one first (mass runtimes install ...)")
		case 1:
			name = resp.Msg.Runtimes[0].RuntimeName
		default:
			names := make([]string, 0, len(resp.Msg.Runtimes))
			for _, rt := range resp.Msg.Runtimes {
				names = append(names, rt.RuntimeName)
			}
			return failUsage("multiple runtimes installed; pass --runtime <" + strings.Join(names, "|") + ">")
		}
	}

	tokenResp, err := newClient(c).CreateJoinToken(ctx, connect.NewRequest(&rpc.CreateJoinTokenRequest{
		TtlSeconds: int64(ttl.Seconds()),
	}))
	if err != nil {
		return fail(c, err)
	}
	token := tokenResp.Msg.GetToken()
	expiresAt := tokenResp.Msg.GetExpiresAt()

	// The embedded base defaults to the API address the CLI talks to, but
	// --mass-url overrides it: a host-local operator can substitute the
	// LAN/DNS/reverse-proxy address the worker machines actually reach.
	base := c.addr
	if *massURL != "" {
		base = *massURL
	}
	base = strings.TrimRight(base, "/")
	// Suffix appended to the worker-bin query for the chosen worker/backend
	// pins; empty when the operator picked neither.
	pins := ""
	if *workerPkg != "" {
		pins += "&worker=" + *workerPkg
	}
	if *backend != "" {
		pins += "&backend=" + *backend
	}
	// `--token` is only a prefill; omitted entirely when no token was minted.
	tokenArg := ""
	if token != "" {
		tokenArg = " --token " + token
	}

	linuxDownload := fmt.Sprintf(
		"curl -fsSL \"%s/setup/worker-bin/%s?os=$(uname -s)&arch=$(uname -m)%s\" -o mass-worker-setup && chmod +x mass-worker-setup",
		base, name, pins)
	linuxRun := fmt.Sprintf("./mass-worker-setup --mass-url %s%s", base, tokenArg)
	windowsDownload := fmt.Sprintf(
		"irm '%s/setup/worker-bin/%s?os=windows&arch=AMD64%s' -OutFile mass-worker-setup.exe",
		base, name, pins)
	windowsRun := fmt.Sprintf(".\\mass-worker-setup.exe --mass-url %s%s", base, tokenArg)

	if c.json {
		out := map[string]any{
			"token":      token,
			"expires_at": expiresAt,
			"mass_url":   base,
			"linux":      map[string]string{"download": linuxDownload, "run": linuxRun},
			"windows":    map[string]string{"download": windowsDownload, "run": windowsRun},
		}
		if *workerPkg != "" {
			out["worker"] = *workerPkg
		}
		if *backend != "" {
			out["backend"] = *backend
		}
		return printAnyJSON(out)
	}

	fmt.Printf("# Linux/macOS — 1. Download the installer:\n%s\n\n", linuxDownload)
	fmt.Printf("# Linux/macOS — 2. Run it (interactive wizard):\n%s\n\n", linuxRun)
	fmt.Printf("# Windows (PowerShell) — 1. Download the installer:\n%s\n\n", windowsDownload)
	fmt.Printf("# Windows (PowerShell) — 2. Run it (interactive wizard):\n%s\n\n", windowsRun)
	fmt.Printf("# Token valid until %s\n", time.Unix(expiresAt, 0).Format(time.RFC3339))
	return exitOK
}

// workersInstallLocal installs a worker on the MASS host itself: MASS resolves
// the worker installer for its own platform, downloads it through the artifact
// cache, and runs it locally with --non-interactive against its own listen
// address. The server mints a short-TTL join token for the installer to enroll
// with (the caller's own bearer is not reused). --runtime is inferred
// server-side when exactly one runtime is installed.
func workersInstallLocal(args []string) int {
	fs := flag.NewFlagSet("workers install-local", flag.ContinueOnError)
	runtimeName := fs.String("runtime", "", "runtime name the worker joins (inferred when one runtime is installed)")
	scope := fs.String("scope", "user", "OS service scope: user|system (system needs privileges MASS usually lacks)")
	name := fs.String("name", "", "worker display name (default: the host's name)")
	c := registerCommon(fs, defaultBenchmarkTimeout)
	ctx, cancel, ok, code := parse(fs, c, args)
	if !ok {
		return code
	}
	defer cancel()

	resp, err := newClient(c).InstallLocalWorker(ctx, connect.NewRequest(&rpc.InstallLocalWorkerRequest{
		RuntimeName: *runtimeName,
		Scope:       *scope,
		Name:        *name,
	}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	if out := strings.TrimSpace(resp.Msg.Output); out != "" {
		fmt.Fprintln(os.Stderr, out)
	}
	return confirm("installed local worker %s@%s", resp.Msg.WorkerPackage, resp.Msg.WorkerVersion)
}

func workersList(args []string) int {
	fs := flag.NewFlagSet("workers list", flag.ContinueOnError)
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, args)
	if !ok {
		return code
	}
	defer cancel()

	resp, err := newClient(c).ListWorkers(ctx, connect.NewRequest(&rpc.ListWorkersRequest{}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	w := newTabWriter()
	w.rowf("ID\tNAME\tRUNTIME\tONLINE\tENABLED\tACTIVE\tDEVICES\n")
	for _, wk := range resp.Msg.Workers {
		w.rowf("%s\t%s\t%s\t%t\t%t\t%d\t%d\n",
			wk.Id, wk.Name, wk.RuntimeName, wk.Online, wk.Enabled, wk.ActiveJobs, len(wk.Devices))
	}
	return w.flush()
}

func workersSetEnabled(args []string, enabled bool) int {
	verb := "enable"
	if !enabled {
		verb = "disable"
	}
	id, rest := peelName(args)
	fs := flag.NewFlagSet("workers "+verb, flag.ContinueOnError)
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, rest)
	if !ok {
		return code
	}
	defer cancel()

	if id == "" {
		return failUsage("workers " + verb + " requires a worker ID")
	}
	resp, err := newClient(c).SetWorkerEnabled(ctx, connect.NewRequest(&rpc.SetWorkerEnabledRequest{
		WorkerId: id,
		Enabled:  enabled,
	}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	return confirm("%sd worker %s", verb, id)
}

// workersDevice handles `workers device enable|disable WORKER_ID DEVICE_ID`.
func workersDevice(args []string) int {
	if len(args) == 0 {
		return failUsage("workers device enable|disable WORKER_ID DEVICE_ID")
	}
	var enabled bool
	switch args[0] {
	case "enable":
		enabled = true
	case "disable":
		enabled = false
	default:
		return failUsage("workers device enable|disable WORKER_ID DEVICE_ID")
	}

	rest := args[1:]
	workerID, rest := peelName(rest)
	deviceID, rest := peelName(rest)
	fs := flag.NewFlagSet("workers device", flag.ContinueOnError)
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, rest)
	if !ok {
		return code
	}
	defer cancel()

	if workerID == "" || deviceID == "" {
		return failUsage("workers device " + args[0] + " requires WORKER_ID and DEVICE_ID")
	}
	resp, err := newClient(c).SetWorkerDeviceEnabled(ctx, connect.NewRequest(&rpc.SetWorkerDeviceEnabledRequest{
		WorkerId: workerID,
		DeviceId: deviceID,
		Enabled:  enabled,
	}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	return confirm("%sd device %s on worker %s", args[0], deviceID, workerID)
}

func workersBenchmark(args []string) int {
	fs := flag.NewFlagSet("workers benchmark", flag.ContinueOnError)
	workers := fs.String("workers", "", "comma-separated worker IDs (empty = all)")
	devices := fs.String("devices", "", "comma-separated device IDs (empty = all)")
	c := registerCommon(fs, defaultBenchmarkTimeout)
	ctx, cancel, ok, code := parse(fs, c, args)
	if !ok {
		return code
	}
	defer cancel()

	resp, err := newClient(c).BenchmarkWorkers(ctx, connect.NewRequest(&rpc.BenchmarkWorkersRequest{
		WorkerIds: splitCSV(*workers),
		DeviceIds: splitCSV(*devices),
	}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	w := newTabWriter()
	w.rowf("WORKER\tDEVICE\tNAME\tMEM_GB\tLOAD_GB\tERROR\n")
	for _, r := range resp.Msg.Results {
		w.rowf("%s\t%s\t%s\t%.1f\t%.1f\t%s\n",
			r.WorkerId, r.DeviceId, r.DeviceName, r.MemoryGbs, r.LoadGbs, r.Error)
	}
	return w.flush()
}

// splitCSV splits a comma-separated flag value into a slice, trimming spaces
// and dropping empties. An empty string yields nil.
func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
