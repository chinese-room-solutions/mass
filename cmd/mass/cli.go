package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"github.com/chinese-room-solutions/mass-proto/gen/go/rpcconnect"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Exit codes for the CLI, mirroring common conventions: 0 success, 1 a
// runtime/RPC error, 2 a usage error (bad verb or flags).
const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2
)

// cliActor is the audit attribution the CLI presents on every request.
const cliActor = "cli"

// Default request timeouts. Most calls finish quickly; benchmarking loads a
// model on every device and can take minutes.
const (
	defaultReqTimeout       = 60 * time.Second
	defaultBenchmarkTimeout = 10 * time.Minute
	// longReqTimeout covers calls that fetch bytes over the network, e.g.
	// downloading a runtime package from the registry.
	longReqTimeout = 10 * time.Minute
)

// verbHandler runs one top-level verb, given its arguments (everything after
// the verb itself). It returns a CLI exit code.
type verbHandler func(args []string) int

// verbs maps top-level verb → handler. Grouped verbs (e.g. "models") dispatch
// to their own subcommand table inside the handler.
var verbs = map[string]verbHandler{
	"serve":     cmdServe,
	"status":    cmdStatus,
	"models":    cmdModels,
	"runtimes":  cmdRuntimes,
	"workers":   cmdWorkers,
	"scheduler": cmdScheduler,
	"queue":     cmdQueue,
	"skill":     cmdSkill,
}

// runCLI dispatches a management subcommand. args is os.Args[1:]. A -h/--help
// anywhere in the arguments is answered here rather than by the subcommand's
// flag set: the flag package would print bare flag defaults, which say nothing
// about what the verb does or which arguments it needs.
func runCLI(args []string) int {
	if helpRequested(args) {
		if printHelp(os.Stdout, verbChain(args)) {
			return exitOK
		}
		fmt.Fprintf(os.Stderr, "error: unknown command %q\n\n", strings.Join(verbChain(args), " "))
		usage(os.Stderr)
		return exitUsage
	}
	verb := args[0]
	h, ok := verbs[verb]
	if !ok {
		fmt.Fprintf(os.Stderr, "error: unknown command %q\n\n", verb)
		usage(os.Stderr)
		return exitUsage
	}
	return h(args[1:])
}

// usage prints the top-level help text. The command list comes from the same
// table --help reads, so the two can't drift.
func usage(w io.Writer) {
	_, _ = fmt.Fprintf(w, `mass — MASS management CLI

Usage:
  mass <command> [subcommand] [flags]
  mass <command> --help            synopsis + detail for any command

Commands:
%s
Flags (every command that reaches the server):
  --addr string     MASS base URL (env MASS_ADDR, else local config)
  --token string    bearer token (env MASS_AUTH_TOKEN)
  --json            emit the raw JSON response
  --timeout dur     request timeout (default 60s; benchmark 10m)

A server command aimed at the local config's address starts the daemon on
demand when none is running (detached, retiring after 2m idle). An explicit
--addr or $MASS_ADDR never does. "serve" runs the daemon itself; "skill"
reaches no server and takes only --json.
`, commandList())
}

// commonFlags holds the flags shared by every subcommand, plus the parse-time
// decision whether newClient may boot the local daemon on demand.
type commonFlags struct {
	addr    string
	token   string
	json    bool
	timeout time.Duration

	spawnLocal bool // the verb targets the local config's address.
	ensured    bool // the daemon check already ran (verbs may build several clients).
}

// registerCommon adds the shared flags to fs and returns a pointer whose
// fields are populated after fs.Parse. defaultTimeout lets the benchmark
// subcommand raise the default without changing the flag semantics.
func registerCommon(fs *flag.FlagSet, defaultTimeout time.Duration) *commonFlags {
	c := &commonFlags{}
	fs.StringVar(&c.addr, "addr", defaultAddr(), "MASS base URL")
	fs.StringVar(&c.token, "token", os.Getenv("MASS_AUTH_TOKEN"), "bearer token")
	fs.BoolVar(&c.json, "json", false, "emit the raw JSON response")
	fs.DurationVar(&c.timeout, "timeout", defaultTimeout, "request timeout")
	return c
}

// defaultAddr resolves the default base URL: MASS_ADDR env wins; otherwise the
// local config's listen address with a scheme matching its TLS setting. A
// config load failure falls back to the well-known loopback address.
func defaultAddr() string {
	if env := os.Getenv("MASS_ADDR"); env != "" {
		return env
	}
	dir, err := config.DefaultDir()
	if err != nil {
		return "http://127.0.0.1:3455"
	}
	cfg, _, err := config.Load(dir)
	if err != nil {
		return "http://127.0.0.1:3455"
	}
	scheme := "http"
	if cfg.TLS.Enabled {
		scheme = "https"
	}
	return config.LocalURL(scheme, cfg.EffectiveListenAddr())
}

// authTransport adds the bearer token (when set) and the audit actor header to
// every request.
type authTransport struct {
	token string
	next  http.RoundTripper
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.token != "" {
		req.Header.Set("Authorization", "Bearer "+t.token)
	}
	req.Header.Set("X-Mass-Actor", cliActor)
	return t.next.RoundTrip(req)
}

// newClient builds a MassClient targeting c.addr with auth wired in. Called
// after the verb validated its arguments, so a usage error never boots a
// daemon; the on-demand launch happens here, once per verb.
func newClient(c *commonFlags) rpcconnect.MassClient {
	ensureLocalDaemonForCLI(c)
	httpClient := &http.Client{
		Transport: &authTransport{token: c.token, next: http.DefaultTransport},
	}
	return rpcconnect.NewMassClient(httpClient, c.addr)
}

// parse parses fs against args and maps a flag error to exitUsage. On success
// it returns a context bounded by the timeout plus its cancel func. The bool
// reports whether parsing succeeded (false → caller returns the returned
// code). It also records — while fs still knows which flags were set —
// whether newClient may boot the local daemon on demand.
func parse(fs *flag.FlagSet, c *commonFlags, args []string) (context.Context, context.CancelFunc, bool, int) {
	if err := fs.Parse(args); err != nil {
		return nil, nil, false, exitUsage
	}
	c.spawnLocal = shouldEnsureLocalDaemon(fs)
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	return ctx, cancel, true, exitOK
}

// ensureLocalDaemonForCLI boots the local daemon on demand before a server
// verb's first request, the way the GUI does for its window — an agent's
// `mass status` works on a machine where nothing is running yet. Only when
// the address comes from the local config: an explicit --addr or $MASS_ADDR
// names a specific server, which is never spawned into existence.
// Best-effort — on failure the verb proceeds and reports its own connection
// error.
func ensureLocalDaemonForCLI(c *commonFlags) {
	if !c.spawnLocal || c.ensured {
		return
	}
	c.ensured = true
	ep, err := localDaemonEndpoint(c.token)
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), launchTimeout+daemonStopTimeout)
		defer cancel()
		// Silent on success: launch decisions are not the verb's output. The
		// CLI has no logger of its own, so skew advisories are silent too.
		err = ensureDaemon(ctx, ep, zerolog.Nop())
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: starting the local daemon: %s\n", err)
	}
}

// shouldEnsureLocalDaemon reports whether a parsed verb targets the local
// config's address (no addr flag on the verb = a local verb; an explicit
// --addr or $MASS_ADDR = a named server, never spawned into existence).
func shouldEnsureLocalDaemon(fs *flag.FlagSet) bool {
	if fs.Lookup("addr") == nil || os.Getenv("MASS_ADDR") != "" {
		return false
	}
	explicit := false
	fs.Visit(func(f *flag.Flag) { explicit = explicit || f.Name == "addr" })
	return !explicit
}

// fail prints an error to stderr in the requested format and returns exitError.
// In JSON mode it emits {"error":...,"code":...}; the code is the Connect code
// for a *connect.Error (connect.CodeOf yields "unknown" for anything else).
func fail(c *commonFlags, err error) int {
	if c.json {
		fmt.Fprintf(os.Stderr, "{\"error\":%q,\"code\":%q}\n", err.Error(), connect.CodeOf(err).String())
	} else {
		fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
	}
	return exitError
}

// printJSON marshals a proto message to indented JSON on stdout.
func printJSON(msg proto.Message) int {
	opts := protojson.MarshalOptions{Multiline: true, Indent: "  ", UseProtoNames: true}
	b, err := opts.Marshal(msg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: marshaling response: %s\n", err)
		return exitError
	}
	fmt.Println(string(b))
	return exitOK
}

// printAnyJSON marshals a plain Go value (not a proto message) as indented JSON.
// Used by commands whose --json shape is assembled locally rather than mirroring
// an RPC response, e.g. `workers join-command --json`.
func printAnyJSON(v any) int {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: marshaling response: %s\n", err)
		return exitError
	}
	fmt.Println(string(b))
	return exitOK
}

// tableWriter buffers tab-aligned rows for stdout. It records the first write
// error so callers append rows without handling an error on each line; flush
// surfaces it. Errors on stdout are effectively unreachable but not dropped.
type tableWriter struct {
	tw  *tabwriter.Writer
	err error
}

// newTabWriter returns a tableWriter targeting stdout.
func newTabWriter() *tableWriter {
	return &tableWriter{tw: tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)}
}

// rowf writes one tab-separated line, remembering the first error.
func (t *tableWriter) rowf(format string, a ...any) {
	if t.err != nil {
		return
	}
	_, t.err = fmt.Fprintf(t.tw, format, a...)
}

// flush emits the buffered table and returns a CLI exit code.
func (t *tableWriter) flush() int {
	if t.err == nil {
		t.err = t.tw.Flush()
	}
	if t.err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", t.err)
		return exitError
	}
	return exitOK
}

// failUsage prints a usage error to stderr and returns exitUsage.
func failUsage(msg string) int {
	fmt.Fprintf(os.Stderr, "usage: %s\n", msg)
	return exitUsage
}

// warnStaleRegistry notes on stderr that a listing came from a cached index
// the client couldn't revalidate (registry unreachable).
func warnStaleRegistry() {
	fmt.Fprintln(os.Stderr, "warning: registry unreachable; showing a cached index (results may be stale)")
}

// confirm prints a one-line mutation confirmation to stdout.
func confirm(format string, a ...any) int {
	fmt.Printf(format+"\n", a...)
	return exitOK
}

// confirmPaths reports a byte-moving mutation and the store-relative paths it
// touched.
func confirmPaths(verb string, paths []string) int {
	if len(paths) == 0 {
		return confirm("%s (no files)", verb)
	}
	fmt.Printf("%s %d file(s):\n", verb, len(paths))
	for _, p := range paths {
		fmt.Printf("  %s\n", p)
	}
	return exitOK
}

// peelName splits a leading positional NAME argument (e.g. `start llama-cpp
// --json`) from the remaining flag args. Go's flag package stops parsing at the
// first non-flag token, so verbs shaped `<verb> NAME [flags]` must extract the
// name before Parse. Returns ("", args) when args[0] looks like a flag.
func peelName(args []string) (string, []string) {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return "", args
	}
	return args[0], args[1:]
}

// subUsage prints a group's subcommand list to stderr and returns exitUsage.
func subUsage(group string, subs []string) int {
	fmt.Fprintf(os.Stderr, "usage: mass %s <%s> [flags]\n", group, joinPipe(subs))
	return exitUsage
}

func joinPipe(ss []string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += "|"
		}
		out += s
	}
	return out
}
