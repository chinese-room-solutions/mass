package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/chinese-room-solutions/mass/internal/web"
)

// The update surface is plain JSON over HTTP rather than a mass.v1 RPC: it is
// the daemon talking about its own process (which build it is, whether it may
// restart itself), not a management operation on the orchestrator.

// cmdUpdate dispatches `mass update [--apply] [--force]`: report whether a
// newer release is available — and what applying it would cost the fleet — and
// with --apply install it. The check itself ran at daemon startup, so this
// reads an answer rather than asking for one.
func cmdUpdate(args []string) int {
	fs := flag.NewFlagSet("update", flag.ContinueOnError)
	apply := fs.Bool("apply", false, "install the available update and restart MASS")
	force := fs.Bool("force", false, "with --apply: update even when connected workers would be stranded")
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, args)
	if !ok {
		return code
	}
	defer cancel()
	if fs.NArg() != 0 {
		return failUsage("mass update [--apply] [--force]")
	}
	if *apply {
		return updateApply(ctx, c, *force)
	}
	return updateCheck(ctx, c)
}

// updateCheck prints the stored answer: the running build, any newer tag, and
// the fleet the candidate would strand.
func updateCheck(ctx context.Context, c *commonFlags) int {
	var st web.UpdateCheckResponse
	if err := updateRequest(ctx, c, http.MethodGet, "/api/update/check", nil, &st); err != nil {
		return fail(c, err)
	}
	if c.json {
		return printAnyJSON(st)
	}
	if st.Available == "" {
		fmt.Printf("mass %s — up to date\n", st.Version)
		return exitOK
	}
	fmt.Printf("%s available (run mass update --apply)\n", st.Available)
	if st.Incompatible > 0 {
		fmt.Printf("warning: %s would be stranded by %s", workersPhrase(st.Incompatible), st.Available)
		if len(st.Names) > 0 {
			fmt.Printf(" (%s)", strings.Join(st.Names, ", "))
		}
		fmt.Println("; upgrade them first, or apply with --force")
	}
	return exitOK
}

// workersPhrase pluralizes the stranded-worker count.
func workersPhrase(n int) string {
	if n == 1 {
		return "1 connected worker"
	}
	return fmt.Sprintf("%d connected workers", n)
}

// updateApply installs the available release. The daemon answers before it
// retires, so a 0 here means the installer is running, not that it has finished
// — the app comes back on its own a moment later.
func updateApply(ctx context.Context, c *commonFlags, force bool) int {
	body, err := json.Marshal(map[string]bool{"force": force})
	if err != nil {
		return fail(c, err)
	}
	var res struct {
		Status  string `json:"status"`
		Version string `json:"version"`
	}
	if err := updateRequest(ctx, c, http.MethodPost, "/api/update/apply", body, &res); err != nil {
		return fail(c, err)
	}
	if c.json {
		return printAnyJSON(res)
	}
	fmt.Printf("installing mass %s — the app restarts itself when it's done\n", res.Version)
	return exitOK
}

// updateRequest calls one of the daemon's update endpoints and decodes its JSON
// answer into out. A non-2xx status becomes an error carrying the server's own
// sentence — the 409 refusals (nothing to install, not installer-placed, needs
// admin rights, operator-managed, an incompatible fleet) all say what to do.
func updateRequest(ctx context.Context, c *commonFlags, method, path string, body []byte, out any) error {
	ensureLocalDaemonForCLI(c)
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(c.addr, "/")+path, rdr)
	if err != nil {
		return fmt.Errorf("building the request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Transport: &authTransport{token: c.token, next: http.DefaultTransport}}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("reaching the MASS daemon: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return fmt.Errorf("reading the response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		msg := strings.TrimSpace(string(payload))
		if msg == "" {
			msg = resp.Status
		}
		return fmt.Errorf("%s", msg)
	}
	if err := json.Unmarshal(payload, out); err != nil {
		return fmt.Errorf("decoding the response: %w", err)
	}
	return nil
}
