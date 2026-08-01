package main

import (
	"flag"
	"strings"

	"connectrpc.com/connect"
	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
)

// cmdRuntimes dispatches the runtime subcommands.
func cmdRuntimes(args []string) int {
	subs := []string{"list", "search", "install", "uninstall", "start", "stop", "auto-start"}
	if len(args) == 0 {
		return subUsage("runtimes", subs)
	}
	switch args[0] {
	case "list":
		return runtimesList(args[1:])
	case "search":
		return runtimesSearch(args[1:])
	case "install":
		return runtimesInstall(args[1:])
	case "uninstall":
		return runtimesUninstall(args[1:])
	case "start":
		return runtimesStart(args[1:])
	case "stop":
		return runtimesStop(args[1:])
	case "auto-start":
		return runtimesAutoStart(args[1:])
	default:
		return subUsage("runtimes", subs)
	}
}

func runtimesList(args []string) int {
	fs := flag.NewFlagSet("runtimes list", flag.ContinueOnError)
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, args)
	if !ok {
		return code
	}
	defer cancel()

	resp, err := newClient(c).ListRuntimes(ctx, connect.NewRequest(&rpc.ListRuntimesRequest{}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	w := newTabWriter()
	w.rowf("NAME\tVERSION\tRUNNING\tAUTO-START\tDESCRIPTION\n")
	for _, r := range resp.Msg.Runtimes {
		w.rowf("%s\t%s\t%t\t%t\t%s\n", r.RuntimeName, r.Version, r.Running, r.AutoStart, r.Description)
	}
	return w.flush()
}

func runtimesSearch(args []string) int {
	query, rest := peelName(args)
	fs := flag.NewFlagSet("runtimes search", flag.ContinueOnError)
	kind := fs.String("kind", "", `filter by kind ("runtime" | "worker")`)
	runtimeName := fs.String("runtime", "", "filter by runtime name")
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, rest)
	if !ok {
		return code
	}
	defer cancel()

	resp, err := newClient(c).SearchPackages(ctx, connect.NewRequest(&rpc.SearchPackagesRequest{
		Kind:        *kind,
		Query:       query,
		RuntimeName: *runtimeName,
	}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	w := newTabWriter()
	w.rowf("NAME\tKIND\tRUNTIME\tLATEST\tINSTALLABLE\tDESCRIPTION\n")
	for _, p := range resp.Msg.Packages {
		latest, installable := latestInstallable(p.Versions)
		w.rowf("%s\t%s\t%s\t%s\t%t\t%s\n", p.Name, p.Kind, p.RuntimeName, latest, installable, p.Description)
	}
	code = w.flush()
	if resp.Msg.Stale {
		warnStaleRegistry()
	}
	return code
}

// latestInstallable returns the first version's number and whether any version
// has an artifact for the server platform. Versions are index-ordered
// (newest-first by convention), so the first is the latest.
func latestInstallable(versions []*rpc.PackageVersion) (latest string, installable bool) {
	for i, v := range versions {
		if i == 0 {
			latest = v.Version
		}
		if v.HasArtifact {
			installable = true
		}
	}
	return latest, installable
}

func runtimesInstall(args []string) int {
	name, rest := peelName(args)
	fs := flag.NewFlagSet("runtimes install", flag.ContinueOnError)
	path := fs.String("path", "", "absolute path to a .mass package (installs from disk instead of the registry)")
	c := registerCommon(fs, longReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, rest)
	if !ok {
		return code
	}
	defer cancel()

	// Install from a local .mass file when --path is given; otherwise install
	// the named package from the registry (name[@version]).
	if *path != "" {
		if name != "" {
			return failUsage("runtimes install takes either NAME or --path, not both")
		}
		resp, err := newClient(c).InstallRuntime(ctx, connect.NewRequest(&rpc.InstallRuntimeRequest{Path: *path}))
		if err != nil {
			return fail(c, err)
		}
		if c.json {
			return printJSON(resp.Msg)
		}
		label := *path
		if resp.Msg.Runtime != nil {
			label = resp.Msg.Runtime.RuntimeName
		}
		return confirm("installed %s", label)
	}

	if name == "" {
		return failUsage("runtimes install requires NAME[@version] (or --path for a local package)")
	}
	pkg, version, _ := strings.Cut(name, "@")
	resp, err := newClient(c).InstallRuntimeFromRegistry(ctx, connect.NewRequest(&rpc.InstallRuntimeFromRegistryRequest{
		Name:    pkg,
		Version: version,
	}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	label := pkg
	if resp.Msg.Runtime != nil {
		label = resp.Msg.Runtime.RuntimeName + " " + resp.Msg.Runtime.Version
	}
	return confirm("installed %s", label)
}

func runtimesUninstall(args []string) int {
	name, rest := peelName(args)
	fs := flag.NewFlagSet("runtimes uninstall", flag.ContinueOnError)
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, rest)
	if !ok {
		return code
	}
	defer cancel()

	if name == "" {
		return failUsage("runtimes uninstall requires a runtime name")
	}
	resp, err := newClient(c).UninstallRuntime(ctx, connect.NewRequest(&rpc.UninstallRuntimeRequest{RuntimeName: name}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	return confirm("uninstalled %s", name)
}

func runtimesStart(args []string) int {
	name, rest := peelName(args)
	fs := flag.NewFlagSet("runtimes start", flag.ContinueOnError)
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, rest)
	if !ok {
		return code
	}
	defer cancel()

	if name == "" {
		return failUsage("runtimes start requires a runtime name")
	}
	resp, err := newClient(c).StartRuntime(ctx, connect.NewRequest(&rpc.StartRuntimeRequest{RuntimeName: name}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	return confirm("started %s", name)
}

func runtimesStop(args []string) int {
	name, rest := peelName(args)
	fs := flag.NewFlagSet("runtimes stop", flag.ContinueOnError)
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, rest)
	if !ok {
		return code
	}
	defer cancel()

	if name == "" {
		return failUsage("runtimes stop requires a runtime name")
	}
	resp, err := newClient(c).StopRuntime(ctx, connect.NewRequest(&rpc.StopRuntimeRequest{RuntimeName: name}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	return confirm("stopped %s", name)
}

func runtimesAutoStart(args []string) int {
	name, rest := peelName(args)
	fs := flag.NewFlagSet("runtimes auto-start", flag.ContinueOnError)
	enabled := fs.Bool("enabled", false, "explicit auto-start value")
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, rest)
	if !ok {
		return code
	}
	defer cancel()

	if name == "" {
		return failUsage("runtimes auto-start requires a runtime name")
	}
	resp, err := newClient(c).SetRuntimeAutoStart(ctx, connect.NewRequest(&rpc.SetRuntimeAutoStartRequest{
		RuntimeName: name,
		Enabled:     *enabled,
	}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	return confirm("auto-start %s=%t", name, *enabled)
}
