package main

import (
	"flag"

	"connectrpc.com/connect"
	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
)

// cmdStatus → GetStatus.
func cmdStatus(args []string) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, args)
	if !ok {
		return code
	}
	defer cancel()

	resp, err := newClient(c).GetStatus(ctx, connect.NewRequest(&rpc.GetStatusRequest{}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	s := resp.Msg
	w := newTabWriter()
	w.rowf("version\t%s\n", s.Version)
	w.rowf("listen\t%s\n", s.ListenAddr)
	w.rowf("runtimes\t%d installed, %d running\n", s.RuntimesInstalled, s.RuntimesRunning)
	w.rowf("workers\t%d total, %d online\n", s.WorkersTotal, s.WorkersOnline)
	w.rowf("jobs\t%d queued, %d running\n", s.QueuedJobs, s.RunningJobs)
	return w.flush()
}

// cmdModels dispatches the model subcommands.
func cmdModels(args []string) int {
	subs := []string{"list", "import-local", "import-remote", "delete"}
	if len(args) == 0 {
		return subUsage("models", subs)
	}
	switch args[0] {
	case "list":
		return modelsList(args[1:])
	case "import-local":
		return modelsImportLocal(args[1:])
	case "import-remote":
		return modelsImportRemote(args[1:])
	case "delete":
		return modelsDelete(args[1:])
	default:
		return subUsage("models", subs)
	}
}

func modelsList(args []string) int {
	fs := flag.NewFlagSet("models list", flag.ContinueOnError)
	runtime := fs.String("runtime", "", "filter by runtime name")
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, args)
	if !ok {
		return code
	}
	defer cancel()

	resp, err := newClient(c).ListModels(ctx, connect.NewRequest(&rpc.ListModelsRequest{RuntimeName: *runtime}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	w := newTabWriter()
	w.rowf("RUNTIME\tGROUP\tID\tFILE\n")
	for _, g := range resp.Msg.Groups {
		grp := g.Group
		if grp == nil {
			continue
		}
		if len(grp.Models) == 0 {
			w.rowf("%s\t%s\t%s\t\n", g.RuntimeName, grp.DisplayName, grp.Id)
			continue
		}
		for _, m := range grp.Models {
			w.rowf("%s\t%s\t%s\t%s\n", g.RuntimeName, grp.DisplayName, m.Id, m.DisplayName)
		}
	}
	return w.flush()
}

func modelsImportLocal(args []string) int {
	fs := flag.NewFlagSet("models import-local", flag.ContinueOnError)
	runtime := fs.String("runtime", "", "runtime that owns the format (required)")
	path := fs.String("path", "", "absolute path on the MASS host (required)")
	name := fs.String("name", "", "group identity (defaults to the runtime CLI actor)")
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, args)
	if !ok {
		return code
	}
	defer cancel()

	if *runtime == "" || *path == "" {
		return failUsage("models import-local requires --runtime and --path")
	}
	op := *name
	if op == "" {
		op = cliActor
	}
	resp, err := newClient(c).ImportLocalModel(ctx, connect.NewRequest(&rpc.ImportLocalModelRequest{
		RuntimeName:  *runtime,
		Path:         *path,
		OperatorName: op,
	}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	return confirmPaths("imported", resp.Msg.RelPaths)
}

func modelsImportRemote(args []string) int {
	fs := flag.NewFlagSet("models import-remote", flag.ContinueOnError)
	runtime := fs.String("runtime", "", "runtime that owns the format (required)")
	repo := fs.String("repo", "", "remote repo, e.g. owner/model (required)")
	file := fs.String("file", "", "file within the repo (required)")
	name := fs.String("name", "", "group identity (defaults to the runtime CLI actor)")
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, args)
	if !ok {
		return code
	}
	defer cancel()

	if *runtime == "" || *repo == "" || *file == "" {
		return failUsage("models import-remote requires --runtime, --repo and --file")
	}
	op := *name
	if op == "" {
		op = cliActor
	}
	resp, err := newClient(c).ImportRemoteModel(ctx, connect.NewRequest(&rpc.ImportRemoteModelRequest{
		RuntimeName:  *runtime,
		RepoId:       *repo,
		Filename:     *file,
		OperatorName: op,
	}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	return confirmPaths("imported", resp.Msg.RelPaths)
}

func modelsDelete(args []string) int {
	fs := flag.NewFlagSet("models delete", flag.ContinueOnError)
	runtime := fs.String("runtime", "", "runtime that owns the model (required)")
	id := fs.String("id", "", "group/model id from `models list` (required)")
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, args)
	if !ok {
		return code
	}
	defer cancel()

	if *runtime == "" || *id == "" {
		return failUsage("models delete requires --runtime and --id")
	}
	resp, err := newClient(c).DeleteModel(ctx, connect.NewRequest(&rpc.DeleteModelRequest{
		RuntimeName: *runtime,
		Id:          *id,
	}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	return confirmPaths("deleted", resp.Msg.RelPaths)
}
