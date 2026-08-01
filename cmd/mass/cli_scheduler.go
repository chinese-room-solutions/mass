package main

import (
	"flag"

	"connectrpc.com/connect"
	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
)

// cmdScheduler dispatches the scheduler subcommands.
func cmdScheduler(args []string) int {
	subs := []string{"list", "evict"}
	if len(args) == 0 {
		return subUsage("scheduler", subs)
	}
	switch args[0] {
	case "list":
		return schedulerList(args[1:])
	case "evict":
		return schedulerEvict(args[1:])
	default:
		return subUsage("scheduler", subs)
	}
}

func schedulerList(args []string) int {
	fs := flag.NewFlagSet("scheduler list", flag.ContinueOnError)
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, args)
	if !ok {
		return code
	}
	defer cancel()

	resp, err := newClient(c).ListInstances(ctx, connect.NewRequest(&rpc.ListInstancesRequest{}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	w := newTabWriter()
	w.rowf("WORKER\tMODEL\tRUNTIME\tSTATUS\tMODE\tPOOL\tACTIVE\n")
	for _, in := range resp.Msg.Instances {
		w.rowf("%s\t%s\t%s\t%s\t%s\t%d\t%d\n",
			in.WorkerName, in.ModelId, in.RuntimeName, in.Status, in.Mode, in.PoolSize, in.Active)
	}
	return w.flush()
}

func schedulerEvict(args []string) int {
	fs := flag.NewFlagSet("scheduler evict", flag.ContinueOnError)
	worker := fs.String("worker", "", "worker ID (required)")
	model := fs.String("model", "", "model ID (required)")
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, args)
	if !ok {
		return code
	}
	defer cancel()

	if *worker == "" || *model == "" {
		return failUsage("scheduler evict requires --worker and --model")
	}
	resp, err := newClient(c).EvictInstance(ctx, connect.NewRequest(&rpc.EvictInstanceRequest{
		WorkerId: *worker,
		ModelId:  *model,
	}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	return confirm("evicted %s from worker %s", *model, *worker)
}
