package main

import (
	"flag"

	"connectrpc.com/connect"
	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
)

// cmdQueue dispatches the queue subcommands.
func cmdQueue(args []string) int {
	subs := []string{"list", "cancel", "cancel-running", "evict"}
	if len(args) == 0 {
		return subUsage("queue", subs)
	}
	switch args[0] {
	case "list":
		return queueList(args[1:])
	case "cancel":
		return queueCancel(args[1:])
	case "cancel-running":
		return queueCancelRunning(args[1:])
	case "evict":
		return queueEvict(args[1:])
	default:
		return subUsage("queue", subs)
	}
}

func queueList(args []string) int {
	fs := flag.NewFlagSet("queue list", flag.ContinueOnError)
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, args)
	if !ok {
		return code
	}
	defer cancel()

	resp, err := newClient(c).GetQueue(ctx, connect.NewRequest(&rpc.GetQueueRequest{}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	w := newTabWriter()
	w.rowf("QUEUE\tMSG_ID\tREQUEST_ID\tMODEL\tRUNNING\tPRIORITY\tQUEUED_S\n")
	for _, sec := range resp.Msg.Sections {
		for _, r := range sec.Rows {
			w.rowf("%s\t%s\t%s\t%s\t%t\t%d\t%.1f\n",
				sec.Name, r.MsgId, r.RequestId, r.ModelId, r.Running, r.Priority, r.QueuedSeconds)
		}
	}
	return w.flush()
}

func queueCancel(args []string) int {
	fs := flag.NewFlagSet("queue cancel", flag.ContinueOnError)
	q := fs.String("queue", "", "queue name (required)")
	msgID := fs.String("msg-id", "", "message ID from `queue list` (required)")
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, args)
	if !ok {
		return code
	}
	defer cancel()

	if *q == "" || *msgID == "" {
		return failUsage("queue cancel requires --queue and --msg-id")
	}
	resp, err := newClient(c).CancelQueuedJob(ctx, connect.NewRequest(&rpc.CancelQueuedJobRequest{
		Queue: *q,
		MsgId: *msgID,
	}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	return confirm("cancelled queued job %s", *msgID)
}

func queueCancelRunning(args []string) int {
	fs := flag.NewFlagSet("queue cancel-running", flag.ContinueOnError)
	reqID := fs.String("request-id", "", "in-flight request ID (required)")
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, args)
	if !ok {
		return code
	}
	defer cancel()

	if *reqID == "" {
		return failUsage("queue cancel-running requires --request-id")
	}
	resp, err := newClient(c).CancelRunningJob(ctx, connect.NewRequest(&rpc.CancelRunningJobRequest{
		RequestId: *reqID,
	}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	return confirm("cancelled running job %s", *reqID)
}

func queueEvict(args []string) int {
	fs := flag.NewFlagSet("queue evict", flag.ContinueOnError)
	q := fs.String("queue", "", "queue name (required)")
	msgID := fs.String("msg-id", "", "message ID from `queue list` (required)")
	c := registerCommon(fs, defaultReqTimeout)
	ctx, cancel, ok, code := parse(fs, c, args)
	if !ok {
		return code
	}
	defer cancel()

	if *q == "" || *msgID == "" {
		return failUsage("queue evict requires --queue and --msg-id")
	}
	resp, err := newClient(c).EvictQueuedJob(ctx, connect.NewRequest(&rpc.EvictQueuedJobRequest{
		Queue: *q,
		MsgId: *msgID,
	}))
	if err != nil {
		return fail(c, err)
	}
	if c.json {
		return printJSON(resp.Msg)
	}
	return confirm("evicted queued job %s", *msgID)
}
