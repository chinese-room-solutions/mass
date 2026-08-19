// MASS updates itself by running its own installer over its own install: the
// daemon notices a newer release, downloads that release's mass-setup, and
// hands it the recorded install directory with --relaunch. The setup waits for
// this process to exit, stages the new build, and starts the app again.
//
// Finding a release and installing one both live in mass-sdk/selfupdate. What
// stays here is what only MASS knows: the fleet gate (a new build can strand
// connected workers) and the operator-managed daemon that won't restart itself.
package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/chinese-room-solutions/mass-sdk/registry"
	"github.com/chinese-room-solutions/mass-sdk/selfupdate"
)

// RunUpdateChecks keeps the daemon's answer to "is there a newer MASS?" fresh:
// one check now, then another every selfupdate.DefaultInterval until ctx ends.
// A single check at startup is not enough — the dashboard can render before the
// first answer lands, and a daemon left running outlives releases published
// after it started.
func (h *Handler) RunUpdateChecks(ctx context.Context) {
	h.update.Run(ctx)
}

// errOperatorManaged refuses to restart a daemon nobody asked to be restarted.
// An on-demand daemon belongs to the GUI/CLI that spawned it and comes back by
// itself; a `mass serve` is a fleet hub someone runs deliberately, and taking it
// down from under its workers is not something a dashboard click gets to do.
// The SDK's own refusals (not installer-placed, needs elevation) come back out
// of Applier.Apply the same way.
var errOperatorManaged = selfupdate.Refuse(
	"this MASS runs as an operator-managed server, so it won't restart itself — " +
		"stop it and run the installer, or update it from the desktop app")

// errIncompatibleFleet is the fleet gate's refusal: connected workers the
// candidate build would strand. Count carries how many, Names a sample for the
// message. It is only advisory in the sense that {"force": true} overrides it.
type errIncompatibleFleet struct {
	Count int
	Names []string
	Tag   string
}

func (e *errIncompatibleFleet) Error() string {
	msg := fmt.Sprintf("%s would strand %s", e.Tag, workersPhrase(e.Count))
	if len(e.Names) > 0 {
		msg += " (" + joinNames(e.Names) + ")"
	}
	return msg + " — a stranded worker exits and stays down until it is upgraded. " +
		"Upgrade the workers first, or send {\"force\": true} to update anyway."
}

func workersPhrase(n int) string {
	if n == 1 {
		return "1 connected worker"
	}
	return fmt.Sprintf("%d connected workers", n)
}

func joinNames(names []string) string {
	out := ""
	for i, n := range names {
		if i > 0 {
			out += ", "
		}
		out += n
	}
	return out
}

// applyUpdate installs tag over this install — MASS's own refusal first, then
// the SDK's.
func (h *Handler) applyUpdate(ctx context.Context, tag string) error {
	// Same guardrail ensureDaemon applies when a skewed client meets a
	// non-on-demand daemon (cmd/mass/launch.go).
	if !h.onDemand {
		return errOperatorManaged
	}
	return h.applier.Apply(ctx, tag)
}

// --- The fleet pre-flight gate ---

// UpdateFleetGate is what the candidate-aware compat check found: how many
// connected workers the registry index says the candidate MASS build would
// reject at Register, and a sample of their names.
type UpdateFleetGate struct {
	Incompatible int      `json:"incompatible"`
	Names        []string `json:"names,omitempty"`
}

// fleetGateNameCap bounds the names carried in the response and the message —
// enough to recognise the fleet, not a full listing.
const fleetGateNameCap = 5

// updateFleetGate counts the connected workers whose registry-index row admits
// this MASS build but not candidate. It is the load-bearing half of the update
// flow: a version-skewed worker is rejected with FAILED_PRECONDITION at
// Register, which the worker treats as fatal — it exits and STAYS DOWN until
// somebody upgrades it (internal/worker/hub.go). So an unchecked MASS upgrade
// can silently empty the fleet.
//
// Cache-only, never network — the same posture as CheckWorkerCompat, whose
// verdict this predicts. Every inconclusive input (no cached index, no row for
// the worker's version, an unparseable range, a dev/pre-release version) counts
// as compatible, because that is exactly what Register would do with it:
// accept-with-warning. The gate must never claim a breakage the hub wouldn't
// enforce.
func (h *Handler) updateFleetGate(candidate string) UpdateFleetGate {
	idx, err := h.cachedIndex()
	if err != nil || idx == nil {
		h.logger.Debug().Err(err).Msg("no cached registry index for the update fleet gate")
		return UpdateFleetGate{}
	}
	return fleetGate(idx, h.fleetPairings(), candidate)
}

// fleetGate is updateFleetGate's verdict over an explicit index and fleet.
func fleetGate(idx *registry.Index, workers []workerPairing, candidate string) UpdateFleetGate {
	gate := UpdateFleetGate{}
	for _, w := range workers {
		if !workerRejectsMass(idx, w, candidate) {
			continue
		}
		gate.Incompatible++
		if len(gate.Names) < fleetGateNameCap {
			gate.Names = append(gate.Names, w.RuntimeName+" "+w.Version)
		}
	}
	return gate
}

// workerRejectsMass reports whether the index says w's version pairs with no
// MASS in candidate's range. Mirrors checkWorkerIndexCompat's row semantics:
// one admitting row is enough, and an inconclusive row admits.
func workerRejectsMass(idx *registry.Index, w workerPairing, candidate string) bool {
	wv, err := semver.NewVersion(w.Version)
	if err != nil {
		return false
	}
	rows := workerVersionRows(idx, w.RuntimeName, wv)
	if len(rows) == 0 {
		return false
	}
	for _, row := range rows {
		if ok, reason := admits(row.Mass, candidate); ok || reason != "" {
			return false
		}
	}
	return true
}

// --- HTTP surface ---

// UpdateCheckResponse is what both /api/update/check verbs answer: the running
// build, the newer tag (empty when there is none), when that answer was taken,
// why the last check failed, and the fleet the candidate would strand.
type UpdateCheckResponse struct {
	selfupdate.Status
	UpdateFleetGate
}

// handleUpdateCheck reports the last known answer and what applying it would
// cost the fleet. It reads stored state and the on-disk index cache — no
// network, so a dashboard may call it on every load.
func (h *Handler) handleUpdateCheck(w http.ResponseWriter, _ *http.Request) {
	h.writeUpdateStatus(w, h.update.Status())
}

// handleUpdateCheckNow asks the release repository right now — the operator
// pressing "Check for updates", or a `mass update` that must not report a
// never-taken answer. A check that can't reach the repository is not an HTTP
// error: the operator needs the sentence, which the status carries, and a
// status code would read as a broken daemon rather than an unreachable GitHub.
func (h *Handler) handleUpdateCheckNow(w http.ResponseWriter, r *http.Request) {
	st, err := h.update.Check(r.Context())
	if err != nil {
		h.logger.Debug().Err(err).Str("url", h.update.BaseURL).Msg("checking for a newer MASS release")
	}
	h.writeUpdateStatus(w, st)
}

// writeUpdateStatus answers with st, priced against the connected fleet.
func (h *Handler) writeUpdateStatus(w http.ResponseWriter, st selfupdate.Status) {
	resp := UpdateCheckResponse{Status: st}
	if st.Available != "" {
		resp.UpdateFleetGate = h.updateFleetGate(st.Available)
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		h.logger.Debug().Err(err).Msg("writing the update check response")
	}
}

// updateApplyRequest is POST /api/update/apply's optional body. force
// overrides the fleet gate — safe by default, but the operator who has read the
// warning can still choose.
type updateApplyRequest struct {
	Force bool `json:"force"`
}

// handleUpdateApply installs the release the check found. It does the parts
// that can fail — the gates, resolving the install, downloading and verifying
// the installer — inside the request, so a caller that gets a 200 knows the
// installer is running. Only then does it retire the daemon, from a goroutine:
// the shutdown drains this very request.
func (h *Handler) handleUpdateApply(w http.ResponseWriter, r *http.Request) {
	tag := h.update.Available()
	if tag == "" {
		http.Error(w, "no MASS update is available", http.StatusConflict)
		return
	}
	var req updateApplyRequest
	if r.Body != nil {
		// An empty or malformed body is simply "no force" — the flag is the only
		// thing in it, and a caller that sends nothing means the default.
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	if !req.Force {
		if gate := h.updateFleetGate(tag); gate.Incompatible > 0 {
			http.Error(w, (&errIncompatibleFleet{
				Count: gate.Incompatible, Names: gate.Names, Tag: tag,
			}).Error(), http.StatusConflict)
			return
		}
	}

	h.logger.Info().Str("tag", tag).Bool("force", req.Force).Msg("applying a MASS update")
	if err := h.applyUpdate(r.Context(), tag); err != nil {
		// A refusal is the operator's situation rather than a fault, so it
		// answers with the sentence to act on and a 409.
		status := http.StatusInternalServerError
		if selfupdate.IsRefusal(err) {
			status = http.StatusConflict
		} else {
			h.logger.Warn().Err(err).Msg("applying the MASS update")
		}
		http.Error(w, err.Error(), status)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "updating", "version": tag}); err != nil {
		h.logger.Debug().Err(err).Msg("writing the update apply response")
	}

	// Tell the attached window before retiring. Without this it would just
	// reconnect to whatever answers on the same port next — which is the
	// relaunched build's own daemon — and the user would be left with two MASS
	// windows: this one re-attached, and the new instance's. With no window
	// attached (a headless serve, a browser) there is nobody to tell, and the
	// browser tab simply reloads against the new daemon.
	h.gui.send(guiEvent{name: GUIEventUpdateRestarting, data: tag})

	// The installer is waiting for this process's files to be replaceable, so
	// the daemon has to go. The window is quitting on the event above; the grace
	// gives that event its trip down the SSE stream before Shutdown closes it.
	h.shutdownMu.Lock()
	fn := h.shutdownFn
	h.shutdownMu.Unlock()
	if fn != nil {
		go func() {
			time.Sleep(updateShutdownGrace)
			fn()
		}()
	}
}

// updateShutdownGrace is the pause between telling the window an update is
// coming and tearing the server down. Only long enough for that event to be
// written and read — the window's own notice-then-quit grace runs in parallel,
// and the installer is already waiting on both processes to exit.
const updateShutdownGrace = 250 * time.Millisecond
