package web

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"

	"github.com/chinese-room-solutions/mass/internal/audit"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/web/templates"
	"github.com/starfederation/datastar-go/datastar"
)

// Per-model benchmark card on the Models tab: what each (worker, device
// set) measured for the selected model, and the manual re-bench that wipes
// those verdicts and queues fresh measurements.
//
// The card is a MASS-owned fragment sitting under the runtime gateway's own
// detail panel — the gateway describes the file, MASS describes what the
// fleet made of it.

// handleModelBenchStatus renders the benchmark card for one catalogue model.
// Answers with an empty body (like the gateway's own detail endpoint) when
// the id names nothing benchable: a companion artifact, a model whose
// runtime isn't running, or an id the catalogue no longer knows.
func (h *Handler) handleModelBenchStatus(w http.ResponseWriter, r *http.Request) {
	runtimeName := r.URL.Query().Get("runtime")
	modelID := r.URL.Query().Get("id")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if runtimeName == "" || modelID == "" {
		return
	}
	key, err := h.modelStoreKey(r.Context(), runtimeName, modelID)
	if err != nil {
		h.logger.Debug().Err(err).Str("runtime", runtimeName).Str("model_id", modelID).
			Msg("resolving model store key for bench card")
		return
	}
	if _, err := io.WriteString(w, templates.RenderModelBenchPanel(h.modelBenchView(runtimeName, key))); err != nil {
		h.logger.Debug().Err(err).Msg("writing model bench card")
	}
}

// handleModelRebench drops every verdict recorded for the model — incapable
// rows included — and queues a fresh measurement on each eligible worker,
// then patches the card back so the operator sees the reset immediately.
//
// The key is the store-relative model key the card was rendered with: the
// catalogue id was already resolved once, on the GET, and re-resolving it
// here would only add a second way to get it wrong.
func (h *Handler) handleModelRebench(w http.ResponseWriter, r *http.Request) {
	if h.orch == nil {
		h.writeJSONErrorMsg(w, http.StatusServiceUnavailable, "scheduler not available")
		return
	}
	runtimeName := r.URL.Query().Get("runtime")
	key := r.URL.Query().Get("key")
	if runtimeName == "" || key == "" {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "runtime and key are required")
		return
	}
	if err := h.orch.RebenchModel(runtimeName, key); err != nil {
		h.writeJSONErrorMsg(w, http.StatusInternalServerError, err.Error())
		return
	}
	audit.Log(h.logger, "model.rebenched", key, audit.OutcomeOK).
		Str("actor", actorFromRequest(r)).Str("runtime", runtimeName).Msg("")

	sse := datastar.NewSSE(w, r)
	if err := sse.PatchElements(
		templates.RenderModelBenchPanel(h.modelBenchView(runtimeName, key)),
		datastar.WithModeReplace(),
	); err != nil {
		h.logger.Debug().Err(err).Msg("sse patch model-bench-card")
	}
}

// modelStoreKey maps a runtime catalogue id to the store-relative key its
// benchmark rows are recorded under, through the same catalogue walk the
// bench orchestrator uses — the key is the model's primary file relative to
// the models root, and nothing else matches a row.
func (h *Handler) modelStoreKey(ctx context.Context, runtimeName, modelID string) (string, error) {
	if h.runtimes == nil {
		return "", fmt.Errorf("%w: runtimes manager", ErrModelOpUnavailable)
	}
	models, err := h.runtimes.BenchModels(config.ModelsDir(h.dataDir))(ctx, runtimeName)
	if err != nil {
		return "", err
	}
	for _, m := range models {
		if m.ID == modelID {
			return m.Key, nil
		}
	}
	return "", fmt.Errorf("%w: no benchable model %s on %s", ErrModelOpInvalid, modelID, runtimeName)
}

// modelBenchView collects the fleet's verdicts on one model: the concluded
// rows from the store, plus the workers measuring it right now.
func (h *Handler) modelBenchView(runtimeName, modelKey string) templates.ModelBenchView {
	var rows []store.ModelBenchmarkRow
	if h.store != nil {
		var err error
		if rows, err = h.store.ListModelBenchmarksByModel(modelKey); err != nil {
			h.logger.Warn().Err(err).Str("model_key", modelKey).Msg("listing model benchmarks")
		}
	}
	names := map[string]string{}
	var benching []string
	if h.workers != nil {
		for _, wkr := range h.workers.All() {
			names[wkr.ID()] = wkr.Name()
			if h.orch != nil && h.orch.BenchInFlight(wkr.ID()) == modelKey {
				benching = append(benching, wkr.ID())
			}
		}
	}
	return templates.ModelBenchView{
		RuntimeName: runtimeName,
		ModelKey:    modelKey,
		Rows:        buildModelBenchRows(rows, names, benching),
	}
}

// buildModelBenchRows merges the concluded rows with the in-flight workers.
// A worker being measured right now shows only that — whatever it recorded
// before is on its way out. Rows sort by worker then device set so the card
// doesn't reshuffle between polls.
func buildModelBenchRows(rows []store.ModelBenchmarkRow, names map[string]string, benching []string) []templates.ModelBenchRowView {
	workerName := func(id string) string {
		if name := names[id]; name != "" {
			return name
		}
		return id
	}
	inFlight := make(map[string]bool, len(benching))
	for _, id := range benching {
		inFlight[id] = true
	}

	out := make([]templates.ModelBenchRowView, 0, len(rows)+len(benching))
	for _, id := range benching {
		out = append(out, templates.ModelBenchRowView{
			WorkerName: workerName(id),
			State:      templates.ModelBenchRunning,
		})
	}
	for _, row := range rows {
		if inFlight[row.WorkerID] {
			continue
		}
		view := templates.ModelBenchRowView{
			WorkerName:   workerName(row.WorkerID),
			DeviceSet:    row.DeviceSet,
			State:        templates.ModelBenchMeasured,
			UnitsPerSec:  row.UnitsPerSec,
			GraphSecs:    row.GraphSecs,
			BaseBytes:    row.BaseBytes,
			PerSlotBytes: row.PerSlotBytes,
		}
		if row.Error != "" {
			view.State = templates.ModelBenchIncapable
			view.Error = row.Error
		}
		out = append(out, view)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].WorkerName != out[j].WorkerName {
			return out[i].WorkerName < out[j].WorkerName
		}
		return out[i].DeviceSet < out[j].DeviceSet
	})
	return out
}
