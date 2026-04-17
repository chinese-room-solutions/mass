package scheduler

import (
	"fmt"
	"sync"
	"time"

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/pkg/llm"
	"github.com/rs/zerolog"
)

// activeStickyDuration is how long an instance stays "Active" after its last
// request finishes. This prevents the status from flickering between Active
// and Idle on short bursts of requests.
const activeStickyDuration = 100 * time.Millisecond

// InstanceMode describes how a model instance was loaded into the pool.
type InstanceMode int

const (
	// ModeDynamic means the model was loaded on-demand by an inference request.
	// It is subject to idle timeout eviction.
	ModeDynamic InstanceMode = iota
	// ModeStatic means the model was explicitly loaded by a user or app.
	// It persists until explicitly evicted.
	ModeStatic
)

func (m InstanceMode) String() string {
	switch m {
	case ModeDynamic:
		return "dynamic"
	case ModeStatic:
		return "static"
	default:
		return "unknown"
	}
}

// chatInstance holds a loaded chat model and its metadata.
type chatInstance struct {
	fingerprint string
	name        string // model requirement name (e.g. "chat"); empty for dynamic loads
	config      config.LlamaChatConfig
	placement   config.PlacementConfig // placement used at load time
	model       llm.ChatModelInterface
	source      string       // who loaded it: "direct", "app:<name>"
	mode        InstanceMode // how it was loaded
	workerID    string       // ID of the agent running this model
	workerName  string       // human-readable name of the agent
	deviceIDs   []string     // device(s) the model is loaded on
	activeReqs  int64
	idleTimer   *time.Timer
	idleSince   time.Time // when activeReqs last reached zero (zero value = never been active)
	loading     bool
	loadDone    chan struct{} // closed when loading finishes
	loadErr     error         // non-nil if loading failed
	// draining is set true under p.mu before Evict releases the lock to call
	// model.Close(). Subsequent Acquire calls must observe this and refuse —
	// closes the TOCTOU window between CanEvict and Evict.
	draining bool
}

// embeddingInstance holds a loaded embedding model and its metadata.
type embeddingInstance struct {
	fingerprint string
	name        string // model requirement name (e.g. "embedding"); empty for dynamic loads
	config      config.LlamaEmbeddingConfig
	placement   config.PlacementConfig // placement used at load time
	model       llm.EmbeddingModelInterface
	source      string       // who loaded it: "direct", "app:<name>"
	mode        InstanceMode // how it was loaded
	workerID    string       // ID of the agent running this model
	workerName  string       // human-readable name of the agent
	deviceIDs   []string     // device(s) the model is loaded on
	activeReqs  int64
	idleTimer   *time.Timer
	idleSince   time.Time // when activeReqs last reached zero (zero value = never been active)
	loading     bool
	loadDone    chan struct{} // closed when loading finishes
	loadErr     error         // non-nil if loading failed
	// draining — see chatInstance.draining.
	draining bool
}

// modelPool manages loaded model instances keyed by config fingerprint.
// Requests with identical config fingerprints share the same loaded instance.
// Dynamic instances are evicted after an idle timeout with no active requests.

// PoolChangeKind distinguishes structural changes from status updates.
type PoolChangeKind int

const (
	// PoolChangeList means the set of loaded models changed (added/removed).
	PoolChangeList PoolChangeKind = iota
	// PoolChangeStatus means an existing model's status changed (active reqs, idle).
	PoolChangeStatus
)

// PoolChangeEvent describes what changed in the model pool.
type PoolChangeEvent struct {
	Kind        PoolChangeKind
	Fingerprint string // set for PoolChangeStatus
}

// PoolChangeCallback is called whenever the pool changes.
type PoolChangeCallback func(PoolChangeEvent)

type modelPool struct {
	mu          sync.RWMutex
	chatModels  map[string]*chatInstance
	embedModels map[string]*embeddingInstance
	loader      llm.ModelLoaderInterface
	loaderID    string // agent ID of the default loader
	loaderName  string // agent name of the default loader
	logger      zerolog.Logger
	idleTimeout time.Duration
	onChange    []PoolChangeCallback
}

func newModelPool(loader llm.ModelLoaderInterface, loaderID, loaderName string, logger zerolog.Logger, idleTimeout time.Duration) *modelPool {
	return &modelPool{
		chatModels:  make(map[string]*chatInstance),
		embedModels: make(map[string]*embeddingInstance),
		loader:      loader,
		loaderID:    loaderID,
		loaderName:  loaderName,
		logger:      logger,
		idleTimeout: idleTimeout,
	}
}

// AddChangeCallback registers a callback invoked when the pool changes.
func (p *modelPool) AddChangeCallback(cb PoolChangeCallback) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onChange = append(p.onChange, cb)
}

// notifyChange calls all registered change callbacks (must be called without holding mu).
func (p *modelPool) notifyChange(evt PoolChangeEvent) {
	p.mu.RLock()
	cbs := make([]PoolChangeCallback, len(p.onChange))
	copy(cbs, p.onChange)
	p.mu.RUnlock()
	for _, cb := range cbs {
		cb(evt)
	}
}

// GetOrLoadChat returns a chat predictor for the given config. If an instance
// with the same fingerprint exists, it is reused. Otherwise a new model is
// loaded and registered. The instance's active request count is incremented;
// the caller must call Release(fp) when done.
func (p *modelPool) GetOrLoadChat(cfg config.LlamaChatConfig, placement config.PlacementConfig, source string) (llm.PredictorInterface, string, error) {
	fp := cfg.Fingerprint()

	for {
		p.mu.RLock()
		inst, ok := p.chatModels[fp]
		if ok && !inst.loading {
			p.mu.RUnlock()
			if p.acquireChat(inst) {
				p.notifyChange(PoolChangeEvent{Kind: PoolChangeStatus, Fingerprint: fp})
				return inst.model.Pool(), fp, nil
			}
			// Instance was draining — treat as gone and loop to create fresh.
			continue
		}
		if ok && inst.loading {
			done := inst.loadDone
			p.mu.RUnlock()
			<-done
			// Re-check: if loading failed, the instance was removed; loop will retry or create new.
			p.mu.RLock()
			inst, ok = p.chatModels[fp]
			p.mu.RUnlock()
			if ok && !inst.loading {
				if p.acquireChat(inst) {
					p.notifyChange(PoolChangeEvent{Kind: PoolChangeStatus, Fingerprint: fp})
					return inst.model.Pool(), fp, nil
				}
				continue
			}
			if ok && inst.loadErr != nil {
				return nil, "", inst.loadErr
			}
			// Instance was removed (load failed and cleaned up), retry from scratch.
			continue
		}
		p.mu.RUnlock()

		// No instance exists — try to create a loading placeholder.
		p.mu.Lock()
		// Double-check after acquiring write lock.
		if inst, ok := p.chatModels[fp]; ok {
			if !inst.loading {
				if !p.acquireChatLocked(inst) {
					// Draining — drop the lock and loop.
					p.mu.Unlock()
					continue
				}
				p.mu.Unlock()
				p.notifyChange(PoolChangeEvent{Kind: PoolChangeStatus, Fingerprint: fp})
				return inst.model.Pool(), fp, nil
			}
			// Another goroutine started loading — wait on it.
			done := inst.loadDone
			p.mu.Unlock()
			<-done
			continue
		}

		// Insert loading placeholder.
		inst = &chatInstance{
			fingerprint: fp,
			config:      cfg,
			placement:   placement,
			source:      source,
			mode:        ModeDynamic,
			workerID:    p.loaderID,
			workerName:  p.loaderName,
			loading:     true,
			loadDone:    make(chan struct{}),
		}
		p.chatModels[fp] = inst
		p.mu.Unlock()

		p.logger.Info().Str("fingerprint", fp).Str("path", cfg.Path).Msg("loading chat model")
		p.notifyChange(PoolChangeEvent{Kind: PoolChangeList})

		// Load outside lock.
		model, err := p.loader.LoadChatModel(p.logger, fp, cfg, placement)

		p.mu.Lock()
		if err != nil {
			inst.loadErr = fmt.Errorf("loading chat model (fp=%s): %w", fp, err)
			delete(p.chatModels, fp)
			close(inst.loadDone)
			p.mu.Unlock()
			p.notifyChange(PoolChangeEvent{Kind: PoolChangeList})
			return nil, "", inst.loadErr
		}

		inst.model = model
		inst.loading = false
		inst.activeReqs = 1 // born with one active request
		// Worker may have allocated fewer slots than requested (VRAM ran
		// out mid-pool-init). Trust its reported size over our heuristic
		// so dispatcher concurrency math matches reality.
		if actual := model.PoolSize(); actual > 0 && actual != inst.placement.MaxConcurrent {
			p.logger.Info().Str("fingerprint", fp).
				Int32("requested", inst.placement.MaxConcurrent).
				Int32("actual", actual).
				Msg("worker capped pool size — updating placement")
			inst.placement.MaxConcurrent = actual
		}
		close(inst.loadDone)
		p.logger.Info().Str("fingerprint", fp).Str("path", cfg.Path).Msg("chat model loaded dynamically")
		p.mu.Unlock()

		p.notifyChange(PoolChangeEvent{Kind: PoolChangeList})
		return model.Pool(), fp, nil
	}
}

// GetOrLoadEmbedding returns an embedder for the given config. If an instance
// with the same fingerprint exists, it is reused. Otherwise a new model is
// loaded and registered. The instance's active request count is incremented;
// the caller must call Release(fp) when done.
func (p *modelPool) GetOrLoadEmbedding(cfg config.LlamaEmbeddingConfig, placement config.PlacementConfig, source string) (llm.EmbedderInterface, string, error) {
	fp := cfg.Fingerprint()

	for {
		p.mu.RLock()
		inst, ok := p.embedModels[fp]
		if ok && !inst.loading {
			p.mu.RUnlock()
			if p.acquireEmbedding(inst) {
				p.notifyChange(PoolChangeEvent{Kind: PoolChangeStatus, Fingerprint: fp})
				return inst.model.Pool(), fp, nil
			}
			continue
		}
		if ok && inst.loading {
			done := inst.loadDone
			p.mu.RUnlock()
			<-done
			p.mu.RLock()
			inst, ok = p.embedModels[fp]
			p.mu.RUnlock()
			if ok && !inst.loading {
				if p.acquireEmbedding(inst) {
					p.notifyChange(PoolChangeEvent{Kind: PoolChangeStatus, Fingerprint: fp})
					return inst.model.Pool(), fp, nil
				}
				continue
			}
			if ok && inst.loadErr != nil {
				return nil, "", inst.loadErr
			}
			continue
		}
		p.mu.RUnlock()

		p.mu.Lock()
		if inst, ok := p.embedModels[fp]; ok {
			if !inst.loading {
				if !p.acquireEmbeddingLocked(inst) {
					p.mu.Unlock()
					continue
				}
				p.mu.Unlock()
				p.notifyChange(PoolChangeEvent{Kind: PoolChangeStatus, Fingerprint: fp})
				return inst.model.Pool(), fp, nil
			}
			done := inst.loadDone
			p.mu.Unlock()
			<-done
			continue
		}

		inst = &embeddingInstance{
			fingerprint: fp,
			config:      cfg,
			placement:   placement,
			source:      source,
			mode:        ModeDynamic,
			workerID:    p.loaderID,
			workerName:  p.loaderName,
			loading:     true,
			loadDone:    make(chan struct{}),
		}
		p.embedModels[fp] = inst
		p.mu.Unlock()

		p.logger.Info().Str("fingerprint", fp).Str("path", cfg.Path).Msg("loading embedding model")
		p.notifyChange(PoolChangeEvent{Kind: PoolChangeList})

		model, err := p.loader.LoadEmbeddingModel(p.logger, fp, cfg, placement)

		p.mu.Lock()
		if err != nil {
			inst.loadErr = fmt.Errorf("loading embedding model (fp=%s): %w", fp, err)
			delete(p.embedModels, fp)
			close(inst.loadDone)
			p.mu.Unlock()
			p.notifyChange(PoolChangeEvent{Kind: PoolChangeList})
			return nil, "", inst.loadErr
		}

		inst.model = model
		inst.loading = false
		inst.activeReqs = 1
		if actual := model.PoolSize(); actual > 0 && actual != inst.placement.MaxConcurrent {
			p.logger.Info().Str("fingerprint", fp).
				Int32("requested", inst.placement.MaxConcurrent).
				Int32("actual", actual).
				Msg("worker capped embedding pool size — updating placement")
			inst.placement.MaxConcurrent = actual
		}
		close(inst.loadDone)
		p.logger.Info().Str("fingerprint", fp).Str("path", cfg.Path).Msg("embedding model loaded dynamically")
		p.mu.Unlock()

		p.notifyChange(PoolChangeEvent{Kind: PoolChangeList})
		return model.Pool(), fp, nil
	}
}

// Release decrements the active request count for the given fingerprint.
// If the count reaches zero and the instance is dynamic, the idle timer starts.
func (p *modelPool) Release(fp string) {
	p.mu.Lock()

	if inst, ok := p.chatModels[fp]; ok {
		if inst.loading {
			p.mu.Unlock()
			return
		}
		inst.activeReqs--
		newCount := inst.activeReqs
		if newCount <= 0 {
			inst.idleSince = time.Now()
			if inst.mode == ModeDynamic && p.idleTimeout > 0 {
				inst.idleTimer = time.AfterFunc(p.idleTimeout, func() {
					p.evict(fp)
				})
			}
		}
		p.mu.Unlock()
		evt := PoolChangeEvent{Kind: PoolChangeStatus, Fingerprint: fp}
		p.notifyChange(evt)
		if newCount <= 0 {
			// Schedule a delayed notification so the UI transitions from Active to Idle
			// after activeStickyDuration expires.
			time.AfterFunc(activeStickyDuration, func() { p.notifyChange(evt) })
		}
		return
	}

	if inst, ok := p.embedModels[fp]; ok {
		if inst.loading {
			p.mu.Unlock()
			return
		}
		inst.activeReqs--
		newCount := inst.activeReqs
		if newCount <= 0 {
			inst.idleSince = time.Now()
			if inst.mode == ModeDynamic && p.idleTimeout > 0 {
				inst.idleTimer = time.AfterFunc(p.idleTimeout, func() {
					p.evict(fp)
				})
			}
		}
		p.mu.Unlock()
		evt := PoolChangeEvent{Kind: PoolChangeStatus, Fingerprint: fp}
		p.notifyChange(evt)
		if newCount <= 0 {
			time.AfterFunc(activeStickyDuration, func() { p.notifyChange(evt) })
		}
		return
	}

	p.mu.Unlock()
}

// CanEvict reports whether the instance with the given fingerprint can safely
// be evicted right now (loaded, idle, no in-flight requests, not already
// draining). This is a snapshot — a concurrent Acquire could land between
// CanEvict and TryEvict; callers must use TryEvict to commit.
func (p *modelPool) CanEvict(fp string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if inst, ok := p.chatModels[fp]; ok {
		return !inst.loading && !inst.draining && inst.activeReqs <= 0
	}
	if inst, ok := p.embedModels[fp]; ok {
		return !inst.loading && !inst.draining && inst.activeReqs <= 0
	}
	return false
}

// TryEvict atomically evicts an idle instance. Returns false if it's busy,
// loading, already draining, or unknown — caller backs off and retries.
//
// Used during model swaps: closes the TOCTOU window between CanEvict and
// Evict by re-checking activeReqs under the same lock that flips draining,
// so a redelivered Acquire can't slip in between.
func (p *modelPool) TryEvict(fp string) bool {
	// See Evict for why model.Close() runs outside the lock.
	var toClose interface{ Close() }
	var path string
	var kind string

	p.mu.Lock()
	if inst, ok := p.chatModels[fp]; ok {
		if !inst.loading && !inst.draining && inst.activeReqs <= 0 {
			inst.draining = true
			if inst.idleTimer != nil {
				inst.idleTimer.Stop()
			}
			delete(p.chatModels, fp)
			toClose, path, kind = inst.model, inst.config.Path, "chat"
		}
	} else if inst, ok := p.embedModels[fp]; ok {
		if !inst.loading && !inst.draining && inst.activeReqs <= 0 {
			inst.draining = true
			if inst.idleTimer != nil {
				inst.idleTimer.Stop()
			}
			delete(p.embedModels, fp)
			toClose, path, kind = inst.model, inst.config.Path, "embedding"
		}
	}
	p.mu.Unlock()

	if toClose == nil {
		return false
	}

	p.notifyChange(PoolChangeEvent{Kind: PoolChangeList})
	go p.safeClose(toClose, fp, path, kind, "evicted for model swap")
	return true
}

// Evict forcefully removes a loaded instance, interrupting any in-flight
// requests. Used by the UI "Evict" button. Loading instances are skipped
// (the loading goroutine owns the slot). Returns true if evicted.
func (p *modelPool) Evict(fp string) bool {
	// Remove the map entry under the lock so eviction is immediately visible
	// to Snapshot/Acquire; run model.Close() outside the lock, since the
	// worker UnloadModel round-trip can take seconds for a large model and
	// would otherwise stall every other pool op.
	var toClose interface{ Close() }
	var path string
	var kind string

	p.mu.Lock()
	if inst, ok := p.chatModels[fp]; ok {
		if !inst.loading {
			inst.draining = true
			if inst.idleTimer != nil {
				inst.idleTimer.Stop()
			}
			delete(p.chatModels, fp)
			toClose, path, kind = inst.model, inst.config.Path, "chat"
		}
	} else if inst, ok := p.embedModels[fp]; ok {
		if !inst.loading {
			inst.draining = true
			if inst.idleTimer != nil {
				inst.idleTimer.Stop()
			}
			delete(p.embedModels, fp)
			toClose, path, kind = inst.model, inst.config.Path, "embedding"
		}
	}
	p.mu.Unlock()

	if toClose == nil {
		return false
	}

	p.notifyChange(PoolChangeEvent{Kind: PoolChangeList})
	go p.safeClose(toClose, fp, path, kind, "evicted by user")
	return true
}

// evict closes and removes an idle instance after the idle timer fires.
// Like TryEvict, sets draining first so a request that arrived in the same
// instant doesn't grab a stale instance.
func (p *modelPool) evict(fp string) {
	// See Evict for why model.Close() runs outside the lock.
	var toClose interface{ Close() }
	var path string
	var kind string

	p.mu.Lock()
	if inst, ok := p.chatModels[fp]; ok {
		if inst.activeReqs <= 0 && inst.mode == ModeDynamic && !inst.loading && !inst.draining {
			inst.draining = true
			delete(p.chatModels, fp)
			toClose, path, kind = inst.model, inst.config.Path, "chat"
		}
	} else if inst, ok := p.embedModels[fp]; ok {
		if inst.activeReqs <= 0 && inst.mode == ModeDynamic && !inst.loading && !inst.draining {
			inst.draining = true
			delete(p.embedModels, fp)
			toClose, path, kind = inst.model, inst.config.Path, "embedding"
		}
	}
	p.mu.Unlock()

	if toClose == nil {
		return
	}

	p.notifyChange(PoolChangeEvent{Kind: PoolChangeList})
	go p.safeClose(toClose, fp, path, kind, "evicted after idle timeout")
}

// EvictByWorker drops every instance hosted on the given worker after it
// disconnects. The inference state died with the worker, so stale pool
// entries pointing at a dead stream would just fail (or crash) on future
// Acquire/Evict. Close() is still called on each (best-effort; safeClose
// swallows the inevitable "worker offline" error). Returns drop count.
func (p *modelPool) EvictByWorker(workerID string) int {
	type victim struct {
		toClose interface{ Close() }
		fp      string
		path    string
		kind    string
	}
	var victims []victim

	p.mu.Lock()
	for fp, inst := range p.chatModels {
		if inst.workerID != workerID || inst.draining {
			continue
		}
		inst.draining = true
		if inst.idleTimer != nil {
			inst.idleTimer.Stop()
		}
		victims = append(victims, victim{inst.model, fp, inst.config.Path, "chat"})
		delete(p.chatModels, fp)
	}
	for fp, inst := range p.embedModels {
		if inst.workerID != workerID || inst.draining {
			continue
		}
		inst.draining = true
		if inst.idleTimer != nil {
			inst.idleTimer.Stop()
		}
		victims = append(victims, victim{inst.model, fp, inst.config.Path, "embedding"})
		delete(p.embedModels, fp)
	}
	p.mu.Unlock()

	if len(victims) == 0 {
		return 0
	}

	p.notifyChange(PoolChangeEvent{Kind: PoolChangeList})
	for _, v := range victims {
		go p.safeClose(v.toClose, v.fp, v.path, v.kind, "evicted on worker disconnect")
	}
	return len(victims)
}

// safeClose runs model.Close() with a panic recovery and logs the result.
// Close() round-trips to a worker, which may be offline, slow, or buggy;
// without a recover here, any panic would kill the entire MASS process.
func (p *modelPool) safeClose(m interface{ Close() }, fp, path, kind, reason string) {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Warn().Str("fingerprint", fp).Str("path", path).Interface("panic", r).Msgf("%s model close panicked (%s)", kind, reason)
		}
	}()
	m.Close()
	p.logger.Info().Str("fingerprint", fp).Str("path", path).Msgf("%s model %s", kind, reason)
}

// acquireChat increments activeReqs and cancels any idle timer (caller holds
// no lock). Returns false if the instance is draining (eviction in progress).
func (p *modelPool) acquireChat(inst *chatInstance) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.acquireChatLocked(inst)
}

// acquireChatLocked increments activeReqs and cancels any idle timer (caller
// holds write lock). Returns false if the instance is draining.
func (p *modelPool) acquireChatLocked(inst *chatInstance) bool {
	if inst.draining {
		return false
	}
	if inst.idleTimer != nil {
		inst.idleTimer.Stop()
		inst.idleTimer = nil
	}
	inst.idleSince = time.Time{}
	inst.activeReqs++
	return true
}

// acquireEmbedding increments activeReqs and cancels any idle timer (caller
// holds no lock). Returns false if the instance is draining.
func (p *modelPool) acquireEmbedding(inst *embeddingInstance) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.acquireEmbeddingLocked(inst)
}

// acquireEmbeddingLocked increments activeReqs and cancels any idle timer
// (caller holds write lock). Returns false if the instance is draining.
func (p *modelPool) acquireEmbeddingLocked(inst *embeddingInstance) bool {
	if inst.draining {
		return false
	}
	if inst.idleTimer != nil {
		inst.idleTimer.Stop()
		inst.idleTimer = nil
	}
	inst.idleSince = time.Time{}
	inst.activeReqs++
	return true
}

// AcquireChat looks up a loaded chat model by fingerprint, increments its
// active request count, and returns its predictor. Returns false if the
// fingerprint is not found, still loading, or being evicted (draining).
func (p *modelPool) AcquireChat(fp string) (llm.PredictorInterface, string, bool) {
	p.mu.Lock()
	inst, ok := p.chatModels[fp]
	if !ok || inst.loading || inst.draining {
		p.mu.Unlock()
		return nil, "", false
	}
	p.acquireChatLocked(inst)
	p.mu.Unlock()
	p.notifyChange(PoolChangeEvent{Kind: PoolChangeStatus, Fingerprint: fp})
	return inst.model.Pool(), fp, true
}

// AcquireEmbedding looks up a loaded embedding model by fingerprint, increments
// its active request count, and returns its embedder. Returns false if the
// fingerprint is not found, still loading, or being evicted (draining).
func (p *modelPool) AcquireEmbedding(fp string) (llm.EmbedderInterface, string, bool) {
	p.mu.Lock()
	inst, ok := p.embedModels[fp]
	if !ok || inst.loading || inst.draining {
		p.mu.Unlock()
		return nil, "", false
	}
	p.acquireEmbeddingLocked(inst)
	p.mu.Unlock()
	p.notifyChange(PoolChangeEvent{Kind: PoolChangeStatus, Fingerprint: fp})
	return inst.model.Pool(), fp, true
}

// IdleInstanceInfo describes an idle model instance for eviction decisions.
type IdleInstanceInfo struct {
	Fingerprint string
	ModelPath   string
	WorkerID    string
	IdleSince   time.Time
}

// evictionGracePeriod is the minimum time at activeReqs==0 before another
// request can evict a model. Much shorter than the cleanup idle-timeout —
// just smooths over brief inter-request gaps within a session (e.g.
// pdf2doc's per-page calls), not a shutdown timer.
const evictionGracePeriod = 2 * time.Second

// IdleInstancesOnDevice returns instances on the given worker+devices that
// have been at activeReqs==0 for at least evictionGracePeriod. Used by the
// dispatcher to find eviction candidates. The grace period prevents
// back-to-back calls from the same app (per-page, multi-turn) being
// interrupted by another request stealing the device in a millisecond gap.
func (p *modelPool) IdleInstancesOnDevice(workerID string, deviceIDs []string) []IdleInstanceInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()

	deviceSet := make(map[string]bool, len(deviceIDs))
	for _, d := range deviceIDs {
		deviceSet[d] = true
	}

	matchesDevice := func(instDeviceIDs []string) bool {
		if len(instDeviceIDs) == 0 {
			// Legacy: no device info tracked, match by agent only.
			return true
		}
		for _, d := range instDeviceIDs {
			if deviceSet[d] {
				return true
			}
		}
		return false
	}

	now := time.Now()
	// Zero idleSince means the instance has never had a request finish (either
	// freshly loaded and unused, or registered as static) — always evictable.
	// Otherwise require evictionGracePeriod to have elapsed since the last
	// request finished.
	idleEnough := func(idleSince time.Time) bool {
		if idleSince.IsZero() {
			return true
		}
		return now.Sub(idleSince) >= evictionGracePeriod
	}

	var out []IdleInstanceInfo
	for _, inst := range p.chatModels {
		if inst.workerID == workerID && inst.activeReqs <= 0 && !inst.loading && matchesDevice(inst.deviceIDs) && idleEnough(inst.idleSince) {
			out = append(out, IdleInstanceInfo{
				Fingerprint: inst.fingerprint,
				ModelPath:   inst.config.Path,
				WorkerID:    inst.workerID,
				IdleSince:   inst.idleSince,
			})
		}
	}
	for _, inst := range p.embedModels {
		if inst.workerID == workerID && inst.activeReqs <= 0 && !inst.loading && matchesDevice(inst.deviceIDs) && idleEnough(inst.idleSince) {
			out = append(out, IdleInstanceInfo{
				Fingerprint: inst.fingerprint,
				ModelPath:   inst.config.Path,
				WorkerID:    inst.workerID,
				IdleSince:   inst.idleSince,
			})
		}
	}
	return out
}

// modelMaxConcurrent returns the max_concurrent placement value for a model by fingerprint.
// Returns 0 if not found.
func (p *modelPool) modelMaxConcurrent(fp string) int32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if inst, ok := p.chatModels[fp]; ok {
		return inst.placement.MaxConcurrent
	}
	if inst, ok := p.embedModels[fp]; ok {
		return inst.placement.MaxConcurrent
	}
	return 0
}

// RegisterChat registers a chat model. mode controls auto-eviction:
//   - ModeStatic: pinned by user action (UI Load Model) — never evicted.
//   - ModeDynamic: loaded by an inference request — eligible for idle-
//     timeout eviction once activeReqs returns to zero.
func (p *modelPool) RegisterChat(mode InstanceMode, source, modelName string, cfg config.LlamaChatConfig, placement config.PlacementConfig, model llm.ChatModelInterface, workerID, workerName string, deviceIDs ...string) string {
	fp := cfg.Fingerprint()
	if actual := model.PoolSize(); actual > 0 {
		placement.MaxConcurrent = actual
	}
	p.mu.Lock()
	p.chatModels[fp] = &chatInstance{
		fingerprint: fp,
		name:        modelName,
		config:      cfg,
		placement:   placement,
		model:       model,
		source:      source,
		mode:        mode,
		workerID:    workerID,
		workerName:  workerName,
		deviceIDs:   deviceIDs,
	}
	p.mu.Unlock()
	p.notifyChange(PoolChangeEvent{Kind: PoolChangeList})
	return fp
}

// RegisterEmbedding registers an embedding model in the pool. See RegisterChat
// for the meaning of mode.
func (p *modelPool) RegisterEmbedding(mode InstanceMode, source, modelName string, cfg config.LlamaEmbeddingConfig, placement config.PlacementConfig, model llm.EmbeddingModelInterface, workerID, workerName string, deviceIDs ...string) string {
	fp := cfg.Fingerprint()
	if actual := model.PoolSize(); actual > 0 {
		placement.MaxConcurrent = actual
	}
	p.mu.Lock()
	p.embedModels[fp] = &embeddingInstance{
		fingerprint: fp,
		name:        modelName,
		config:      cfg,
		placement:   placement,
		model:       model,
		source:      source,
		mode:        mode,
		workerID:    workerID,
		workerName:  workerName,
		deviceIDs:   deviceIDs,
	}
	p.mu.Unlock()
	p.notifyChange(PoolChangeEvent{Kind: PoolChangeList})
	return fp
}

// HasChat returns true if a chat model with the given fingerprint is loaded (or loading).
func (p *modelPool) HasChat(fp string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.chatModels[fp]
	return ok
}

// HasEmbedding returns true if an embedding model with the given fingerprint is loaded (or loading).
func (p *modelPool) HasEmbedding(fp string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.embedModels[fp]
	return ok
}

// AllChatPredictors returns all loaded chat predictors.
func (p *modelPool) AllChatPredictors() map[string]llm.PredictorInterface {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make(map[string]llm.PredictorInterface, len(p.chatModels))
	for fp, inst := range p.chatModels {
		if inst.loading {
			continue
		}
		result[fp] = inst.model.Pool()
	}
	return result
}

// AllEmbedders returns all loaded embedders.
func (p *modelPool) AllEmbedders() map[string]llm.EmbedderInterface {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make(map[string]llm.EmbedderInterface, len(p.embedModels))
	for fp, inst := range p.embedModels {
		if inst.loading {
			continue
		}
		result[fp] = inst.model.Pool()
	}
	return result
}

// LoadedInstanceInfo holds raw metadata + config for a loaded model
// instance, suitable for emitting to API callers who need to address the
// instance from outside MASS (echo the config back in inference requests).
// Unlike [ModelInstanceInfo], the configs are typed structs rather than
// display strings.
type LoadedInstanceInfo struct {
	Fingerprint     string
	Type            llm.ModelKind
	Source          string
	WorkerID        string
	WorkerName      string
	DeviceIDs       []string
	ActiveReqs      int64
	ChatConfig      *config.LlamaChatConfig
	EmbeddingConfig *config.LlamaEmbeddingConfig
	Placement       config.PlacementConfig
}

// LoadedSnapshot returns raw config snapshots for every fully-loaded
// instance. Loading instances are skipped because their config isn't
// finalized yet (placement may not have been computed).
func (p *modelPool) LoadedSnapshot() []LoadedInstanceInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()

	out := make([]LoadedInstanceInfo, 0, len(p.chatModels)+len(p.embedModels))
	for _, inst := range p.chatModels {
		if inst.loading {
			continue
		}
		cfg := inst.config
		out = append(out, LoadedInstanceInfo{
			Fingerprint: inst.fingerprint,
			Type:        llm.ModelKindChat,
			Source:      inst.source,
			WorkerID:    inst.workerID,
			WorkerName:  inst.workerName,
			DeviceIDs:   inst.deviceIDs,
			ActiveReqs:  inst.activeReqs,
			ChatConfig:  &cfg,
			Placement:   inst.placement,
		})
	}
	for _, inst := range p.embedModels {
		if inst.loading {
			continue
		}
		cfg := inst.config
		out = append(out, LoadedInstanceInfo{
			Fingerprint:     inst.fingerprint,
			Type:            llm.ModelKindEmbedding,
			Source:          inst.source,
			WorkerID:        inst.workerID,
			WorkerName:      inst.workerName,
			DeviceIDs:       inst.deviceIDs,
			ActiveReqs:      inst.activeReqs,
			EmbeddingConfig: &cfg,
			Placement:       inst.placement,
		})
	}
	return out
}

// ModelInstanceInfo holds display-ready metadata about a loaded model instance.
type ModelInstanceInfo struct {
	Fingerprint string
	Path        string
	Type        llm.ModelKind
	Source      string       // who loaded it: "direct", "app:<name>"
	Mode        InstanceMode // how it was loaded
	WorkerID    string       // ID of the agent running this model
	WorkerName  string       // human-readable name of the agent
	DeviceIDs   []string     // device(s) the model is loaded on
	ActiveReqs  int64
	Active      bool // true if processing requests or within activeStickyDuration
	Loading     bool // true if the model is currently being loaded into memory
	// Config holds instance configuration as ordered key-value pairs for display.
	Config []ConfigEntry
}

// ConfigEntry is a single key-value pair for display in the UI.
type ConfigEntry struct {
	Key   string
	Value string
}

func chatConfigEntries(c config.LlamaChatConfig, p config.PlacementConfig) []ConfigEntry {
	var entries []ConfigEntry
	if c.ContextSize > 0 {
		entries = append(entries, ConfigEntry{"Context Size", fmt.Sprintf("%d", c.ContextSize)})
	}
	if c.BatchSize > 0 {
		entries = append(entries, ConfigEntry{"Batch Size", fmt.Sprintf("%d", c.BatchSize)})
	}
	if p.GpuLayers != 0 {
		entries = append(entries, ConfigEntry{"GPU Layers", fmt.Sprintf("%d", p.GpuLayers)})
	}
	if p.Threads > 0 {
		entries = append(entries, ConfigEntry{"Threads", fmt.Sprintf("%d", p.Threads)})
	}
	if p.MaxConcurrent > 0 {
		entries = append(entries, ConfigEntry{"Max Concurrent", fmt.Sprintf("%d", p.MaxConcurrent)})
	}
	if c.MaxTokens > 0 {
		entries = append(entries, ConfigEntry{"Max Tokens", fmt.Sprintf("%d", c.MaxTokens)})
	}
	if c.FlashAttn != "" {
		entries = append(entries, ConfigEntry{"Flash Attention", c.FlashAttn})
	}
	if p.MainGPU != "" {
		entries = append(entries, ConfigEntry{"Main GPU", p.MainGPU})
	}
	if p.TensorSplit != "" {
		entries = append(entries, ConfigEntry{"Tensor Split", p.TensorSplit})
	}
	if c.Thinking {
		entries = append(entries, ConfigEntry{"Thinking", "enabled"})
	}
	if c.MmprojPath != "" {
		entries = append(entries, ConfigEntry{"Vision Projector", c.MmprojPath})
	}
	if c.CacheType != "" {
		entries = append(entries, ConfigEntry{"KV Cache Type", c.CacheType})
	}
	if c.ChatTemplate != "" {
		entries = append(entries, ConfigEntry{"Chat Template", c.ChatTemplate})
	}
	return entries
}

func embeddingConfigEntries(c config.LlamaEmbeddingConfig, p config.PlacementConfig) []ConfigEntry {
	var entries []ConfigEntry
	if c.ContextSize > 0 {
		entries = append(entries, ConfigEntry{"Context Size", fmt.Sprintf("%d", c.ContextSize)})
	}
	if p.GpuLayers != 0 {
		entries = append(entries, ConfigEntry{"GPU Layers", fmt.Sprintf("%d", p.GpuLayers)})
	}
	if p.Threads > 0 {
		entries = append(entries, ConfigEntry{"Threads", fmt.Sprintf("%d", p.Threads)})
	}
	if p.MaxConcurrent > 0 {
		entries = append(entries, ConfigEntry{"Max Concurrent", fmt.Sprintf("%d", p.MaxConcurrent)})
	}
	if p.MainGPU != "" {
		entries = append(entries, ConfigEntry{"Main GPU", p.MainGPU})
	}
	if p.TensorSplit != "" {
		entries = append(entries, ConfigEntry{"Tensor Split", p.TensorSplit})
	}
	return entries
}

// Snapshot returns metadata about all loaded model instances.
func (p *modelPool) Snapshot() []ModelInstanceInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()

	now := time.Now()
	out := make([]ModelInstanceInfo, 0, len(p.chatModels)+len(p.embedModels))
	for _, inst := range p.chatModels {
		reqs := inst.activeReqs
		active := reqs > 0 || (!inst.idleSince.IsZero() && now.Sub(inst.idleSince) < activeStickyDuration)
		out = append(out, ModelInstanceInfo{
			Fingerprint: inst.fingerprint,
			Path:        inst.config.Path,
			Type:        llm.ModelKindChat,
			Source:      inst.source,
			Mode:        inst.mode,
			WorkerID:    inst.workerID,
			WorkerName:  inst.workerName,
			DeviceIDs:   inst.deviceIDs,
			ActiveReqs:  reqs,
			Active:      active,
			Loading:     inst.loading,
		})
	}
	for _, inst := range p.embedModels {
		reqs := inst.activeReqs
		active := reqs > 0 || (!inst.idleSince.IsZero() && now.Sub(inst.idleSince) < activeStickyDuration)
		out = append(out, ModelInstanceInfo{
			Fingerprint: inst.fingerprint,
			Path:        inst.config.Path,
			Type:        llm.ModelKindEmbedding,
			Source:      inst.source,
			Mode:        inst.mode,
			WorkerID:    inst.workerID,
			WorkerName:  inst.workerName,
			DeviceIDs:   inst.deviceIDs,
			ActiveReqs:  reqs,
			Active:      active,
			Loading:     inst.loading,
		})
	}
	return out
}

// SnapshotInstance returns metadata for a single instance by fingerprint.
// Returns zero value and false if the fingerprint is not found.
func (p *modelPool) SnapshotInstance(fp string) (ModelInstanceInfo, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	now := time.Now()
	if inst, ok := p.chatModels[fp]; ok {
		reqs := inst.activeReqs
		active := reqs > 0 || (!inst.idleSince.IsZero() && now.Sub(inst.idleSince) < activeStickyDuration)
		info := ModelInstanceInfo{
			Fingerprint: inst.fingerprint,
			Path:        inst.config.Path,
			Type:        llm.ModelKindChat,
			Source:      inst.source,
			Mode:        inst.mode,
			WorkerID:    inst.workerID,
			WorkerName:  inst.workerName,
			DeviceIDs:   inst.deviceIDs,
			ActiveReqs:  reqs,
			Active:      active,
			Loading:     inst.loading,
		}
		if !inst.loading {
			info.Config = chatConfigEntries(inst.config, inst.placement)
		}
		return info, true
	}
	if inst, ok := p.embedModels[fp]; ok {
		reqs := inst.activeReqs
		active := reqs > 0 || (!inst.idleSince.IsZero() && now.Sub(inst.idleSince) < activeStickyDuration)
		info := ModelInstanceInfo{
			Fingerprint: inst.fingerprint,
			Path:        inst.config.Path,
			Type:        llm.ModelKindEmbedding,
			Source:      inst.source,
			Mode:        inst.mode,
			WorkerID:    inst.workerID,
			WorkerName:  inst.workerName,
			DeviceIDs:   inst.deviceIDs,
			ActiveReqs:  reqs,
			Active:      active,
			Loading:     inst.loading,
		}
		if !inst.loading {
			info.Config = embeddingConfigEntries(inst.config, inst.placement)
		}
		return info, true
	}
	return ModelInstanceInfo{}, false
}

// CloseAll stops all idle timers and closes all model instances.
func (p *modelPool) CloseAll() {
	p.mu.Lock()
	had := len(p.chatModels) > 0 || len(p.embedModels) > 0
	for fp, inst := range p.chatModels {
		if inst.idleTimer != nil {
			inst.idleTimer.Stop()
		}
		if !inst.loading {
			inst.model.Close()
		}
		delete(p.chatModels, fp)
	}
	for fp, inst := range p.embedModels {
		if inst.idleTimer != nil {
			inst.idleTimer.Stop()
		}
		if !inst.loading {
			inst.model.Close()
		}
		delete(p.embedModels, fp)
	}
	p.mu.Unlock()
	if had {
		p.notifyChange(PoolChangeEvent{Kind: PoolChangeList})
	}
}
