package scheduler

import (
	"fmt"
	"sync"
	"time"

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/llm"
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
	// ModeStatic means the model was explicitly loaded by a user or module.
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
	config      config.ChatModelConfig
	placement   config.PlacementConfig // placement used at load time
	model       llm.ChatModelInterface
	source      string       // who loaded it: "direct", "module:<name>"
	mode        InstanceMode // how it was loaded
	agentID     string       // ID of the agent running this model
	agentName   string       // human-readable name of the agent
	deviceIDs   []string     // device(s) the model is loaded on
	activeReqs  int64
	idleTimer   *time.Timer
	idleSince   time.Time // when activeReqs last reached zero (zero value = never been active)
	loading     bool
	loadDone    chan struct{} // closed when loading finishes
	loadErr     error         // non-nil if loading failed
}

// embeddingInstance holds a loaded embedding model and its metadata.
type embeddingInstance struct {
	fingerprint string
	name        string // model requirement name (e.g. "embedding"); empty for dynamic loads
	config      config.EmbeddingModelConfig
	placement   config.PlacementConfig // placement used at load time
	model       llm.EmbeddingModelInterface
	source      string       // who loaded it: "direct", "module:<name>"
	mode        InstanceMode // how it was loaded
	agentID     string       // ID of the agent running this model
	agentName   string       // human-readable name of the agent
	deviceIDs   []string     // device(s) the model is loaded on
	activeReqs  int64
	idleTimer   *time.Timer
	idleSince   time.Time // when activeReqs last reached zero (zero value = never been active)
	loading     bool
	loadDone    chan struct{} // closed when loading finishes
	loadErr     error         // non-nil if loading failed
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
func (p *modelPool) GetOrLoadChat(cfg config.ChatModelConfig, placement config.PlacementConfig, source string) (llm.PredictorInterface, string, error) {
	fp := config.ChatModelFingerprint(cfg)

	for {
		p.mu.RLock()
		inst, ok := p.chatModels[fp]
		if ok && !inst.loading {
			p.mu.RUnlock()
			p.acquireChat(inst)
			p.notifyChange(PoolChangeEvent{Kind: PoolChangeStatus, Fingerprint: fp})
			return inst.model.Pool(), fp, nil
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
				p.acquireChat(inst)
				p.notifyChange(PoolChangeEvent{Kind: PoolChangeStatus, Fingerprint: fp})
				return inst.model.Pool(), fp, nil
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
				p.acquireChatLocked(inst)
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
			agentID:     p.loaderID,
			agentName:   p.loaderName,
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
func (p *modelPool) GetOrLoadEmbedding(cfg config.EmbeddingModelConfig, placement config.PlacementConfig, source string) (llm.EmbedderInterface, string, error) {
	fp := config.EmbeddingModelFingerprint(cfg)

	for {
		p.mu.RLock()
		inst, ok := p.embedModels[fp]
		if ok && !inst.loading {
			p.mu.RUnlock()
			p.acquireEmbedding(inst)
			p.notifyChange(PoolChangeEvent{Kind: PoolChangeStatus, Fingerprint: fp})
			return inst.model.Pool(), fp, nil
		}
		if ok && inst.loading {
			done := inst.loadDone
			p.mu.RUnlock()
			<-done
			p.mu.RLock()
			inst, ok = p.embedModels[fp]
			p.mu.RUnlock()
			if ok && !inst.loading {
				p.acquireEmbedding(inst)
				p.notifyChange(PoolChangeEvent{Kind: PoolChangeStatus, Fingerprint: fp})
				return inst.model.Pool(), fp, nil
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
				p.acquireEmbeddingLocked(inst)
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
			agentID:     p.loaderID,
			agentName:   p.loaderName,
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

// Evict forcefully removes a loaded instance by fingerprint.
// Returns true if the instance was found and evicted.
// Instances that are still loading cannot be evicted (returns false).
// Evict forcefully removes a loaded model instance by fingerprint.
// Loading instances cannot be evicted. Returns true if the instance was found and evicted.
func (p *modelPool) Evict(fp string) bool {
	evicted := false

	p.mu.Lock()
	if inst, ok := p.chatModels[fp]; ok {
		if !inst.loading {
			if inst.idleTimer != nil {
				inst.idleTimer.Stop()
			}
			inst.model.Close()
			delete(p.chatModels, fp)
			p.logger.Info().Str("fingerprint", fp).Str("path", inst.config.Path).Msg("chat model evicted by user")
			evicted = true
		}
	} else if inst, ok := p.embedModels[fp]; ok {
		if !inst.loading {
			if inst.idleTimer != nil {
				inst.idleTimer.Stop()
			}
			inst.model.Close()
			delete(p.embedModels, fp)
			p.logger.Info().Str("fingerprint", fp).Str("path", inst.config.Path).Msg("embedding model evicted by user")
			evicted = true
		}
	}
	p.mu.Unlock()

	if evicted {
		p.notifyChange(PoolChangeEvent{Kind: PoolChangeList})
	}
	return evicted
}

// evict closes and removes an idle instance.
func (p *modelPool) evict(fp string) {
	evicted := false

	p.mu.Lock()
	if inst, ok := p.chatModels[fp]; ok {
		if inst.activeReqs <= 0 && inst.mode == ModeDynamic && !inst.loading {
			inst.model.Close()
			delete(p.chatModels, fp)
			p.logger.Info().Str("fingerprint", fp).Str("path", inst.config.Path).Msg("chat model evicted after idle timeout")
			evicted = true
		}
	} else if inst, ok := p.embedModels[fp]; ok {
		if inst.activeReqs <= 0 && inst.mode == ModeDynamic && !inst.loading {
			inst.model.Close()
			delete(p.embedModels, fp)
			p.logger.Info().Str("fingerprint", fp).Str("path", inst.config.Path).Msg("embedding model evicted after idle timeout")
			evicted = true
		}
	}
	p.mu.Unlock()

	if evicted {
		p.notifyChange(PoolChangeEvent{Kind: PoolChangeList})
	}
}

// acquireChat increments activeReqs and cancels any idle timer (caller holds no lock).
func (p *modelPool) acquireChat(inst *chatInstance) {
	p.mu.Lock()
	p.acquireChatLocked(inst)
	p.mu.Unlock()
}

// acquireChatLocked increments activeReqs and cancels any idle timer (caller holds write lock).
func (p *modelPool) acquireChatLocked(inst *chatInstance) {
	if inst.idleTimer != nil {
		inst.idleTimer.Stop()
		inst.idleTimer = nil
	}
	inst.idleSince = time.Time{}
	inst.activeReqs++
}

// acquireEmbedding increments activeReqs and cancels any idle timer (caller holds no lock).
func (p *modelPool) acquireEmbedding(inst *embeddingInstance) {
	p.mu.Lock()
	p.acquireEmbeddingLocked(inst)
	p.mu.Unlock()
}

// acquireEmbeddingLocked increments activeReqs and cancels any idle timer (caller holds write lock).
func (p *modelPool) acquireEmbeddingLocked(inst *embeddingInstance) {
	if inst.idleTimer != nil {
		inst.idleTimer.Stop()
		inst.idleTimer = nil
	}
	inst.idleSince = time.Time{}
	inst.activeReqs++
}

// AcquireChat looks up a loaded chat model by fingerprint, increments its
// active request count, and returns its predictor. Returns false if the
// fingerprint is not found or is still loading.
func (p *modelPool) AcquireChat(fp string) (llm.PredictorInterface, string, bool) {
	p.mu.Lock()
	inst, ok := p.chatModels[fp]
	if !ok || inst.loading {
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
// fingerprint is not found or is still loading.
func (p *modelPool) AcquireEmbedding(fp string) (llm.EmbedderInterface, string, bool) {
	p.mu.Lock()
	inst, ok := p.embedModels[fp]
	if !ok || inst.loading {
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
	AgentID     string
	IdleSince   time.Time
}

// IdleInstancesOnDevice returns idle (activeReqs == 0, not loading) instances
// running on the specified agent and device(s). Used by the dispatcher to find
// eviction candidates for a specific device queue.
func (p *modelPool) IdleInstancesOnDevice(agentID string, deviceIDs []string) []IdleInstanceInfo {
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

	var out []IdleInstanceInfo
	for _, inst := range p.chatModels {
		if inst.agentID == agentID && inst.activeReqs <= 0 && !inst.loading && matchesDevice(inst.deviceIDs) {
			out = append(out, IdleInstanceInfo{
				Fingerprint: inst.fingerprint,
				ModelPath:   inst.config.Path,
				AgentID:     inst.agentID,
				IdleSince:   inst.idleSince,
			})
		}
	}
	for _, inst := range p.embedModels {
		if inst.agentID == agentID && inst.activeReqs <= 0 && !inst.loading && matchesDevice(inst.deviceIDs) {
			out = append(out, IdleInstanceInfo{
				Fingerprint: inst.fingerprint,
				ModelPath:   inst.config.Path,
				AgentID:     inst.agentID,
				IdleSince:   inst.idleSince,
			})
		}
	}
	return out
}

// modelPath returns the file path for a model by fingerprint, or empty string if not found.
// It checks both chat and embedding models.
func (p *modelPool) modelPath(fp string) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if inst, ok := p.chatModels[fp]; ok {
		return inst.config.Path
	}
	if inst, ok := p.embedModels[fp]; ok {
		return inst.config.Path
	}
	return ""
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

// modelContextSize returns the context size for a model by fingerprint, or 0 if not found.
func (p *modelPool) modelContextSize(fp string) int32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if inst, ok := p.chatModels[fp]; ok {
		return inst.config.ContextSize
	}
	if inst, ok := p.embedModels[fp]; ok {
		return inst.config.ContextSize
	}
	return 0
}

// modelCacheType returns the KV cache type for a model by fingerprint, or "" if not found.
func (p *modelPool) modelCacheType(fp string) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if inst, ok := p.chatModels[fp]; ok {
		return inst.config.CacheType
	}
	return "" // embedding models don't have KV cache type
}

// modelMmprojPath returns the vision projector path for a chat model, or "" if none.
func (p *modelPool) modelMmprojPath(fp string) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if inst, ok := p.chatModels[fp]; ok {
		return inst.config.MmprojPath
	}
	return ""
}

// RegisterChat registers a user-loaded static chat model in the pool.
func (p *modelPool) RegisterChat(source, modelName string, cfg config.ChatModelConfig, placement config.PlacementConfig, model llm.ChatModelInterface, agentID, agentName string, deviceIDs ...string) string {
	fp := config.ChatModelFingerprint(cfg)
	p.mu.Lock()
	p.chatModels[fp] = &chatInstance{
		fingerprint: fp,
		name:        modelName,
		config:      cfg,
		placement:   placement,
		model:       model,
		source:      source,
		mode:        ModeStatic,
		agentID:     agentID,
		agentName:   agentName,
		deviceIDs:   deviceIDs,
	}
	p.mu.Unlock()
	p.notifyChange(PoolChangeEvent{Kind: PoolChangeList})
	return fp
}

// RegisterEmbedding registers a user-loaded static embedding model in the pool.
func (p *modelPool) RegisterEmbedding(source, modelName string, cfg config.EmbeddingModelConfig, placement config.PlacementConfig, model llm.EmbeddingModelInterface, agentID, agentName string, deviceIDs ...string) string {
	fp := config.EmbeddingModelFingerprint(cfg)
	p.mu.Lock()
	p.embedModels[fp] = &embeddingInstance{
		fingerprint: fp,
		name:        modelName,
		config:      cfg,
		placement:   placement,
		model:       model,
		source:      source,
		mode:        ModeStatic,
		agentID:     agentID,
		agentName:   agentName,
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

// LookupChatByName finds a chat model by its model requirement name.
func (p *modelPool) LookupChatByName(name string) (llm.PredictorInterface, string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, inst := range p.chatModels {
		if inst.loading {
			continue
		}
		if inst.name == name {
			return inst.model.Pool(), inst.fingerprint, true
		}
	}
	return nil, "", false
}

// LookupEmbeddingByName finds an embedding model by its model requirement name.
func (p *modelPool) LookupEmbeddingByName(name string) (llm.EmbedderInterface, string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, inst := range p.embedModels {
		if inst.loading {
			continue
		}
		if inst.name == name {
			return inst.model.Pool(), inst.fingerprint, true
		}
	}
	return nil, "", false
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

// ModelInstanceInfo holds display-ready metadata about a loaded model instance.
type ModelInstanceInfo struct {
	Fingerprint string
	Path        string
	Type        string       // "chat" or "embedding"
	Source      string       // who loaded it: "direct", "module:<name>"
	Mode        InstanceMode // how it was loaded
	AgentID     string       // ID of the agent running this model
	AgentName   string       // human-readable name of the agent
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

func chatConfigEntries(c config.ChatModelConfig, p config.PlacementConfig) []ConfigEntry {
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

func embeddingConfigEntries(c config.EmbeddingModelConfig, p config.PlacementConfig) []ConfigEntry {
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
			Type:        "chat",
			Source:      inst.source,
			Mode:        inst.mode,
			AgentID:     inst.agentID,
			AgentName:   inst.agentName,
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
			Type:        "embedding",
			Source:      inst.source,
			Mode:        inst.mode,
			AgentID:     inst.agentID,
			AgentName:   inst.agentName,
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
			Type:        "chat",
			Source:      inst.source,
			Mode:        inst.mode,
			AgentID:     inst.agentID,
			AgentName:   inst.agentName,
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
			Type:        "embedding",
			Source:      inst.source,
			Mode:        inst.mode,
			AgentID:     inst.agentID,
			AgentName:   inst.agentName,
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
