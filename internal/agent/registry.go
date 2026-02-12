package agent

import (
	"fmt"
	"sync"
)

// RegistryChangeKind describes what changed in the agent registry.
type RegistryChangeKind int

const (
	RegistryChangeAdded RegistryChangeKind = iota
	RegistryChangeRemoved
	RegistryChangeUpdated
)

// RegistryChangeEvent describes a change in the agent registry.
type RegistryChangeEvent struct {
	Kind    RegistryChangeKind
	AgentID string
}

// RegistryChangeCallback is called when the agent registry changes.
type RegistryChangeCallback func(evt RegistryChangeEvent)

// Registry manages the set of known agents.
type Registry struct {
	mu       sync.RWMutex
	agents   map[string]AgentInterface
	order    []string // insertion order for deterministic iteration
	onChange []RegistryChangeCallback
}

// NewRegistry creates a new agent registry.
func NewRegistry() *Registry {
	return &Registry{
		agents: make(map[string]AgentInterface),
	}
}

// Register adds an agent to the registry.
func (r *Registry) Register(a AgentInterface) error {
	r.mu.Lock()
	id := a.ID()
	if _, exists := r.agents[id]; exists {
		r.mu.Unlock()
		return fmt.Errorf("agent %q already registered", id)
	}
	r.agents[id] = a
	r.order = append(r.order, id)
	cbs := r.onChange
	r.mu.Unlock()

	for _, cb := range cbs {
		cb(RegistryChangeEvent{Kind: RegistryChangeAdded, AgentID: id})
	}
	return nil
}

// Deregister removes an agent from the registry.
func (r *Registry) Deregister(id string) error {
	r.mu.Lock()
	if _, exists := r.agents[id]; !exists {
		r.mu.Unlock()
		return fmt.Errorf("agent %q not found", id)
	}
	delete(r.agents, id)
	for i, oid := range r.order {
		if oid == id {
			r.order = append(r.order[:i], r.order[i+1:]...)
			break
		}
	}
	cbs := r.onChange
	r.mu.Unlock()

	for _, cb := range cbs {
		cb(RegistryChangeEvent{Kind: RegistryChangeRemoved, AgentID: id})
	}
	return nil
}

// Get returns the agent with the given ID, or nil if not found.
func (r *Registry) Get(id string) AgentInterface {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.agents[id]
}

// All returns all registered agents in insertion order.
func (r *Registry) All() []AgentInterface {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]AgentInterface, 0, len(r.order))
	for _, id := range r.order {
		if a, ok := r.agents[id]; ok {
			result = append(result, a)
		}
	}
	return result
}

// SelectAgent returns the first online agent. Returns nil if none available.
func (r *Registry) SelectAgent() AgentInterface {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, id := range r.order {
		a := r.agents[id]
		if a.Status().Online {
			return a
		}
	}
	return nil
}

// IsLocal returns true if the agent with the given ID is the local agent.
func (r *Registry) IsLocal(id string) bool {
	return id == "local"
}

// NotifyUpdate fires change callbacks for an agent stats update.
func (r *Registry) NotifyUpdate(id string) {
	r.mu.RLock()
	cbs := r.onChange
	r.mu.RUnlock()
	for _, cb := range cbs {
		cb(RegistryChangeEvent{Kind: RegistryChangeUpdated, AgentID: id})
	}
}

// AddChangeCallback registers a callback for registry changes.
func (r *Registry) AddChangeCallback(cb RegistryChangeCallback) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onChange = append(r.onChange, cb)
}
