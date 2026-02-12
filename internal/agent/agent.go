// Package agent defines the agent abstraction for compute resource management.
// MASS delegates hardware discovery, benchmarking, and model loading to agents.
// The local agent runs in-process; remote agents connect over ConnectRPC.
package agent

import (
	"time"

	"github.com/chinese-room-solutions/mass/pkg/bench"
	"github.com/chinese-room-solutions/mass/pkg/llm"
)

// AgentInterface represents a compute agent that can discover hardware,
// run benchmarks, and load/run inference models.
// It embeds BencherInterface (Devices + Bench) and ModelLoaderInterface.
type AgentInterface interface {
	llm.ModelLoaderInterface
	bench.BencherInterface

	// ID returns the unique identifier of this agent.
	ID() string

	// Name returns a human-readable name for this agent.
	Name() string

	// Status returns the current status of this agent.
	Status() AgentStatus

	// Stats returns live utilization metrics for all devices.
	Stats() []bench.DeviceStats
}

// AgentStatus describes the current state of an agent.
type AgentStatus struct {
	Online   bool
	LastSeen time.Time
}
