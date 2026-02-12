package agent

import (
	"time"

	"github.com/chinese-room-solutions/mass/pkg/bench"
	"github.com/chinese-room-solutions/mass/pkg/llm"
	"github.com/rs/zerolog"
)

// Compile-time check.
var _ AgentInterface = (*LocalAgent)(nil)

// LocalAgent is the built-in agent that runs in the same process as MASS.
// It delegates model loading to a ModelLoaderInterface (e.g., LlamaLoader)
// and hardware discovery to a BencherInterface.
type LocalAgent struct {
	loader        llm.ModelLoaderInterface
	bencher       bench.BencherInterface
	statsProvider bench.StatsProviderInterface
}

// NewLocalAgent creates a local agent with the given loader and bencher.
// If bencher also implements StatsProviderInterface, it is used for live stats.
func NewLocalAgent(loader llm.ModelLoaderInterface, bencher bench.BencherInterface) *LocalAgent {
	a := &LocalAgent{
		loader:  loader,
		bencher: bencher,
	}
	if sp, ok := bencher.(bench.StatsProviderInterface); ok {
		a.statsProvider = sp
	}
	return a
}

func (a *LocalAgent) ID() string   { return "local" }
func (a *LocalAgent) Name() string { return "Local" }

func (a *LocalAgent) Status() AgentStatus {
	return AgentStatus{Online: true, LastSeen: time.Now()}
}

func (a *LocalAgent) Stats() []bench.DeviceStats {
	if a.statsProvider != nil {
		return a.statsProvider.Stats()
	}
	return []bench.DeviceStats{bench.CPUStats()}
}

func (a *LocalAgent) Devices() []bench.Device {
	if a.bencher == nil {
		return nil
	}
	return a.bencher.Devices()
}

func (a *LocalAgent) Bench(deviceID string) (bench.Result, error) {
	return a.bencher.Bench(deviceID)
}

func (a *LocalAgent) LoadChatModel(logger zerolog.Logger, name string, cfg llm.ChatModelConfig, placement llm.PlacementConfig) (llm.ChatModelInterface, error) {
	return a.loader.LoadChatModel(logger, name, cfg, placement)
}

func (a *LocalAgent) LoadEmbeddingModel(logger zerolog.Logger, name string, cfg llm.EmbeddingModelConfig, placement llm.PlacementConfig) (llm.EmbeddingModelInterface, error) {
	return a.loader.LoadEmbeddingModel(logger, name, cfg, placement)
}
