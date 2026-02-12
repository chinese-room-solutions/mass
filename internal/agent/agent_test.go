package agent

import (
	"context"
	"fmt"
	"testing"

	"github.com/chinese-room-solutions/mass/pkg/bench"
	"github.com/chinese-room-solutions/mass/pkg/llm"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// --- stubs ---

type stubLoader struct {
	loadChatCalled  int
	loadEmbedCalled int
}

func (l *stubLoader) LoadChatModel(_ zerolog.Logger, _ string, _ llm.ChatModelConfig, _ llm.PlacementConfig) (llm.ChatModelInterface, error) {
	l.loadChatCalled++
	return &stubChatModel{}, nil
}

func (l *stubLoader) LoadEmbeddingModel(_ zerolog.Logger, _ string, _ llm.EmbeddingModelConfig, _ llm.PlacementConfig) (llm.EmbeddingModelInterface, error) {
	l.loadEmbedCalled++
	return &stubEmbedModel{}, nil
}

type stubChatModel struct{}

func (m *stubChatModel) Pool() llm.PredictorInterface { return &stubPredictor{} }
func (m *stubChatModel) Close()                       {}

type stubEmbedModel struct{}

func (m *stubEmbedModel) Pool() llm.EmbedderInterface { return nil }
func (m *stubEmbedModel) Close()                      {}

type stubPredictor struct{}

func (p *stubPredictor) Submit(_ context.Context, _ llm.CompletionRequest) llm.CompletionResult {
	return llm.CompletionResult{Text: "ok"}
}
func (p *stubPredictor) SubmitStream(_ context.Context, _ llm.CompletionRequest) (<-chan llm.CompletionDelta, <-chan error) {
	return nil, nil
}
func (p *stubPredictor) Tokenize(_ context.Context, _ string) ([]int32, error) { return nil, nil }
func (p *stubPredictor) Name() string                                          { return "stub" }

type stubBencher struct {
	devices []bench.Device
}

func (b *stubBencher) Devices() []bench.Device { return b.devices }
func (b *stubBencher) Bench(deviceID string) (bench.Result, error) {
	return bench.Result{DeviceID: deviceID, MemoryGBs: 10.0, ComputeGFlops: 5.0}, nil
}

// --- LocalAgent tests ---

func TestLocalAgent_IDAndName(t *testing.T) {
	a := NewLocalAgent(&stubLoader{}, &stubBencher{})
	require.Equal(t, "local", a.ID())
	require.Equal(t, "Local", a.Name())
}

func TestLocalAgent_Status(t *testing.T) {
	a := NewLocalAgent(&stubLoader{}, &stubBencher{})
	status := a.Status()
	require.True(t, status.Online)
	require.False(t, status.LastSeen.IsZero())
}

func TestLocalAgent_Devices(t *testing.T) {
	tests := []struct {
		name    string
		devices []bench.Device
		want    int
	}{
		{"no devices", nil, 0},
		{"one device", []bench.Device{{ID: "cpu:0", Type: "CPU"}}, 1},
		{"two devices", []bench.Device{{ID: "cpu:0"}, {ID: "gpu:0"}}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewLocalAgent(&stubLoader{}, &stubBencher{devices: tt.devices})
			require.Len(t, a.Devices(), tt.want)
		})
	}
}

func TestLocalAgent_NilBencher(t *testing.T) {
	a := NewLocalAgent(&stubLoader{}, nil)
	require.Nil(t, a.Devices())
}

func TestLocalAgent_Bench(t *testing.T) {
	a := NewLocalAgent(&stubLoader{}, &stubBencher{})
	res, err := a.Bench("cpu:0")
	require.NoError(t, err)
	require.Equal(t, "cpu:0", res.DeviceID)
	require.Equal(t, 10.0, res.MemoryGBs)
}

func TestLocalAgent_LoadChatModel(t *testing.T) {
	loader := &stubLoader{}
	a := NewLocalAgent(loader, &stubBencher{})
	model, err := a.LoadChatModel(zerolog.Nop(), "test", llm.ChatModelConfig{Path: "/test.gguf"}, llm.PlacementConfig{})
	require.NoError(t, err)
	require.NotNil(t, model)
	require.Equal(t, 1, loader.loadChatCalled)
}

func TestLocalAgent_LoadEmbeddingModel(t *testing.T) {
	loader := &stubLoader{}
	a := NewLocalAgent(loader, &stubBencher{})
	model, err := a.LoadEmbeddingModel(zerolog.Nop(), "test", llm.EmbeddingModelConfig{Path: "/test.gguf"}, llm.PlacementConfig{})
	require.NoError(t, err)
	require.NotNil(t, model)
	require.Equal(t, 1, loader.loadEmbedCalled)
}

// --- Registry tests ---

type fakeAgent struct {
	id     string
	name   string
	online bool
}

func (a *fakeAgent) ID() string              { return a.id }
func (a *fakeAgent) Name() string            { return a.name }
func (a *fakeAgent) Status() AgentStatus     { return AgentStatus{Online: a.online} }
func (a *fakeAgent) Devices() []bench.Device { return nil }
func (a *fakeAgent) Bench(_ string) (bench.Result, error) {
	return bench.Result{}, fmt.Errorf("not supported")
}
func (a *fakeAgent) LoadChatModel(_ zerolog.Logger, _ string, _ llm.ChatModelConfig, _ llm.PlacementConfig) (llm.ChatModelInterface, error) {
	return nil, fmt.Errorf("not supported")
}
func (a *fakeAgent) LoadEmbeddingModel(_ zerolog.Logger, _ string, _ llm.EmbeddingModelConfig, _ llm.PlacementConfig) (llm.EmbeddingModelInterface, error) {
	return nil, fmt.Errorf("not supported")
}
func (a *fakeAgent) Stats() []bench.DeviceStats { return nil }

func TestRegistry_RegisterAndGet(t *testing.T) {
	reg := NewRegistry()
	a := &fakeAgent{id: "a1", name: "Agent 1", online: true}

	require.NoError(t, reg.Register(a))
	require.Equal(t, a, reg.Get("a1"))
	require.Nil(t, reg.Get("nonexistent"))
}

func TestRegistry_DuplicateRegister(t *testing.T) {
	reg := NewRegistry()
	a := &fakeAgent{id: "a1", name: "Agent 1"}

	require.NoError(t, reg.Register(a))
	require.Error(t, reg.Register(a))
}

func TestRegistry_Deregister(t *testing.T) {
	reg := NewRegistry()
	a := &fakeAgent{id: "a1", name: "Agent 1"}

	require.NoError(t, reg.Register(a))
	require.NoError(t, reg.Deregister("a1"))
	require.Nil(t, reg.Get("a1"))
	require.Empty(t, reg.All())
}

func TestRegistry_DeregisterNotFound(t *testing.T) {
	reg := NewRegistry()
	require.Error(t, reg.Deregister("nonexistent"))
}

func TestRegistry_All_InsertionOrder(t *testing.T) {
	reg := NewRegistry()
	a1 := &fakeAgent{id: "a1", name: "First"}
	a2 := &fakeAgent{id: "a2", name: "Second"}
	a3 := &fakeAgent{id: "a3", name: "Third"}

	require.NoError(t, reg.Register(a1))
	require.NoError(t, reg.Register(a2))
	require.NoError(t, reg.Register(a3))

	all := reg.All()
	require.Len(t, all, 3)
	require.Equal(t, "a1", all[0].ID())
	require.Equal(t, "a2", all[1].ID())
	require.Equal(t, "a3", all[2].ID())
}

func TestRegistry_SelectAgent(t *testing.T) {
	tests := []struct {
		name   string
		agents []*fakeAgent
		wantID string
	}{
		{
			"selects first online agent",
			[]*fakeAgent{
				{id: "a1", online: false},
				{id: "a2", online: true},
				{id: "a3", online: true},
			},
			"a2",
		},
		{
			"no online agents",
			[]*fakeAgent{
				{id: "a1", online: false},
			},
			"",
		},
		{
			"empty registry",
			nil,
			"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := NewRegistry()
			for _, a := range tt.agents {
				require.NoError(t, reg.Register(a))
			}
			selected := reg.SelectAgent()
			if tt.wantID == "" {
				require.Nil(t, selected)
			} else {
				require.NotNil(t, selected)
				require.Equal(t, tt.wantID, selected.ID())
			}
		})
	}
}

func TestRegistry_ChangeCallback(t *testing.T) {
	reg := NewRegistry()
	var events []RegistryChangeEvent
	reg.AddChangeCallback(func(evt RegistryChangeEvent) {
		events = append(events, evt)
	})

	a := &fakeAgent{id: "a1", name: "Agent 1"}
	require.NoError(t, reg.Register(a))
	require.Len(t, events, 1)
	require.Equal(t, RegistryChangeAdded, events[0].Kind)
	require.Equal(t, "a1", events[0].AgentID)

	require.NoError(t, reg.Deregister("a1"))
	require.Len(t, events, 2)
	require.Equal(t, RegistryChangeRemoved, events[1].Kind)
	require.Equal(t, "a1", events[1].AgentID)
}
