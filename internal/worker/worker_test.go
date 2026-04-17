package worker

import (
	"fmt"
	"testing"

	"github.com/chinese-room-solutions/mass/pkg/bench"
	"github.com/chinese-room-solutions/mass/pkg/llm"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// --- Fleet tests ---

type fakeWorker struct {
	id     string
	name   string
	online bool
}

func (a *fakeWorker) ID() string              { return a.id }
func (a *fakeWorker) Name() string            { return a.name }
func (a *fakeWorker) Status() WorkerStatus    { return WorkerStatus{Online: a.online} }
func (a *fakeWorker) Devices() []stats.Device { return nil }
func (a *fakeWorker) Bench(_ string) (bench.Result, error) {
	return bench.Result{}, fmt.Errorf("not supported")
}
func (a *fakeWorker) LoadChatModel(_ zerolog.Logger, _ string, _ llm.ChatModelConfigInterface, _ llm.PlacementConfig) (llm.ChatModelInterface, error) {
	return nil, fmt.Errorf("not supported")
}
func (a *fakeWorker) LoadEmbeddingModel(_ zerolog.Logger, _ string, _ llm.EmbeddingModelConfigInterface, _ llm.PlacementConfig) (llm.EmbeddingModelInterface, error) {
	return nil, fmt.Errorf("not supported")
}
func (a *fakeWorker) Stats() []stats.DeviceStats { return nil }

func TestFleet_RegisterAndGet(t *testing.T) {
	reg := NewFleet()
	a := &fakeWorker{id: "a1", name: "Worker 1", online: true}

	require.NoError(t, reg.Register(a))
	require.Equal(t, a, reg.Get("a1"))
	require.Nil(t, reg.Get("nonexistent"))
}

func TestFleet_DuplicateRegister(t *testing.T) {
	reg := NewFleet()
	a := &fakeWorker{id: "a1", name: "Worker 1"}

	require.NoError(t, reg.Register(a))
	require.Error(t, reg.Register(a))
}

func TestFleet_Deregister(t *testing.T) {
	reg := NewFleet()
	a := &fakeWorker{id: "a1", name: "Worker 1"}

	require.NoError(t, reg.Register(a))
	require.NoError(t, reg.Deregister("a1"))
	require.Nil(t, reg.Get("a1"))
	require.Empty(t, reg.All())
}

func TestFleet_DeregisterNotFound(t *testing.T) {
	reg := NewFleet()
	require.Error(t, reg.Deregister("nonexistent"))
}

func TestFleet_All_InsertionOrder(t *testing.T) {
	reg := NewFleet()
	a1 := &fakeWorker{id: "a1", name: "First"}
	a2 := &fakeWorker{id: "a2", name: "Second"}
	a3 := &fakeWorker{id: "a3", name: "Third"}

	require.NoError(t, reg.Register(a1))
	require.NoError(t, reg.Register(a2))
	require.NoError(t, reg.Register(a3))

	all := reg.All()
	require.Len(t, all, 3)
	require.Equal(t, "a1", all[0].ID())
	require.Equal(t, "a2", all[1].ID())
	require.Equal(t, "a3", all[2].ID())
}

func TestFleet_SelectWorker(t *testing.T) {
	tests := []struct {
		name    string
		workers []*fakeWorker
		wantID  string
	}{
		{
			"selects first online worker",
			[]*fakeWorker{
				{id: "a1", online: false},
				{id: "a2", online: true},
				{id: "a3", online: true},
			},
			"a2",
		},
		{
			"no online workers",
			[]*fakeWorker{
				{id: "a1", online: false},
			},
			"",
		},
		{
			"empty fleet",
			nil,
			"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := NewFleet()
			for _, a := range tt.workers {
				require.NoError(t, reg.Register(a))
			}
			selected := reg.SelectWorker()
			if tt.wantID == "" {
				require.Nil(t, selected)
			} else {
				require.NotNil(t, selected)
				require.Equal(t, tt.wantID, selected.ID())
			}
		})
	}
}

func TestFleet_ChangeCallback(t *testing.T) {
	reg := NewFleet()
	var events []FleetChangeEvent
	reg.AddChangeCallback(func(evt FleetChangeEvent) {
		events = append(events, evt)
	})

	a := &fakeWorker{id: "a1", name: "Worker 1"}
	require.NoError(t, reg.Register(a))
	require.Len(t, events, 1)
	require.Equal(t, FleetChangeAdded, events[0].Kind)
	require.Equal(t, "a1", events[0].WorkerID)

	require.NoError(t, reg.Deregister("a1"))
	require.Len(t, events, 2)
	require.Equal(t, FleetChangeRemoved, events[1].Kind)
	require.Equal(t, "a1", events[1].WorkerID)
}
