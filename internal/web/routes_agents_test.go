package web

import (
	"testing"

	"github.com/chinese-room-solutions/mass/pkg/bench"
	"github.com/stretchr/testify/require"
)

func TestParseCSV(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want map[string]bool
	}{
		{
			name: "empty string",
			in:   "",
			want: map[string]bool{},
		},
		{
			name: "single value",
			in:   "alpha",
			want: map[string]bool{"alpha": true},
		},
		{
			name: "multiple values",
			in:   "a,b,c",
			want: map[string]bool{"a": true, "b": true, "c": true},
		},
		{
			name: "whitespace trimming",
			in:   " a , b , c ",
			want: map[string]bool{"a": true, "b": true, "c": true},
		},
		{
			name: "empty elements ignored",
			in:   "a,,b, ,c",
			want: map[string]bool{"a": true, "b": true, "c": true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseCSV(tt.in)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestSliceToSet(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want map[string]bool
	}{
		{
			name: "nil slice",
			in:   nil,
			want: map[string]bool{},
		},
		{
			name: "empty slice",
			in:   []string{},
			want: map[string]bool{},
		},
		{
			name: "values with whitespace",
			in:   []string{" x ", "y ", " z"},
			want: map[string]bool{"x": true, "y": true, "z": true},
		},
		{
			name: "empty strings filtered",
			in:   []string{"a", "", " ", "b"},
			want: map[string]bool{"a": true, "b": true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sliceToSet(tt.in)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestBenchmarkTarget_MatchAgent(t *testing.T) {
	tests := []struct {
		name     string
		agentIDs map[string]bool
		id       string
		want     bool
	}{
		{
			name:     "empty AgentIDs matches all",
			agentIDs: nil,
			id:       "any-agent",
			want:     true,
		},
		{
			name:     "empty map matches all",
			agentIDs: map[string]bool{},
			id:       "any-agent",
			want:     true,
		},
		{
			name:     "specific ID matches",
			agentIDs: map[string]bool{"agent-1": true, "agent-2": true},
			id:       "agent-1",
			want:     true,
		},
		{
			name:     "specific ID does not match",
			agentIDs: map[string]bool{"agent-1": true, "agent-2": true},
			id:       "agent-3",
			want:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := benchmarkTarget{AgentIDs: tt.agentIDs}
			require.Equal(t, tt.want, bt.matchAgent(tt.id))
		})
	}
}

func TestBenchmarkTarget_MatchDevice(t *testing.T) {
	tests := []struct {
		name      string
		deviceIDs map[string]bool
		id        string
		want      bool
	}{
		{
			name:      "empty DeviceIDs matches all",
			deviceIDs: nil,
			id:        "gpu:0",
			want:      true,
		},
		{
			name:      "empty map matches all",
			deviceIDs: map[string]bool{},
			id:        "gpu:0",
			want:      true,
		},
		{
			name:      "specific ID matches",
			deviceIDs: map[string]bool{"gpu:0": true, "cpu:0": true},
			id:        "gpu:0",
			want:      true,
		},
		{
			name:      "specific ID does not match",
			deviceIDs: map[string]bool{"gpu:0": true},
			id:        "cpu:0",
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := benchmarkTarget{DeviceIDs: tt.deviceIDs}
			require.Equal(t, tt.want, bt.matchDevice(tt.id))
		})
	}
}

type panicDevices struct{}

func (p panicDevices) Devices() []bench.Device { panic("boom") }

type normalDevices struct{ devs []bench.Device }

func (n normalDevices) Devices() []bench.Device { return n.devs }

func TestSafeDevices(t *testing.T) {
	tests := []struct {
		name string
		ag   interface{ Devices() []bench.Device }
		want []bench.Device
	}{
		{
			name: "normal return",
			ag: normalDevices{devs: []bench.Device{
				{ID: "gpu:0", Name: "Test GPU", Type: "GPU", TotalMemoryMB: 8192},
			}},
			want: []bench.Device{
				{ID: "gpu:0", Name: "Test GPU", Type: "GPU", TotalMemoryMB: 8192},
			},
		},
		{
			name: "panicking implementation returns nil",
			ag:   panicDevices{},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := safeDevices(tt.ag)
			require.Equal(t, tt.want, got)
		})
	}
}
