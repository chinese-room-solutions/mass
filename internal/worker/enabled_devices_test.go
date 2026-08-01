package worker

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// ComputeEnabledDevices must keep the three whitelist states distinct —
// especially "everything disabled", which the old empty-list-means-all
// wire encoding silently inverted to all-enabled.
func TestComputeEnabledDevices(t *testing.T) {
	advertised := []string{"cpu:0", "gpu:0", "gpu:1"}
	tests := []struct {
		name  string
		state map[string]bool
		want  EnabledDevices
	}{
		{
			name:  "no persisted rows: all",
			state: nil,
			want:  EnabledDevices{All: true},
		},
		{
			name:  "one disabled: exact subset, rowless devices stay enabled",
			state: map[string]bool{"gpu:1": false},
			want:  EnabledDevices{IDs: []string{"cpu:0", "gpu:0"}},
		},
		{
			name:  "explicit enabled rows only: full set, but not All",
			state: map[string]bool{"cpu:0": true, "gpu:0": true, "gpu:1": true},
			want:  EnabledDevices{IDs: []string{"cpu:0", "gpu:0", "gpu:1"}},
		},
		{
			name:  "everything disabled: explicit none, not all",
			state: map[string]bool{"cpu:0": false, "gpu:0": false, "gpu:1": false},
			want:  EnabledDevices{IDs: []string{}},
		},
		{
			name:  "stale row for un-advertised device is ignored",
			state: map[string]bool{"gpu:9": false},
			want:  EnabledDevices{IDs: []string{"cpu:0", "gpu:0", "gpu:1"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ComputeEnabledDevices(advertised, tt.state))
		})
	}
}
