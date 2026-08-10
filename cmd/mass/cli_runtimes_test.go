package main

import (
	"testing"

	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
	"github.com/stretchr/testify/require"
)

// The index appends newest-last, so reading the first entry reported an
// outdated LATEST as soon as a package had more than one version.
func TestLatestInstallable(t *testing.T) {
	tests := []struct {
		name            string
		versions        []*rpc.PackageVersion
		wantLatest      string
		wantInstallable bool
	}{
		{name: "none"},
		{
			name:            "single",
			versions:        []*rpc.PackageVersion{{Version: "0.1.0", HasArtifact: true}},
			wantLatest:      "0.1.0",
			wantInstallable: true,
		},
		{
			name: "newest is the last entry",
			versions: []*rpc.PackageVersion{
				{Version: "0.1.0", HasArtifact: true},
				{Version: "0.2.0", HasArtifact: true},
			},
			wantLatest:      "0.2.0",
			wantInstallable: true,
		},
		{
			name: "installable when any version has an artifact",
			versions: []*rpc.PackageVersion{
				{Version: "0.1.0", HasArtifact: true},
				{Version: "0.2.0", HasArtifact: false},
			},
			wantLatest:      "0.2.0",
			wantInstallable: true,
		},
		{
			name: "no artifact anywhere",
			versions: []*rpc.PackageVersion{
				{Version: "0.1.0", HasArtifact: false},
				{Version: "0.2.0", HasArtifact: false},
			},
			wantLatest:      "0.2.0",
			wantInstallable: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			latest, installable := latestInstallable(tt.versions)
			require.Equal(t, tt.wantLatest, latest)
			require.Equal(t, tt.wantInstallable, installable)
		})
	}
}
