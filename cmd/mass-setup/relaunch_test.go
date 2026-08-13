package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass-sdk/install"
	"github.com/stretchr/testify/require"
)

// installArgs must carry --relaunch through to the elevated child, or a
// self-update that needs admin rights would stage the new build and never
// bring MASS back.
func TestInstallArgsRelaunch(t *testing.T) {
	tests := []struct {
		name     string
		relaunch bool
	}{
		{"a plain install passes no relaunch", false},
		{"a self-update install passes it on", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := collected{
				scope:      install.ScopeUser,
				installDir: filepath.Join(t.TempDir(), "app"),
				dataDir:    filepath.Join(t.TempDir(), "data"),
				listenAddr: "127.0.0.1:3455",
				relaunch:   tt.relaunch,
			}
			args := installArgs(c)
			require.Equal(t, "--install", args[0])
			require.Equal(t, tt.relaunch, args[len(args)-1] == "--relaunch")
		})
	}
}

// A path nothing is running from is replaceable at once, so the wait returns
// immediately rather than burning its whole budget on a first install.
func TestWaitReplaceableReturnsForAMissingPath(t *testing.T) {
	start := time.Now()
	waitReplaceable(filepath.Join(t.TempDir(), "absent"), 5*time.Second)
	require.Less(t, time.Since(start), time.Second)
}
