// Package bench re-exports public bench types from pkg/bench and provides
// internal helpers. All type definitions live in pkg/bench.
package bench

import (
	pkgbench "github.com/chinese-room-solutions/mass/pkg/bench"
)

// Re-export public types so existing internal code keeps compiling.
type Device = pkgbench.Device
type Result = pkgbench.Result
type DeviceStats = pkgbench.DeviceStats
type BencherInterface = pkgbench.BencherInterface
type StatsProviderInterface = pkgbench.StatsProviderInterface

// Re-export public functions.
var (
	RunAll   = pkgbench.RunAll
	RunCPU   = pkgbench.RunCPU
	CPUInfo  = pkgbench.CPUInfo
	CPUStats = pkgbench.CPUStats
)
