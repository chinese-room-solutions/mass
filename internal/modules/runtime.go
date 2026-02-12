package modules

import (
	"context"

	"github.com/chinese-room-solutions/mass/internal/config"
)

// ModuleRuntimeInterface abstracts the module execution environment.
// The default implementation is Manager (bare process via go-plugin).
// Future implementations may run modules as Docker containers or K8s pods.
//
// All implementations share the same post-connection behavior: once a module
// is loaded, communication happens over gRPC (ModuleInterface) regardless of
// how the process was started.
type ModuleRuntimeInterface interface {
	// LoadModule starts a module and establishes a gRPC connection to it.
	LoadModule(ctx context.Context, conf config.ModuleConfig) error

	// GetModule returns a loaded module by name, or nil if not found.
	GetModule(name string) *LoadedModule

	// Modules returns all loaded modules.
	Modules() []*LoadedModule

	// UnloadModule stops and removes a single module by name.
	UnloadModule(name string) error

	// Shutdown stops all managed modules.
	Shutdown()

	// SetLogCallback registers a function called for each module log line.
	SetLogCallback(fn func(name, line string))

	// SetExtraEnv sets additional environment variables (or equivalent
	// configuration) for module processes.
	SetExtraEnv(env []string)
}
