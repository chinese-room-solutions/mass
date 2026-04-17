package apps

import (
	"context"

	"github.com/chinese-room-solutions/mass/internal/config"
)

// AppRuntimeInterface abstracts the app execution environment. Default:
// Manager (bare process via go-plugin); future: Docker, K8s, etc.
//
// One runtime instance per app — created on launch, dropped on stop;
// [Shutdown] terminates that app's process. After load, all
// implementations communicate over gRPC (AppInterface) regardless of how
// the process was started.
type AppRuntimeInterface interface {
	// LoadApp starts an app and establishes a gRPC connection to it.
	LoadApp(ctx context.Context, conf config.AppConfig) error

	// GetApp returns a loaded app by name, or nil if not found.
	GetApp(name string) *LoadedApp

	// Shutdown terminates the managed app's process.
	Shutdown()

	// SetLogCallback registers a function called for each app log line.
	SetLogCallback(fn func(name, line string))

	// SetExtraEnv sets additional environment variables (or equivalent
	// configuration) for app processes.
	SetExtraEnv(env []string)
}
