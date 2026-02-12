package llm

import (
	"os"
	"sync"

	"github.com/rs/zerolog"
	llama "github.com/tcpipuk/llama-go"
)

var initLogOnce sync.Once

// zerologToLlamaLog maps zerolog's global level to a LLAMA_LOG value so the
// C-side filter matches what zerolog would show.
func zerologToLlamaLog() string {
	switch zerolog.GlobalLevel() {
	case zerolog.TraceLevel, zerolog.DebugLevel:
		return "debug"
	case zerolog.InfoLevel:
		return "info"
	case zerolog.WarnLevel:
		return "warn"
	case zerolog.ErrorLevel, zerolog.FatalLevel, zerolog.PanicLevel:
		return "error"
	case zerolog.Disabled:
		return "none"
	default:
		return "info"
	}
}

// initLlamaLogging configures llama-go to route all llama.cpp log messages
// through the provided zerolog logger. Safe to call multiple times; only the
// first call takes effect.
//
// Maps zerolog's global level to LLAMA_LOG so the C-side filter matches.
func initLlamaLogging(logger zerolog.Logger) {
	initLogOnce.Do(func() {
		_ = os.Setenv("LLAMA_LOG", zerologToLlamaLog())
		llama.InitLogging()

		ll := logger.With().Str("component", "llama.cpp").Logger()
		llama.SetLogCallback(func(level llama.LogLevel, message string) {
			switch level {
			case llama.LogLevelDebug:
				ll.Debug().Msg(message)
			case llama.LogLevelInfo:
				ll.Info().Msg(message)
			case llama.LogLevelWarn:
				ll.Warn().Msg(message)
			case llama.LogLevelError:
				ll.Error().Msg(message)
			}
		})
	})
}
