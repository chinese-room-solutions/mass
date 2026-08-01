package web

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// firstPathOverlap is the residency guard's byte-level matcher: a delete is
// refused when a doomed store-relative path exactly equals, contains, or is
// contained by any path a queued job still needs.
func TestFirstPathOverlap(t *testing.T) {
	set := func(keys ...string) map[string]struct{} {
		m := make(map[string]struct{}, len(keys))
		for _, k := range keys {
			m[k] = struct{}{}
		}
		return m
	}
	tests := []struct {
		name   string
		doomed []string
		queued map[string]struct{}
		want   string
	}{
		{
			name:   "exact match is busy",
			doomed: []string{"gguf/a/model.gguf"},
			queued: set("gguf/a/model.gguf"),
			want:   "gguf/a/model.gguf",
		},
		{
			name:   "doomed dir covers a queued file",
			doomed: []string{"onnx/whisper"},
			queued: set("onnx/whisper/model.onnx"),
			want:   "onnx/whisper",
		},
		{
			name:   "queued dir covers a doomed file",
			doomed: []string{"onnx/whisper/model.onnx"},
			queued: set("onnx/whisper"),
			want:   "onnx/whisper/model.onnx",
		},
		{
			name:   "no overlap is clear",
			doomed: []string{"gguf/a/model.gguf"},
			queued: set("gguf/b/model.gguf"),
			want:   "",
		},
		{
			name:   "sibling prefix does not overlap",
			doomed: []string{"onnx/whisper"},
			queued: set("onnx/whisperx/model.onnx"),
			want:   "",
		},
		{
			name:   "empty queued set is clear",
			doomed: []string{"gguf/a/model.gguf"},
			queued: set(),
			want:   "",
		},
		{
			name:   "reports the first overlapping doomed path",
			doomed: []string{"gguf/clear.gguf", "gguf/busy.gguf"},
			queued: set("gguf/busy.gguf"),
			want:   "gguf/busy.gguf",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, firstPathOverlap(tt.doomed, tt.queued))
		})
	}
}
