package worker

import (
	"fmt"
	"sync"
	"testing"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestNegotiateWorkerProtocol(t *testing.T) {
	tests := []struct {
		name            string
		workerProtocols []int32
		wantErr         bool
	}{
		{"exact match accepts", []int32{1}, false},
		{"superset accepts", []int32{1, 2, 3}, false},
		{"unset list rejects", nil, true},
		{"empty list rejects", []int32{}, true},
		{"no common version rejects", []int32{2, 3}, true},
		{"unknown-only version rejects", []int32{99}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := negotiateWorkerProtocol("w1", tt.workerProtocols)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			// The rejection reaches the worker's log: it must name the
			// worker and both lists so the operator can act on it.
			require.Contains(t, err.Error(), "w1")
			require.Contains(t, err.Error(), fmt.Sprintf("%v", tt.workerProtocols))
			require.Contains(t, err.Error(), fmt.Sprintf("%v", workerpb.SupportedProtocols))
		})
	}
}

func TestCovers(t *testing.T) {
	tests := []struct {
		name      string
		dir, path string
		want      bool
	}{
		{"strict child", "a/b", "a/b/c", true},
		{"grandchild", "a/b", "a/b/c/d.gguf", true},
		{"equal is not covered", "a/b", "a/b", false},
		{"sibling prefix not covered", "a/b", "a/bc", false},
		{"unrelated", "a/b", "x/y", false},
		{"parent not covered by child", "a/b/c", "a/b", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, covers(tt.dir, tt.path))
		})
	}
}

func TestIsProtected(t *testing.T) {
	tests := []struct {
		name      string
		f         string
		protected []string
		want      bool
	}{
		{"exact file match", "gguf/a/model.gguf", []string{"gguf/a/model.gguf"}, true},
		{"file under protected dir", "onnx/whisper/model.onnx", []string{"onnx/whisper"}, true},
		{"reported dir covers protected file", "onnx/whisper", []string{"onnx/whisper/model.onnx"}, true},
		{"no overlap", "gguf/b/model.gguf", []string{"gguf/a/model.gguf"}, false},
		{"sibling prefix not protected", "onnx/whisperx/model.onnx", []string{"onnx/whisper"}, false},
		{"empty protected set", "gguf/a/model.gguf", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isProtected(tt.f, tt.protected))
		})
	}
}

// captureDeletes installs a fake sender that records every DeleteCacheFiles
// dispatch so a reconcile pass can be asserted on.
func captureDeletes(w *StreamWorker, got *[]string, mu *sync.Mutex) {
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if d := msg.GetDeleteCacheFiles(); d != nil {
			mu.Lock()
			*got = append(*got, d.Filenames...)
			mu.Unlock()
		}
		return nil
	})
}

func TestMaybeReconcile(t *testing.T) {
	tests := []struct {
		name      string
		cacheFile []string
		loaded    []*workerpb.LoadedModelStatus
		canonical map[string]struct{}
		wantStale []string
	}{
		{
			name:      "stale file reaped",
			cacheFile: []string{"gguf/a/model.gguf", "gguf/old/model.gguf"},
			canonical: map[string]struct{}{"gguf/a/model.gguf": {}},
			wantStale: []string{"gguf/old/model.gguf"},
		},
		{
			name:      "loaded file protected even though non-canonical",
			cacheFile: []string{"gguf/live/model.gguf"},
			loaded:    []*workerpb.LoadedModelStatus{{ModelId: "x", Files: []string{"gguf/live/model.gguf"}}},
			canonical: map[string]struct{}{"gguf/a/model.gguf": {}},
			wantStale: nil,
		},
		{
			name:      "file under a loaded directory subtree protected",
			cacheFile: []string{"onnx/whisper/model.onnx", "onnx/whisper/tokens.txt"},
			loaded:    []*workerpb.LoadedModelStatus{{ModelId: "x", Files: []string{"onnx/whisper"}}},
			canonical: map[string]struct{}{"gguf/a/model.gguf": {}},
			wantStale: nil,
		},
		{
			name:      "reported dir covering a loaded file protected",
			cacheFile: []string{"onnx/whisper"},
			loaded:    []*workerpb.LoadedModelStatus{{ModelId: "x", Files: []string{"onnx/whisper/model.onnx"}}},
			canonical: map[string]struct{}{"gguf/a/model.gguf": {}},
			wantStale: nil,
		},
		{
			name:      "canonical directory subtree keeps its files",
			cacheFile: []string{"onnx/whisper/model.onnx"},
			canonical: map[string]struct{}{"onnx/whisper/model.onnx": {}},
			wantStale: nil,
		},
		{
			name:      "empty canonical set skips reconcile (store unreadable)",
			cacheFile: []string{"gguf/old/model.gguf"},
			canonical: map[string]struct{}{},
			wantStale: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &StreamWorker{logger: zerolog.Nop()}
			w.ApplyHeartbeat(&workerpb.WorkerHeartbeat{
				CacheFiles:   tt.cacheFile,
				LoadedModels: tt.loaded,
			})
			var mu sync.Mutex
			var got []string
			captureDeletes(w, &got, &mu)

			h := &Hub{canonical: func() map[string]struct{} { return tt.canonical }, logger: zerolog.Nop()}
			h.maybeReconcile(w)

			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, tt.wantStale, got)
		})
	}
}

// A nil canonical fn means the provider hasn't been wired yet: reconcile must
// be a no-op, never a mass delete.
func TestMaybeReconcile_NilCanonicalSkips(t *testing.T) {
	w := &StreamWorker{logger: zerolog.Nop()}
	w.ApplyHeartbeat(&workerpb.WorkerHeartbeat{CacheFiles: []string{"gguf/x/model.gguf"}})
	var mu sync.Mutex
	var got []string
	captureDeletes(w, &got, &mu)

	h := &Hub{logger: zerolog.Nop()}
	h.maybeReconcile(w)

	require.Empty(t, got)
}
