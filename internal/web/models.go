package web

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/chinese-room-solutions/mass-module/uikit"
	"github.com/chinese-room-solutions/mass/internal/gguf"
)

// LocalModelInfo represents a GGUF model file found in the centralized models directory.
type LocalModelInfo struct {
	Path         string    // absolute path to the .gguf file
	Filename     string    // base filename (e.g. "Qwen2.5-7B-Instruct-Q4_K_M.gguf")
	RelPath      string    // path relative to models root (e.g. "publisher/repo/file.gguf")
	ModelID      string    // relative path without extension (e.g. "publisher/repo/file")
	ModelType    string    // "Chat", "Embedding", or "Unknown"
	Quantization string    // parsed quant tag like "Q4_K_M", "Q5_K_S", ""
	SizeBytes    int64     // file size
	ModTime      time.Time // modification time
	SubDir       string    // publisher/repo prefix (first two path segments)
	HasVision    bool      // true if a sibling mmproj file exists in the same directory
	HasThinking  bool      // true if GGUF chat template contains thinking tokens
}

// ModelGroup groups variants of the same base model together.
type ModelGroup struct {
	BaseName  string           // human-readable base model name (quant stripped)
	ModelType string           // dominant type among variants
	SubDir    string           // subdirectory under models/
	Variants  []LocalModelInfo // individual GGUF files
}

// ScanModels walks the given models directory recursively and returns all .gguf files.
func ScanModels(modelsDir string) ([]LocalModelInfo, error) {
	var models []LocalModelInfo

	err := filepath.WalkDir(modelsDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // skip inaccessible entries
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".gguf") {
			return nil
		}
		// Skip in-progress download temp files.
		if strings.HasPrefix(d.Name(), ".downloading-") {
			return nil
		}

		info, statErr := d.Info()
		if statErr != nil {
			return nil
		}

		rel, _ := filepath.Rel(modelsDir, path)
		rel = filepath.ToSlash(rel)

		// ModelID is the relative path without extension.
		modelID := strings.TrimSuffix(rel, filepath.Ext(rel))

		// SubDir is the publisher/repo prefix (everything before the filename).
		subDir := ""
		if idx := strings.LastIndex(rel, "/"); idx >= 0 {
			subDir = rel[:idx]
		}

		m := LocalModelInfo{
			Path:         path,
			Filename:     d.Name(),
			RelPath:      rel,
			ModelID:      modelID,
			ModelType:    inferModelType(d.Name(), rel),
			Quantization: uikit.ExtractQuant(d.Name()),
			SizeBytes:    info.Size(),
			ModTime:      info.ModTime(),
			SubDir:       subDir,
		}
		models = append(models, m)
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Mark vision-capable models: collect directories that contain mmproj files,
	// then set HasVision on non-mmproj models in those directories.
	mmprojDirs := make(map[string]bool)
	for _, m := range models {
		if m.ModelType == "Mmproj" {
			mmprojDirs[filepath.Dir(m.Path)] = true
		}
	}
	for i := range models {
		if models[i].ModelType != "Mmproj" && mmprojDirs[filepath.Dir(models[i].Path)] {
			models[i].HasVision = true
		}
	}

	// Detect thinking-capable models from cached GGUF metadata (parallel).
	var wg sync.WaitGroup
	for i := range models {
		if models[i].ModelType == "Mmproj" || models[i].ModelType == "Embedding" {
			continue
		}
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tmpl := ReadGGUFChatTemplate(models[idx].Path)
			models[idx].HasThinking = detectThinkingSupport(tmpl)
		}(i)
	}
	wg.Wait()

	// Sort newest first.
	sort.Slice(models, func(i, j int) bool {
		return models[i].ModTime.After(models[j].ModTime)
	})

	return models, nil
}

// inferModelType guesses the model type from the filename and relative path.
func inferModelType(filename, relPath string) string {
	lower := strings.ToLower(filename + " " + relPath)
	if strings.Contains(lower, "mmproj") {
		return "Mmproj"
	}
	if strings.Contains(lower, "embed") || strings.Contains(lower, "sentence") ||
		strings.Contains(lower, "feature-extraction") || strings.Contains(lower, "bge-") ||
		strings.Contains(lower, "gte-") || strings.Contains(lower, "e5-") {
		return "Embedding"
	}
	return "Chat"
}

// stripQuant removes the quantization suffix from a GGUF filename to get the base model name.
// It preserves dots in version numbers (e.g. "v1.5") by only splitting on '-' and treating
// dot-separated quant suffixes (e.g. ".Q4_K_M") as a single unit.
func stripQuant(filename string) string {
	base := strings.TrimSuffix(filename, ".gguf")
	base = strings.TrimSuffix(base, ".GGUF")

	// First, strip any dot-separated trailing quant tokens (e.g. ".Q4_K_M", ".F16").
	// Walk backwards through dot-separated segments from the right.
	// A segment is only considered quant-like if it contains at least one recognized
	// quant token (not just bare digits, which could be version numbers like ".5" in "v1.5").
	for {
		idx := strings.LastIndex(base, ".")
		if idx <= 0 {
			break
		}
		seg := base[idx+1:]
		// Split segment on '-' or '_' and check each sub-token.
		subTokens := strings.FieldsFunc(seg, func(r rune) bool { return r == '-' || r == '_' })
		allQuant := len(subTokens) > 0
		hasRealQuant := false
		for _, st := range subTokens {
			upper := strings.ToUpper(st)
			if uikit.IsQuantToken(upper) {
				hasRealQuant = true
			} else if (len(upper) != 1 || upper < "A" || upper > "Z") && !isAllDigits(st) {
				allQuant = false
				break
			}
		}
		if !allQuant || !hasRealQuant {
			break
		}
		base = base[:idx]
	}

	// Then, strip any dash-separated trailing quant tokens (e.g. "-Q4_K_M").
	parts := strings.Split(base, "-")
	end := len(parts)
	for end > 1 {
		p := strings.ToUpper(parts[end-1])
		if uikit.IsQuantToken(p) {
			end--
			continue
		}
		if len(p) == 1 && end > 1 {
			prev := strings.ToUpper(parts[end-2])
			if uikit.IsQuantToken(prev) {
				end -= 2
				continue
			}
		}
		break
	}

	if end < len(parts) {
		return strings.Join(parts[:end], "-")
	}
	return base
}

// isAllDigits returns true if s is non-empty and contains only digits.
func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// GroupModels groups a flat list of LocalModelInfo into ModelGroups
// by clustering files that share the same base model name and subdirectory.
func GroupModels(models []LocalModelInfo) []ModelGroup {
	type groupKey struct {
		baseName string
		subDir   string
	}

	groupMap := make(map[groupKey]*ModelGroup)
	var order []groupKey

	// First pass: group non-mmproj models.
	for _, m := range models {
		if m.ModelType == "Mmproj" {
			continue
		}
		base := stripQuant(m.Filename)
		key := groupKey{baseName: base, subDir: m.SubDir}
		if _, ok := groupMap[key]; !ok {
			groupMap[key] = &ModelGroup{
				BaseName:  uikit.FormatModelName(base),
				ModelType: m.ModelType,
				SubDir:    m.SubDir,
			}
			order = append(order, key)
		}
		groupMap[key].Variants = append(groupMap[key].Variants, m)
	}

	// Second pass: attach mmproj files to the first model group in the same SubDir.
	// This keeps them visible (deletable) but grouped with their parent model.
	subDirGroup := make(map[string]groupKey)
	for _, key := range order {
		if _, ok := subDirGroup[key.subDir]; !ok {
			subDirGroup[key.subDir] = key
		}
	}
	for _, m := range models {
		if m.ModelType != "Mmproj" {
			continue
		}
		if parentKey, ok := subDirGroup[m.SubDir]; ok {
			groupMap[parentKey].Variants = append(groupMap[parentKey].Variants, m)
		} else {
			// Orphan mmproj with no parent model — show as standalone group.
			base := stripQuant(m.Filename)
			key := groupKey{baseName: base, subDir: m.SubDir}
			if _, ok := groupMap[key]; !ok {
				groupMap[key] = &ModelGroup{
					BaseName:  uikit.FormatModelName(base),
					ModelType: m.ModelType,
					SubDir:    m.SubDir,
				}
				order = append(order, key)
			}
			groupMap[key].Variants = append(groupMap[key].Variants, m)
		}
	}

	groups := make([]ModelGroup, 0, len(order))
	for _, key := range order {
		groups = append(groups, *groupMap[key])
	}
	return groups
}

// GGUFModelInfo holds parsed GGUF metadata for display in the properties panel.
type GGUFModelInfo struct {
	Filename        string
	Path            string
	Architecture    string
	Name            string
	QuantType       string // human-readable file type name
	ContextLength   uint64
	EmbeddingLength uint64
	BlockCount      uint64
	HeadCount       uint64
	HeadCountKV     uint64
	VocabSize       int
	TokenizerModel  string
	ChatTemplate    string // raw chat template string from GGUF metadata
	FileSize        int64
	FileSizeStr     string
}

// ggufFileTypeNames maps GGUF general.file_type enum to human-readable names.
var ggufFileTypeNames = map[uint32]string{
	0: "All F32", 1: "Mostly F16", 2: "Mostly Q4_0", 3: "Mostly Q4_1",
	7: "Mostly Q8_0", 8: "Mostly Q5_0", 9: "Mostly Q5_1", 10: "Mostly Q2_K",
	11: "Mostly Q3_K_S", 12: "Mostly Q3_K_M", 13: "Mostly Q3_K_L",
	14: "Mostly Q4_K_S", 15: "Mostly Q4_K_M", 16: "Mostly Q5_K_S",
	17: "Mostly Q5_K_M", 18: "Mostly Q6_K", 19: "Mostly IQ2_XXS",
	20: "Mostly IQ2_XS", 21: "Mostly IQ3_XXS", 22: "Mostly IQ1_S",
	23: "Mostly IQ4_NL", 24: "Mostly IQ3_S", 25: "Mostly IQ2_S",
	26: "Mostly IQ4_XS", 27: "Mostly IQ1_M", 28: "Mostly BF16",
}

// ggufInfoCache caches GGUFModelInfo results keyed by absolute path.
// Populated lazily; never expires (GGUF files don't change at runtime).
var ggufInfoCache sync.Map // map[string]*GGUFModelInfo

// ReadGGUFChatTemplate reads only the chat template string from a GGUF file,
// using the shared GGUF info cache.
func ReadGGUFChatTemplate(path string) string {
	info, err := ReadGGUFModelInfo(path)
	if err != nil {
		return ""
	}
	return info.ChatTemplate
}

// ReadGGUFModelInfo reads GGUF metadata from a model file, using an
// in-memory cache to avoid repeated disk reads.
func ReadGGUFModelInfo(path string) (*GGUFModelInfo, error) {
	if v, ok := ggufInfoCache.Load(path); ok {
		return v.(*GGUFModelInfo), nil
	}

	meta, err := gguf.ReadMeta(path)
	if err != nil {
		return nil, err
	}

	arch := meta.GetString("general.architecture")
	info := &GGUFModelInfo{
		Filename:       filepath.Base(path),
		Path:           path,
		Architecture:   arch,
		Name:           meta.GetString("general.name"),
		TokenizerModel: meta.GetString("tokenizer.ggml.model"),
		ChatTemplate:   meta.GetString("tokenizer.chat_template"),
		VocabSize:      meta.GetArrayLen("tokenizer.ggml.tokens"),
	}

	// File type → human-readable name.
	if ft := meta.GetUint32("general.file_type"); ft > 0 {
		if name, ok := ggufFileTypeNames[ft]; ok {
			info.QuantType = name
		} else {
			info.QuantType = fmt.Sprintf("Type %d", ft)
		}
	}

	// Architecture-prefixed keys.
	if arch != "" {
		info.ContextLength = meta.GetUint64(arch + ".context_length")
		info.EmbeddingLength = meta.GetUint64(arch + ".embedding_length")
		info.BlockCount = meta.GetUint64(arch + ".block_count")
		info.HeadCount = meta.GetUint64(arch + ".attention.head_count")
		info.HeadCountKV = meta.GetUint64(arch + ".attention.head_count_kv")
	}

	// File size from disk.
	if fi, err := os.Stat(path); err == nil {
		info.FileSize = fi.Size()
		info.FileSizeStr = uikit.FormatBytes(fi.Size())
	}

	ggufInfoCache.Store(path, info)
	return info, nil
}

// InvalidateGGUFCache removes a model's cached GGUF info by path.
func InvalidateGGUFCache(path string) {
	ggufInfoCache.Delete(path)
}

// downloadedFilesMap scans the models directory and builds a set of
// "repoID/filename" keys for files already on disk.
// With the publisher/repo/file structure, RelPath is already "owner/repo/file.gguf".
func downloadedFilesMap(modelsDir string) map[string]bool {
	m := make(map[string]bool)
	entries, err := ScanModels(modelsDir)
	if err != nil {
		return m
	}
	for _, e := range entries {
		// RelPath is "publisher/repo/file.gguf" which matches "repoID/filename".
		m[e.RelPath] = true
	}
	return m
}
