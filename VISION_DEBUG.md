# Vision/Multimodal Debug Notes

## Status: RESOLVED (2026-03-13)
Vision works correctly. Root cause was sampling defaults — proto3 zero values overrode llama-go's sane defaults with `temp=0, top_p=0, top_k=0`, producing garbage output.

## Problem (original)
LLaVA 1.6 Mistral 7B (Q4_K_M) produces garbage output when processing images through MASS, while text-only requests work perfectly. The official `llama-mtmd-cli` works correctly with the same model and image.

## Test Model
- **Model**: `cjpais/llava-1.6-mistral-7b-gguf/llava-v1.6-mistral-7b.Q4_K_M.gguf`
- **mmproj**: `cjpais/llava-1.6-mistral-7b-gguf/mmproj-model-f16.gguf`
- **Location**: `D:\Programs\MASS Data\models\cjpais\llava-1.6-mistral-7b-gguf\`

## Test Image
Any image works for testing. The playground UI has "Attach Image" button. Images are sent as raw bytes (jpg/png/bmp/gif), base64-decoded before reaching llama-go.

## What Works
- Text-only chat with the same model
- Same model + same image via official `llama-mtmd-cli.exe` (produces correct descriptions)
- CLIP encoding succeeds (image → embeddings)
- Tokenization succeeds (text + image → chunks)
- Eval succeeds (chunks → KV cache)
- Generation loop runs and produces tokens

## What Doesn't Work
- Vision output is garbage: random tokens, license text fragments ("MERCHANTABILITY"), repetitive "The. The. The."
- With default template (no chat_template override): 500 "string field contains invalid UTF-8" (complete binary garbage)
- With vicuna template: garbage text but valid UTF-8

## Fixes Applied So Far

### 1. f16 KV Cache for Vision Models (APPLIED, PARTIAL FIX)
**File**: `pkg/llama/llama.go` → `buildContextOpts()`
```go
if m.mmprojPath != "" {
    opts = append(opts, llama.WithKVCacheType("f16"))
}
```
**Why**: llama-go defaults KV cache to q8_0 quantization. Image embeddings injected as raw float vectors get corrupted by quantization. The CLI uses f16 by default.

**Result**: KV cache is confirmed f16 in logs (`K (f16): 2048.00 MiB, V (f16): 2048.00 MiB`). Output changed from pure repetition to garbage-with-some-real-words, suggesting partial improvement.

### 2. Reverted Forced Flash Attention Disable (APPLIED)
**File**: `pkg/llama/pool.go` → `newPool()`
```go
// Before (wrong fix attempt):
vision, vErr := model.model.NewVisionContext(model.mmprojPath,
    llama.WithVisionFlashAttn("disabled"))
// After (correct):
vision, vErr := model.model.NewVisionContext(model.mmprojPath)
```
**Why**: CLI works with flash attention enabled. Disabling it was a wrong hypothesis.

### 3. Diagnostic Logging Added
**File**: `llama-go/wrapper.cpp` → `llama_wrapper_vision_generate()`
- Logs full prompt text being sent to `mtmd_tokenize`
- Logs chunk types and token counts
- Logs eval result and n_past

**File**: `llama-go/chat.go` → `chatVisionWithContext()`
- Logs full formatted prompt, template name, image sizes

## Confirmed Working: Prompt Format
From logs, the prompt sent to mtmd is correct:
```
You are a helpful assistant.\n\nUSER: <__media__>What's on this image?\nASSISTANT:
```
- `<__media__>` marker is present and correctly placed
- Tokenization: 3 chunks (text=12tok, image=2880tok, text=12tok), total=2904
- CLIP encoding: ~868ms, succeeds
- Image decoding: 2 batches (2048 + 832), ~3.5s total, succeeds
- eval_res=0, new_n_past=2904

## Key Differences: Our Code vs Working CLI

### 1. Chat Template Engine (LIKELY SIGNIFICANT)
- **CLI**: Uses `common_chat_templates_init()` + `common_chat_format_single()` — **Jinja-based** template engine
- **Our code**: Uses `llama_chat_apply_template()` — **native C** template engine (supports ~40 named templates)
- For vicuna template, the output should be equivalent, but for the model's built-in Jinja template they may differ

### 2. Message Processing (POSSIBLE ISSUE)
- **CLI**: Processes messages **incrementally** — system prompt first (with BOS), then user message (no BOS). Each gets its own tokenize+eval cycle
- **Our code**: Formats ALL messages into a single prompt string, tokenizes and evals everything in one shot
- Both approaches should produce equivalent KV cache state, but haven't been verified

### 3. Default Sampling Parameters (CONFIRMED ISSUE - NOT ROOT CAUSE)
- **CLI default**: temperature=0.8, top_k=40, top_p=0.95, min_p=0.05
- **MASS default when user sends no params**: temperature=0.0, top_k=0, top_p=0.0 (proto zero values)
- Temperature 0.0 = greedy sampling. Even with greedy, model should produce coherent output if it understands the image
- Fix: MASS server should apply sane defaults when proto fields are zero

### 4. Flash Attention Mode
- **CLI**: `LLAMA_FLASH_ATTN_TYPE_AUTO` (checks compatibility, then enables/disables)
- **Our code**: `"enabled"` (forced on when GPU detected, in `pkg/llama/llama.go` line 127)
- Both result in flash attention being on for GPU models. Shouldn't matter.

### 5. `add_special` Flag
- **CLI**: `add_special = true` only for first message (BOS), `false` for subsequent messages
- **Our code**: Always `add_special = true` for the full prompt
- Since we format everything as one prompt, one BOS at start is correct. mtmd_tokenize handles this.

## Code Comparison with CLI (2026-03-13)

Thorough comparison of `llama-mtmd-cli.cpp` vs `wrapper.cpp`:

### Confirmed Equivalent
- **Chat template output**: Both use `llama_chat_apply_template` for vicuna. Even though CLI uses `common_chat_format_single` (Jinja wrapper), it falls back to the same C template engine for named templates like "vicuna". Output format is identical.
- **mtmd_tokenize call**: Same flags (`add_special=true`, `parse_special=true`).
- **mtmd_helper_eval_chunks params**: Same (seq_id=0, logits_last=true, n_batch from context).
- **Bitmap creation**: CLI uses `mtmd_helper_bitmap_init_from_file`, our code uses `mtmd_helper_bitmap_init_from_buf`. Both produce `mtmd_bitmap` via stbi. Functionally equivalent.
- **Sampler token acceptance**: Both call `common_sampler_accept()` only for generated tokens, not prompt tokens.
- **KV cache clear**: Our code clears before each vision request. CLI has cumulative cache but achieves same result for first turn.

### Differences Found and Fixed
1. **Sampling defaults overridden with zero values (FIXED)**
   - MASS was setting `llama.Float32(0.0)` for all sampling params when user didn't specify them (proto3 zero values). This created non-nil pointers that override llama-go defaults (temp=0.8, top_k=40, top_p=0.95, min_p=0.05) with zeros.
   - With temp=0: greedy (OK for debugging but unusual default). top_p=0, min_p=0: technically doesn't affect greedy but wrong when temp>0.
   - **Fix**: Only set sampling option pointers when value is non-zero.
   - **File**: `pkg/llama/llama.go` → `buildChatArgs()`

2. **Vision context flash_attn mismatch (FIXED)**
   - Main context created with `flash_attn="enabled"`, vision context with default `"auto"`.
   - CLI passes the same `flash_attn_type` to both contexts.
   - **Fix**: Pass `model.flashAttn` to `NewVisionContext()`.
   - **File**: `pkg/llama/pool.go` → `newPool()`

### Diagnostics Added
- **Image SHA-256 hash**: Logged at server level (`server.go`) and llama-go level (`chat.go`). Compare hashes at both layers to verify no corruption in transit.
- **CLIP embedding dump**: `wrapper.cpp` now manually iterates chunks for image chunks: encodes via `mtmd_encode_chunk`, logs first/last 4 floats and L2 norm of first token embedding, then decodes via `mtmd_helper_decode_image_chunk`.
- **Sampling params log**: Logs actual temp, top_k, top_p, min_p, max_tokens, n_past before generation starts.

## Remaining Hypotheses

### A. Chat Template Mismatch (RULED OUT for vicuna)
Both paths produce identical vicuna format. If the model's built-in Jinja template differs from the native vicuna format, passing `--chat-template vicuna` to the CLI also uses the native C path. No difference expected.

### B. Image Data Corruption in Transit
The image bytes could be getting corrupted somewhere in the pipeline (proto encoding, playground JS, server decoding). The CLIP encoder might silently produce bad embeddings from corrupt data.

**Test**: Compare SHA-256 hashes logged at server vs llama-go level. Save raw bytes to file and compare.

### C. CLIP Embedding Corruption
The CLIP encoder might produce different embeddings due to subtle float precision differences, or the embedding dimension (`n_embd_inp` vs `n_embd`) might be mismatched.

**Test**: Compare embedding values (first/last floats, L2 norm) logged by wrapper.cpp with values from CLI (add same logging to CLI to compare).

### D. Something in Context Creation
There might be a subtle difference in how the context is created that affects vision decoding. For example, `n_ubatch` or some other context param.

**Test**: Match all context creation params exactly to CLI defaults.

## How to Test with the Official CLI

### Build the CLI
```powershell
cd D:\workspace\llama-go\llama.cpp
cmake -S . -B build-cli -G "Visual Studio 17 2022" `
    -DCMAKE_BUILD_TYPE=Release `
    -DBUILD_SHARED_LIBS=OFF `
    -DGGML_CUDA=ON `
    -DCMAKE_CUDA_ARCHITECTURES=86
cmake --build build-cli --config Release --target llama-mtmd-cli
```
Binary: `build-cli\bin\Release\llama-mtmd-cli.exe`

### Run the CLI (interactive)
```bash
cd D:\workspace\llama-go\llama.cpp
./build-cli/bin/Release/llama-mtmd-cli.exe \
    -m "D:/Programs/MASS Data/models/cjpais/llava-1.6-mistral-7b-gguf/llava-v1.6-mistral-7b.Q4_K_M.gguf" \
    --mmproj "D:/Programs/MASS Data/models/cjpais/llava-1.6-mistral-7b-gguf/mmproj-model-f16.gguf" \
    --chat-template vicuna \
    -ngl 99 \
    -fa
```
Then in the interactive prompt:
```
/image path/to/test-image.jpg
What's on this image?
```

### Run the CLI (single-turn)
```bash
./build-cli/bin/Release/llama-mtmd-cli.exe \
    -m "D:/Programs/MASS Data/models/cjpais/llava-1.6-mistral-7b-gguf/llava-v1.6-mistral-7b.Q4_K_M.gguf" \
    --mmproj "D:/Programs/MASS Data/models/cjpais/llava-1.6-mistral-7b-gguf/mmproj-model-f16.gguf" \
    --chat-template vicuna \
    -ngl 99 \
    -fa \
    --image path/to/test-image.jpg \
    -p "What's on this image?"
```

## How to Rebuild MASS After Changes

### Rebuild wrapper.cpp only (after C++ changes)
```powershell
$MinGW = "C:\msys64\mingw64\bin"
$LlamaGoDir = "D:\workspace\llama-go"
$env:PATH = "$MinGW;$env:PATH"

& "$MinGW\g++.exe" -c -O3 -DNDEBUG "-std=c++17" -DGGML_USE_CUDA "-D_WIN32_WINNT=0x0A00" `
    "-I$LlamaGoDir" "-I$LlamaGoDir/llama.cpp" "-I$LlamaGoDir/llama.cpp/common" `
    "-I$LlamaGoDir/llama.cpp/ggml/include" "-I$LlamaGoDir/llama.cpp/include" `
    "-I$LlamaGoDir/llama.cpp/vendor" "-I$LlamaGoDir/llama.cpp/tools/mtmd" `
    "$LlamaGoDir/wrapper.cpp" -o "$LlamaGoDir/wrapper_mingw.o"

& "$MinGW\ar.exe" crs "$LlamaGoDir/libbinding.a" "$LlamaGoDir/wrapper_mingw.o"
```

### Build mass.exe
```powershell
$MinGW = "C:\msys64\mingw64\bin"
$LlamaGoDir = "D:\workspace\llama-go"
$env:PATH = "$MinGW;$env:PATH"
$env:CGO_ENABLED = "1"
$env:C_INCLUDE_PATH = $LlamaGoDir
$env:LIBRARY_PATH = $LlamaGoDir
$env:CGO_LDFLAGS = "-L$LlamaGoDir"

cd D:\workspace\mass
go build -a -tags cublas -p 16 -ldflags="-H windowsgui" -o bin\mass.exe .\cmd\mass
```

Or use the Makefile which delegates to `make-win.ps1`:
```bash
make build  # Full build (skips libs if already present)
```

## Build Architecture (Windows)
- **MSVC+CUDA**: Builds ggml DLLs (GPU inference) → `build-cuda-dll/`
- **MinGW**: Builds static libs (llama, common, mtmd) → `build-mingw/`
- **MinGW**: Compiles `wrapper.cpp` → `wrapper_mingw.o` → `libbinding.a`
- **Go/CGO**: Links MinGW static libs + ggml DLLs via import libs
- Script: `make-win.ps1` orchestrates everything

## Key Files

### llama-go
| File | Purpose |
|------|---------|
| `wrapper.cpp` | C++ bridge: vision generate, chat template, sampling |
| `chat.go` | Go chat dispatch: vision detection, image collection, prompt formatting |
| `vision.go` | VisionContext type, constructor, options |
| `chat_types.go` | ChatMessage, ContentPart types |
| `types.go:81` | Default KV cache type (q8_0 — needs f16 override for vision) |
| `model.go:448` | `applyChatTemplate()` — wraps `llama_chat_apply_template()` |

### MASS
| File | Purpose |
|------|---------|
| `pkg/llama/llama.go` | Model loading, context creation, predict/predictStream |
| `pkg/llama/pool.go` | Worker pool with per-worker VisionContext |
| `internal/server/server.go` | RPC → CompletionRequest conversion |
| `internal/config/config.go` | LlamaChatConfig with flash_attn, chat_template, etc. |

## Logs Location
```
C:\Users\kerne\AppData\Roaming\mass\logs\mass.log
```

## Next Steps
1. **Build and test** — rebuild wrapper.cpp + mass.exe, test with the same LLaVA model + image
2. **Check logs** — verify sampling params are now sane (temp=0.8, top_p=0.95), check embedding values
3. **Compare embedding values with CLI** — add same logging to CLI, run same image, compare float values
4. **Try with a different vision model** (e.g., Qwen2-VL, llava-phi3) to see if it's model-specific
5. **If embeddings match but output is still garbage**: investigate context creation params (n_ubatch, etc.)
6. **If embeddings differ**: investigate CLIP encoder config (n_threads, GPU offload, etc.)
