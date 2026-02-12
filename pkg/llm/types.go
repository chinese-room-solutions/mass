package llm

// CompletionRequest represents a single chat completion request.
type CompletionRequest struct {
	Messages         []ChatMessage
	MaxTokens        int
	Temperature      float32  // 0 = greedy/deterministic
	TopP             float32  // Nucleus sampling threshold
	TopK             int      // Top-K sampling
	Seed             int      // Random seed (0 = random)
	Stop             []string // Stop sequences
	MinP             float32  // Minimum probability threshold
	RepeatPenalty    float32  // Repeat penalty multiplier (1.0 = disabled)
	FrequencyPenalty float32  // Frequency-based penalty (0.0 = disabled)
	PresencePenalty  float32  // Presence-based penalty (0.0 = disabled)
	EnableThinking   bool     // Enable reasoning/thinking extraction
}

// ChatMessage represents a message in a chat conversation.
type ChatMessage struct {
	Role    string
	Content string
	Parts   []ContentPart // Multipart content (takes precedence over Content when non-empty)
}

// ContentType enumerates the supported media types for content parts.
// Each inference engine decides which types it can handle.
type ContentType string

const (
	ContentText  ContentType = "text"  // Inline text
	ContentImage ContentType = "image" // Raw image bytes (jpg, png, bmp, gif)
	ContentAudio ContentType = "audio" // Raw audio bytes (wav)
	ContentFile  ContentType = "file"  // Opaque file (engine-dependent support)
)

// ContentPart represents a single part of a multipart message.
type ContentPart struct {
	Type     ContentType // Media type
	Text     string      // Text content (for ContentText)
	Data     []byte      // Raw media bytes (for ContentImage, ContentAudio, ContentFile)
	Filename string      // Original filename (informational)
}

// CompletionResult represents the result of a chat completion request.
type CompletionResult struct {
	Text             string
	ReasoningContent string // Extracted reasoning/thinking content
	Error            error
	PromptTokens     int     // Number of tokens in the prompt
	CompletionTokens int     // Number of tokens generated
	TokensPerSecond  float64 // Generation speed from llama.cpp timing
}

// CompletionDelta represents a streaming chunk from chat completion.
type CompletionDelta struct {
	Content          string
	ReasoningContent string
	// Usage is populated only in the final delta (after generation completes).
	Usage *CompletionUsage
}

// CompletionUsage holds token usage statistics.
type CompletionUsage struct {
	PromptTokens     int
	CompletionTokens int
	TotalTokens      int
	TokensPerSecond  float64
}

// EmbeddingResult represents the result of a single embedding request.
type EmbeddingResult struct {
	Embedding []float32
	Error     error
}

// BatchEmbeddingResult represents the result of a batch embedding request.
type BatchEmbeddingResult struct {
	Embeddings [][]float32
	Error      error
}
