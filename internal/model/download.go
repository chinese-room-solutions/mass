package model

// Download represents a persisted model download record.
type Download struct {
	Filename   string
	RepoID     string
	GroupName  string
	Status     string // "active" or "paused"
	Downloaded int64
	Total      int64
}
