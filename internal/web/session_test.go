package web

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSessionStore(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "create and validate",
			run: func(t *testing.T) {
				t.Helper()
				ss := NewSessionStore(time.Hour)
				id, err := ss.Create()
				require.NoError(t, err)
				require.NotEmpty(t, id)
				require.True(t, ss.Valid(id))
			},
		},
		{
			name: "unknown session invalid",
			run: func(t *testing.T) {
				t.Helper()
				ss := NewSessionStore(time.Hour)
				require.False(t, ss.Valid("nonexistent"))
			},
		},
		{
			name: "expired session invalid",
			run: func(t *testing.T) {
				t.Helper()
				ss := NewSessionStore(time.Nanosecond)
				id, err := ss.Create()
				require.NoError(t, err)
				time.Sleep(time.Millisecond)
				require.False(t, ss.Valid(id))
			},
		},
		{
			name: "invalidate removes session",
			run: func(t *testing.T) {
				t.Helper()
				ss := NewSessionStore(time.Hour)
				id, err := ss.Create()
				require.NoError(t, err)
				ss.Invalidate(id)
				require.False(t, ss.Valid(id))
			},
		},
		{
			name: "unique session IDs",
			run: func(t *testing.T) {
				t.Helper()
				ss := NewSessionStore(time.Hour)
				id1, err := ss.Create()
				require.NoError(t, err)
				id2, err := ss.Create()
				require.NoError(t, err)
				require.NotEqual(t, id1, id2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}
