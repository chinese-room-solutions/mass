package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJoinTokens(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, s *Store)
	}{
		{
			name: "insert then list returns valid rows",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				now := int64(1000)
				require.NoError(t, s.InsertJoinToken(JoinTokenRow{
					ID: "jt1", TokenHash: "hash1", ExpiresAt: now + 3600, CreatedAt: now,
				}, now))

				rows, err := s.ListValidJoinTokens(now)
				require.NoError(t, err)
				require.Len(t, rows, 1)
				require.Equal(t, "jt1", rows[0].ID)
				require.Equal(t, "hash1", rows[0].TokenHash)
				require.Equal(t, now+3600, rows[0].ExpiresAt)
				require.Equal(t, now, rows[0].CreatedAt)
			},
		},
		{
			name: "list excludes expired rows",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.InsertJoinToken(JoinTokenRow{
					ID: "live", TokenHash: "h", ExpiresAt: 2000, CreatedAt: 1000,
				}, 1000))
				// Insert an already-expired-relative-to-later-now token.
				require.NoError(t, s.InsertJoinToken(JoinTokenRow{
					ID: "soon", TokenHash: "h", ExpiresAt: 1500, CreatedAt: 1000,
				}, 1000))

				rows, err := s.ListValidJoinTokens(1600)
				require.NoError(t, err)
				require.Len(t, rows, 1)
				require.Equal(t, "live", rows[0].ID)
			},
		},
		{
			name: "insert prunes expired rows",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.InsertJoinToken(JoinTokenRow{
					ID: "old", TokenHash: "h", ExpiresAt: 500, CreatedAt: 100,
				}, 100))
				// A later insert with now past the old row's expiry prunes it.
				require.NoError(t, s.InsertJoinToken(JoinTokenRow{
					ID: "new", TokenHash: "h", ExpiresAt: 5000, CreatedAt: 1000,
				}, 1000))

				rows, err := s.ListValidJoinTokens(1000)
				require.NoError(t, err)
				require.Len(t, rows, 1)
				require.Equal(t, "new", rows[0].ID)
			},
		},
		{
			name: "list prunes expired rows opportunistically",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.InsertJoinToken(JoinTokenRow{
					ID: "gone", TokenHash: "h", ExpiresAt: 900, CreatedAt: 100,
				}, 100))

				// Listing at a now past expiry deletes the row.
				rows, err := s.ListValidJoinTokens(1000)
				require.NoError(t, err)
				require.Empty(t, rows)

				// A boundary now (== expiry) is treated as expired.
				require.NoError(t, s.InsertJoinToken(JoinTokenRow{
					ID: "boundary", TokenHash: "h", ExpiresAt: 2000, CreatedAt: 100,
				}, 100))
				rows, err = s.ListValidJoinTokens(2000)
				require.NoError(t, err)
				require.Empty(t, rows)
			},
		},
		{
			name: "empty store lists nothing",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				rows, err := s.ListValidJoinTokens(1)
				require.NoError(t, err)
				require.Empty(t, rows)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestDB(t)
			tt.run(t, s)
		})
	}
}
