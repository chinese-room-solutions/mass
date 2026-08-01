package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRebind(t *testing.T) {
	tests := []struct {
		name    string
		dialect Dialect
		in      string
		want    string
	}{
		{"sqlite identity", DialectSQLite, "INSERT INTO t (a,b) VALUES (?, ?)", "INSERT INTO t (a,b) VALUES (?, ?)"},
		{"postgres simple", DialectPostgres, "INSERT INTO t (a,b) VALUES (?, ?)", "INSERT INTO t (a,b) VALUES ($1, $2)"},
		{"postgres many", DialectPostgres, "?,?,?,?,?,?,?,?,?,?,?,?", "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12"},
		{"postgres skips placeholder inside single-quoted literal", DialectPostgres, "WHERE x = 'a?b' AND y = ?", "WHERE x = 'a?b' AND y = $1"},
		{"postgres skips placeholder inside double-quoted identifier", DialectPostgres, `WHERE "a?col" = ?`, `WHERE "a?col" = $1`},
		{"empty", DialectPostgres, "", ""},
		{"no placeholders", DialectPostgres, "SELECT 1", "SELECT 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, rebind(tt.dialect, tt.in))
		})
	}
}
