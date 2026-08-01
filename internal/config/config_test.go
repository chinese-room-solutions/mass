package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsLoopbackAddr(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want bool
	}{
		{name: "empty addr", addr: "", want: false},
		{name: "empty host binds all interfaces", addr: ":3455", want: false},
		{name: "default listen addr", addr: DefaultListenAddr, want: true},
		{name: "ipv4 unspecified", addr: "0.0.0.0:3455", want: false},
		{name: "ipv6 unspecified bracketed", addr: "[::]:3455", want: false},
		{name: "ipv6 unspecified bare", addr: "::", want: false},
		{name: "ipv4 loopback", addr: "127.0.0.1:3455", want: true},
		{name: "ipv4 loopback non-canonical", addr: "127.0.0.2:3455", want: true},
		{name: "ipv4 loopback without port", addr: "127.0.0.1", want: true},
		{name: "ipv6 loopback", addr: "[::1]:3455", want: true},
		{name: "localhost", addr: "localhost:3455", want: true},
		{name: "localhost case-insensitive", addr: "LocalHost:3455", want: true},
		{name: "LAN IP", addr: "192.168.1.10:3455", want: false},
		{name: "public IP", addr: "203.0.113.7:3455", want: false},
		{name: "non-localhost hostname", addr: "example.com:3455", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsLoopbackAddr(tt.addr))
		})
	}
}

func TestLocalURL(t *testing.T) {
	tests := []struct {
		name   string
		scheme string
		addr   string
		want   string
	}{
		{name: "empty host", scheme: "http", addr: ":3455", want: "http://localhost:3455"},
		{name: "ipv4 unspecified", scheme: "http", addr: "0.0.0.0:3455", want: "http://localhost:3455"},
		{name: "ipv6 unspecified", scheme: "https", addr: "[::]:3455", want: "https://localhost:3455"},
		{name: "loopback ip kept", scheme: "http", addr: "127.0.0.1:3455", want: "http://127.0.0.1:3455"},
		{name: "ipv6 loopback bracketed", scheme: "http", addr: "[::1]:3455", want: "http://[::1]:3455"},
		{name: "hostname kept", scheme: "https", addr: "example.com:8443", want: "https://example.com:8443"},
		{name: "bare host no port", scheme: "http", addr: "127.0.0.1", want: "http://127.0.0.1"},
		{name: "default listen addr", scheme: "http", addr: DefaultListenAddr, want: "http://127.0.0.1:3455"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, LocalURL(tt.scheme, tt.addr))
		})
	}
}
