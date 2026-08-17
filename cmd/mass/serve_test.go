package main

import (
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// recordingConn tracks the read deadline net/http leaves on a served
// connection.
type recordingConn struct {
	net.Conn
	mu       sync.Mutex
	deadline time.Time
}

func (c *recordingConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadline = t
	c.mu.Unlock()
	return c.Conn.SetReadDeadline(t)
}

func (c *recordingConn) readDeadline() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deadline
}

type recordingListener struct {
	net.Listener
	mu    sync.Mutex
	conns []*recordingConn
}

func (l *recordingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	rc := &recordingConn{Conn: c}
	l.mu.Lock()
	l.conns = append(l.conns, rc)
	l.mu.Unlock()
	return rc, nil
}

func (l *recordingListener) accepted() []*recordingConn {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]*recordingConn(nil), l.conns...)
}

// TestPlaintextServerLeavesNoConnectionReadDeadline pins the daemon's
// plaintext server against a whole-connection read deadline. net/http arms
// ReadHeaderTimeout on the raw connection before it sniffs the h2c preface,
// and the HTTP/2 server only disarms an inherited deadline when ReadTimeout is
// also set — so on h2c the deadline survives into the stream's lifetime and
// kills the TCP connection that long after Accept, however much traffic flows.
// Worker control streams and runtime gateways ride those connections for
// hours.
func TestPlaintextServerLeavesNoConnectionReadDeadline(t *testing.T) {
	tests := []struct {
		name      string
		protocols func() *http.Protocols
		wantProto string
	}{
		{
			name: "h2c",
			protocols: func() *http.Protocols {
				p := &http.Protocols{}
				p.SetUnencryptedHTTP2(true)
				return p
			},
			wantProto: "HTTP/2.0",
		},
		{
			name: "http1",
			protocols: func() *http.Protocols {
				p := &http.Protocols{}
				p.SetHTTP1(true)
				return p
			},
			wantProto: "HTTP/1.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inHandler := make(chan string, 1)
			release := make(chan struct{})
			srv := newPlaintextServer("", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				inHandler <- r.Proto
				<-release
			}))
			t.Cleanup(func() { require.NoError(t, srv.Close()) })

			raw, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			ln := &recordingListener{Listener: raw}
			go func() { _ = srv.Serve(ln) }()

			client := &http.Client{Transport: &http.Transport{Protocols: tt.protocols()}}
			done := make(chan struct{})
			go func() {
				defer close(done)
				resp, err := client.Get("http://" + raw.Addr().String() + "/")
				if err == nil {
					_ = resp.Body.Close()
				}
			}()

			var proto string
			select {
			case proto = <-inHandler:
			case <-time.After(10 * time.Second):
				t.Fatal("handler never ran")
			}
			// A silent fallback to HTTP/1.1 would make the h2c case vacuous.
			require.Equal(t, tt.wantProto, proto)

			conns := ln.accepted()
			require.Len(t, conns, 1)
			require.True(t, conns[0].readDeadline().IsZero(),
				"server left a read deadline on the connection while a handler was running; "+
					"it will tear down every stream on that connection when it expires")

			close(release)
			<-done
		})
	}
}
