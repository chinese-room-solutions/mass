package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"connectrpc.com/connect"
	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
	"github.com/chinese-room-solutions/mass-proto/gen/go/rpcconnect"
	"github.com/stretchr/testify/require"
)

// stubHandler is a configurable MassHandler for tests. Unset funcs fall back to
// UnimplementedMassHandler (which returns CodeUnimplemented).
type stubHandler struct {
	rpcconnect.UnimplementedMassHandler
	getStatus       func() (*rpc.GetStatusResponse, error)
	listRuntimes    func() (*rpc.ListRuntimesResponse, error)
	listWorkers     func() (*rpc.ListWorkersResponse, error)
	getQueue        func() (*rpc.GetQueueResponse, error)
	startRuntime    func(name string) (*rpc.StartRuntimeResponse, error)
	installLocal    func(req *rpc.InstallLocalWorkerRequest) (*rpc.InstallLocalWorkerResponse, error)
	createJoinToken func(req *rpc.CreateJoinTokenRequest) (*rpc.CreateJoinTokenResponse, error)
	lastAuth        string
	lastActor       string
}

func (s *stubHandler) GetStatus(ctx context.Context, r *connect.Request[rpc.GetStatusRequest]) (*connect.Response[rpc.GetStatusResponse], error) {
	s.lastActor = r.Header().Get("X-Mass-Actor")
	if s.getStatus == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, nil)
	}
	msg, err := s.getStatus()
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(msg), nil
}

func (s *stubHandler) ListRuntimes(ctx context.Context, r *connect.Request[rpc.ListRuntimesRequest]) (*connect.Response[rpc.ListRuntimesResponse], error) {
	if s.listRuntimes == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, nil)
	}
	msg, err := s.listRuntimes()
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(msg), nil
}

func (s *stubHandler) CreateJoinToken(ctx context.Context, r *connect.Request[rpc.CreateJoinTokenRequest]) (*connect.Response[rpc.CreateJoinTokenResponse], error) {
	if s.createJoinToken == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, nil)
	}
	msg, err := s.createJoinToken(r.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(msg), nil
}

func (s *stubHandler) ListWorkers(ctx context.Context, r *connect.Request[rpc.ListWorkersRequest]) (*connect.Response[rpc.ListWorkersResponse], error) {
	if s.listWorkers == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, nil)
	}
	msg, err := s.listWorkers()
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(msg), nil
}

func (s *stubHandler) GetQueue(ctx context.Context, r *connect.Request[rpc.GetQueueRequest]) (*connect.Response[rpc.GetQueueResponse], error) {
	if s.getQueue == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, nil)
	}
	msg, err := s.getQueue()
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(msg), nil
}

func (s *stubHandler) StartRuntime(ctx context.Context, r *connect.Request[rpc.StartRuntimeRequest]) (*connect.Response[rpc.StartRuntimeResponse], error) {
	if s.startRuntime == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, nil)
	}
	msg, err := s.startRuntime(r.Msg.RuntimeName)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(msg), nil
}

func (s *stubHandler) InstallLocalWorker(ctx context.Context, r *connect.Request[rpc.InstallLocalWorkerRequest]) (*connect.Response[rpc.InstallLocalWorkerResponse], error) {
	s.lastAuth = r.Header().Get("Authorization")
	if s.installLocal == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, nil)
	}
	msg, err := s.installLocal(r.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(msg), nil
}

// newStubServer mounts stub on an httptest server and returns its base URL.
func newStubServer(t *testing.T, stub *stubHandler) string {
	t.Helper()
	mux := http.NewServeMux()
	path, h := rpcconnect.NewMassHandler(stub)
	mux.Handle(path, h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

// capture runs fn with stdout+stderr redirected, returning their contents.
func capture(t *testing.T, fn func()) (stdout, stderr string) {
	t.Helper()
	origOut, origErr := os.Stdout, os.Stderr
	rOut, wOut, _ := os.Pipe()
	rErr, wErr, _ := os.Pipe()
	os.Stdout, os.Stderr = wOut, wErr
	defer func() { os.Stdout, os.Stderr = origOut, origErr }()

	fn()

	_ = wOut.Close()
	_ = wErr.Close()
	outB, _ := io.ReadAll(rOut)
	errB, _ := io.ReadAll(rErr)
	return string(outB), string(errB)
}

func TestRunCLI_VerbRouting(t *testing.T) {
	stub := &stubHandler{
		getStatus: func() (*rpc.GetStatusResponse, error) {
			return &rpc.GetStatusResponse{Version: "test", ListenAddr: "127.0.0.1:3455", WorkersOnline: 2}, nil
		},
		listRuntimes: func() (*rpc.ListRuntimesResponse, error) {
			return &rpc.ListRuntimesResponse{Runtimes: []*rpc.Runtime{{RuntimeName: "llama-cpp", Version: "1.0", Running: true}}}, nil
		},
		listWorkers: func() (*rpc.ListWorkersResponse, error) {
			return &rpc.ListWorkersResponse{Workers: []*rpc.Worker{{Id: "w1", Name: "box", Online: true}}}, nil
		},
		getQueue: func() (*rpc.GetQueueResponse, error) {
			return &rpc.GetQueueResponse{Sections: []*rpc.QueueSection{{Name: "global", Rows: []*rpc.QueueRow{{MsgId: "m1", ModelId: "x"}}}}}, nil
		},
	}
	url := newStubServer(t, stub)

	tests := []struct {
		name     string
		args     []string
		wantCode int
		wantOut  string
	}{
		{"status", []string{"status", "--addr", url}, exitOK, "version"},
		{"status value", []string{"status", "--addr", url}, exitOK, "test"},
		{"runtimes list", []string{"runtimes", "list", "--addr", url}, exitOK, "llama-cpp"},
		{"workers list", []string{"workers", "list", "--addr", url}, exitOK, "box"},
		{"queue list", []string{"queue", "list", "--addr", url}, exitOK, "m1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var code int
			out, _ := capture(t, func() { code = runCLI(tt.args) })
			require.Equal(t, tt.wantCode, code)
			require.Contains(t, out, tt.wantOut)
		})
	}
}

func TestRunCLI_JSONOutput(t *testing.T) {
	stub := &stubHandler{
		getStatus: func() (*rpc.GetStatusResponse, error) {
			return &rpc.GetStatusResponse{Version: "v9", ListenAddr: "127.0.0.1:3455"}, nil
		},
	}
	url := newStubServer(t, stub)

	var code int
	out, _ := capture(t, func() { code = runCLI([]string{"status", "--json", "--addr", url}) })
	require.Equal(t, exitOK, code)
	require.Contains(t, out, `"version"`)
	require.Contains(t, out, `"v9"`)
	require.Contains(t, out, `"listen_addr"`) // proto field names, not camelCase
}

func TestRunCLI_ActorHeader(t *testing.T) {
	stub := &stubHandler{
		getStatus: func() (*rpc.GetStatusResponse, error) { return &rpc.GetStatusResponse{}, nil },
	}
	url := newStubServer(t, stub)
	capture(t, func() { runCLI([]string{"status", "--addr", url}) })
	require.Equal(t, "cli", stub.lastActor)
}

func TestRunCLI_ErrorExitCode(t *testing.T) {
	stub := &stubHandler{
		startRuntime: func(name string) (*rpc.StartRuntimeResponse, error) {
			return nil, connect.NewError(connect.CodeNotFound, io.EOF)
		},
	}
	url := newStubServer(t, stub)

	t.Run("human", func(t *testing.T) {
		var code int
		_, errOut := capture(t, func() { code = runCLI([]string{"runtimes", "start", "nope", "--addr", url}) })
		require.Equal(t, exitError, code)
		require.Contains(t, errOut, "error:")
	})
	t.Run("json", func(t *testing.T) {
		var code int
		_, errOut := capture(t, func() { code = runCLI([]string{"runtimes", "start", "nope", "--json", "--addr", url}) })
		require.Equal(t, exitError, code)
		require.Contains(t, errOut, `"code":"not_found"`)
	})
}

func TestRunCLI_UsageExit(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"unknown verb", []string{"bogus"}},
		{"unknown subcommand", []string{"models", "frobnicate"}},
		{"group without sub", []string{"runtimes"}},
		{"missing required flag", []string{"models", "delete"}},
		{"missing positional", []string{"runtimes", "start"}},
		{"bad flag", []string{"status", "--nope"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var code int
			_, errOut := capture(t, func() { code = runCLI(tt.args) })
			require.Equal(t, exitUsage, code)
			require.NotEmpty(t, errOut)
		})
	}
}

func TestWorkersJoinCommand(t *testing.T) {
	oneRuntime := func() (*rpc.ListRuntimesResponse, error) {
		return &rpc.ListRuntimesResponse{Runtimes: []*rpc.Runtime{{RuntimeName: "llama-cpp"}}}, nil
	}
	twoRuntimes := func() (*rpc.ListRuntimesResponse, error) {
		return &rpc.ListRuntimesResponse{Runtimes: []*rpc.Runtime{{RuntimeName: "llama-cpp"}, {RuntimeName: "vllm"}}}, nil
	}

	// mintToken echoes the requested TTL into the returned token so a test can
	// assert --ttl reached the server, and always returns a mjt_-shaped token.
	mintToken := func(req *rpc.CreateJoinTokenRequest) (*rpc.CreateJoinTokenResponse, error) {
		return &rpc.CreateJoinTokenResponse{
			Token:     "mjt_test",
			ExpiresAt: 1_000_000_000 + req.GetTtlSeconds(),
		}, nil
	}

	tests := []struct {
		name       string
		list       func() (*rpc.ListRuntimesResponse, error)
		extra      []string // args after --addr
		wantCode   int
		wantOut    []string
		wantNotOut []string
		wantErr    []string
	}{
		{
			name:     "single runtime inferred, minted token embedded",
			list:     oneRuntime,
			wantCode: exitOK,
			wantOut: []string{
				"/setup/worker-bin/llama-cpp?os=$(uname -s)&arch=$(uname -m)",
				"-o mass-worker-setup && chmod +x mass-worker-setup",
				"./mass-worker-setup --mass-url",
				"--token mjt_test",
				"?os=windows&arch=AMD64",
				"-OutFile mass-worker-setup.exe",
				".\\mass-worker-setup.exe --mass-url",
				"Token valid until",
			},
		},
		{
			name:     "json emits structured fields",
			list:     oneRuntime,
			extra:    []string{"--json"},
			wantCode: exitOK,
			wantOut:  []string{`"token": "mjt_test"`, `"expires_at"`, `"mass_url"`, `"linux"`, `"windows"`, `"download"`, `"run"`},
		},
		{
			name:     "explicit runtime flag",
			list:     twoRuntimes,
			extra:    []string{"--runtime", "vllm"},
			wantCode: exitOK,
			wantOut:  []string{"/setup/worker-bin/vllm?"},
		},
		{
			name:     "worker and backend flags embed in url",
			list:     oneRuntime,
			extra:    []string{"--worker", "llama-cpp-worker", "--backend", "cuda"},
			wantCode: exitOK,
			wantOut:  []string{"&worker=llama-cpp-worker&backend=cuda"},
		},
		{
			name:     "json emits worker and backend fields",
			list:     oneRuntime,
			extra:    []string{"--worker", "llama-cpp-worker", "--backend", "cuda", "--json"},
			wantCode: exitOK,
			wantOut:  []string{`"worker": "llama-cpp-worker"`, `"backend": "cuda"`},
		},
		{
			name:     "mass-url overrides the embedded base in commands",
			list:     oneRuntime,
			extra:    []string{"--mass-url", "http://mass.lan:3455"},
			wantCode: exitOK,
			wantOut: []string{
				"http://mass.lan:3455/setup/worker-bin/llama-cpp",
				"./mass-worker-setup --mass-url http://mass.lan:3455",
				".\\mass-worker-setup.exe --mass-url http://mass.lan:3455",
			},
		},
		{
			name:     "mass-url trailing slash trimmed",
			list:     oneRuntime,
			extra:    []string{"--mass-url", "http://mass.lan:3455/"},
			wantCode: exitOK,
			wantOut:  []string{"http://mass.lan:3455/setup/worker-bin/"},
			// A double slash would mean the trailing slash wasn't trimmed.
			wantNotOut: []string{"http://mass.lan:3455//setup"},
		},
		{
			name:     "mass-url overrides mass_url json field",
			list:     oneRuntime,
			extra:    []string{"--mass-url", "http://mass.lan:3455", "--json"},
			wantCode: exitOK,
			wantOut:  []string{`"mass_url": "http://mass.lan:3455"`},
		},
		{
			name:       "json omits worker and backend when unset",
			list:       oneRuntime,
			extra:      []string{"--json"},
			wantCode:   exitOK,
			wantOut:    []string{`"linux"`},
			wantNotOut: []string{`"worker"`, `"backend"`},
		},
		{
			name:     "multiple runtimes require flag",
			list:     twoRuntimes,
			wantCode: exitUsage,
			wantErr:  []string{"multiple runtimes installed", "llama-cpp|vllm"},
		},
		{
			name:     "no runtimes installed",
			list:     func() (*rpc.ListRuntimesResponse, error) { return &rpc.ListRuntimesResponse{}, nil },
			wantCode: exitUsage,
			wantErr:  []string{"no runtimes installed"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := newStubServer(t, &stubHandler{listRuntimes: tt.list, createJoinToken: mintToken})
			args := append([]string{"workers", "join-command", "--addr", url}, tt.extra...)

			var code int
			out, errOut := capture(t, func() { code = runCLI(args) })
			require.Equal(t, tt.wantCode, code)
			for _, sub := range tt.wantOut {
				require.Contains(t, out, sub)
			}
			for _, sub := range tt.wantNotOut {
				require.NotContains(t, out, sub)
			}
			for _, sub := range tt.wantErr {
				require.Contains(t, errOut, sub)
			}
		})
	}
}

func TestWorkersInstallLocal(t *testing.T) {
	t.Run("plumbs flags and reuses bearer token", func(t *testing.T) {
		t.Setenv("MASS_AUTH_TOKEN", "op-token")
		var got *rpc.InstallLocalWorkerRequest
		stub := &stubHandler{installLocal: func(req *rpc.InstallLocalWorkerRequest) (*rpc.InstallLocalWorkerResponse, error) {
			got = req
			return &rpc.InstallLocalWorkerResponse{WorkerPackage: "test-rt-worker", WorkerVersion: "0.1.0", Output: "installed ok"}, nil
		}}
		url := newStubServer(t, stub)

		var code int
		out, errOut := capture(t, func() {
			code = runCLI([]string{"workers", "install-local", "--addr", url,
				"--runtime", "test-rt", "--scope", "user", "--name", "box1"})
		})
		require.Equal(t, exitOK, code)
		require.Equal(t, "test-rt", got.RuntimeName)
		require.Equal(t, "user", got.Scope)
		require.Equal(t, "box1", got.Name)
		// The CLI's own bearer token reached the server (its own header) — this
		// is the token the server reuses for the worker to join with.
		require.Equal(t, "Bearer op-token", stub.lastAuth)
		require.Contains(t, out, "test-rt-worker@0.1.0")
		require.Contains(t, errOut, "installed ok")
	})

	t.Run("server error is a non-zero exit", func(t *testing.T) {
		stub := &stubHandler{installLocal: func(*rpc.InstallLocalWorkerRequest) (*rpc.InstallLocalWorkerResponse, error) {
			return nil, connect.NewError(connect.CodeInvalidArgument, io.EOF)
		}}
		url := newStubServer(t, stub)

		var code int
		_, errOut := capture(t, func() {
			code = runCLI([]string{"workers", "install-local", "--addr", url})
		})
		require.Equal(t, exitError, code)
		require.Contains(t, errOut, "error:")
	})
}

func TestDefaultAddr_EnvOverride(t *testing.T) {
	t.Setenv("MASS_ADDR", "http://example.test:9000")
	require.Equal(t, "http://example.test:9000", defaultAddr())
}

func TestSplitCSV(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"  ", nil},
		{"a", []string{"a"}},
		{"a,b,c", []string{"a", "b", "c"}},
		{" a , b ", []string{"a", "b"}},
		{"a,,b", []string{"a", "b"}},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, splitCSV(tt.in))
	}
}

func TestPeelName(t *testing.T) {
	tests := []struct {
		in       []string
		wantName string
		wantRest []string
	}{
		{[]string{"llama", "--json"}, "llama", []string{"--json"}},
		{[]string{"--json"}, "", []string{"--json"}},
		{nil, "", nil},
		{[]string{"llama"}, "llama", []string{}},
	}
	for _, tt := range tests {
		name, rest := peelName(tt.in)
		require.Equal(t, tt.wantName, name)
		require.Equal(t, tt.wantRest, rest)
	}
}

// TestRunCLI_TableVsJSON confirms the human path emits tab-aligned columns and
// the JSON path emits a JSON object for the same list verb.
func TestRunCLI_TableVsJSON(t *testing.T) {
	stub := &stubHandler{
		listRuntimes: func() (*rpc.ListRuntimesResponse, error) {
			return &rpc.ListRuntimesResponse{Runtimes: []*rpc.Runtime{{RuntimeName: "llama-cpp", Version: "1.0"}}}, nil
		},
	}
	url := newStubServer(t, stub)

	human, _ := capture(t, func() { runCLI([]string{"runtimes", "list", "--addr", url}) })
	require.Contains(t, human, "NAME")
	require.Contains(t, human, "llama-cpp")
	require.False(t, strings.HasPrefix(strings.TrimSpace(human), "{"))

	jsonOut, _ := capture(t, func() { runCLI([]string{"runtimes", "list", "--json", "--addr", url}) })
	require.True(t, strings.HasPrefix(strings.TrimSpace(jsonOut), "{"))
	require.Contains(t, jsonOut, "llama-cpp")
}
