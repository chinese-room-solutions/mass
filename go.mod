module github.com/chinese-room-solutions/mass

go 1.26.0

require (
	connectrpc.com/connect v1.19.1
	github.com/KernelPryanic/ctxerr v1.0.0
	github.com/KernelPryanic/golog v1.2.0
	github.com/a-h/templ v0.3.1001
	github.com/chinese-room-solutions/mass-module v0.0.0
	github.com/google/uuid v1.6.0
	github.com/hashicorp/go-hclog v1.6.3
	github.com/hashicorp/go-plugin v1.7.0
	github.com/jchv/go-webview2 v0.0.0-20260205173254-56598839c808
	github.com/prometheus/client_golang v1.23.2
	github.com/rs/zerolog v1.34.0
	github.com/starfederation/datastar-go v1.1.0
	github.com/stretchr/testify v1.11.1
	github.com/tcpipuk/llama-go v0.0.0-00010101000000-000000000000
	github.com/webview/webview_go v0.0.0-20240831120633-6173450d4dd6
	golang.org/x/crypto v0.48.0
	golang.org/x/net v0.49.0
	golang.org/x/sync v0.20.0
	golang.org/x/sys v0.42.0
	google.golang.org/grpc v1.79.1
	google.golang.org/protobuf v1.36.11
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
	gopkg.in/yaml.v3 v3.0.1
	maragu.dev/goqite v0.4.0
	modernc.org/sqlite v1.46.1
)

require (
	github.com/CAFxX/httpcompression v0.0.9 // indirect
	github.com/Masterminds/semver/v3 v3.4.0 // indirect
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/fatih/color v1.16.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/hashicorp/yamux v0.1.2 // indirect
	github.com/jchv/go-winloader v0.0.0-20250406163304-c1995be93bd1 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/oklog/run v1.1.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	golang.org/x/exp v0.0.0-20251023183803-a4bb9ffd2546 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	modernc.org/libc v1.67.6 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

replace github.com/tcpipuk/llama-go => ../llama-go

replace github.com/chinese-room-solutions/mass-embedding => ../mass-embedding

replace github.com/chinese-room-solutions/mass-module => ../mass-module
