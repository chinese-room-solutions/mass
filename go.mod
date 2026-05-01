module github.com/chinese-room-solutions/mass

go 1.26.0

require (
	connectrpc.com/connect v1.19.1
	github.com/KernelPryanic/ctxerr v1.0.0
	github.com/KernelPryanic/golog v1.2.0
	github.com/KimMachineGun/automemlimit v0.7.5
	github.com/a-h/templ v0.3.1001
	github.com/chinese-room-solutions/mass-proto/gen/go v0.0.0
	github.com/google/uuid v1.6.0
	github.com/jchv/go-webview2 v0.0.0-20260205173254-56598839c808
	github.com/prometheus/client_golang v1.23.2
	github.com/rs/zerolog v1.34.0
	github.com/shirou/gopsutil/v3 v3.24.5
	github.com/stretchr/testify v1.11.1
	github.com/webview/webview_go v0.0.0-20240831120633-6173450d4dd6
	golang.org/x/crypto v0.48.0
	golang.org/x/net v0.49.0
	golang.org/x/sys v0.42.0
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
	gopkg.in/yaml.v3 v3.0.1
	maragu.dev/goqite v0.4.0
	modernc.org/sqlite v1.46.1
)

require (
	github.com/CAFxX/httpcompression v0.0.9 // indirect
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/chinese-room-solutions/mass-sdk v0.0.0
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/fatih/color v1.16.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/pprof v0.0.0-20250923004556-9e5a51aed1e8 // indirect
	github.com/hashicorp/go-hclog v1.6.3
	github.com/hashicorp/go-plugin v1.8.0
	github.com/hashicorp/yamux v0.1.2 // indirect
	github.com/jchv/go-winloader v0.0.0-20250406163304-c1995be93bd1 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/oklog/run v1.1.0 // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/starfederation/datastar-go v1.2.1
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	golang.org/x/exp v0.0.0-20251023183803-a4bb9ffd2546 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260120221211-b8f7ae30c516 // indirect
	google.golang.org/grpc v1.80.0
	google.golang.org/protobuf v1.36.11
	modernc.org/libc v1.67.6 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

replace github.com/chinese-room-solutions/mass-proto/gen/go => ../mass-proto/gen/go

replace github.com/chinese-room-solutions/mass-sdk => ../mass-sdk
