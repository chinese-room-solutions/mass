# MASS Build System
#
# MASS is a pure-Go scheduler/UI/API with no inference-library dependencies;
# inference runs on workers (e.g. mass-worker-llama-cpp) over gRPC. The only CGO
# is the native GUI on Linux/macOS (webview/webview_go) — see the build target.
#
# Usage: make <target> [VAR=val]
#
# Windows (Git Bash / MSYS2) builds pure Go (no CGO); Linux / macOS link the
# native webview (CGO). Both run native commands from this one Makefile.

# -- Platform detection -------------------------------------------------------

UNAME_S := $(shell uname -s)
IS_WIN  := $(findstring MINGW,$(UNAME_S))$(findstring MSYS,$(UNAME_S))$(findstring CYGWIN,$(UNAME_S))
IS_MAC  := $(filter Darwin,$(UNAME_S))

# -- Common variables ---------------------------------------------------------

BIN_DIR := bin
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

# -- WebKitGTK discovery (Linux GUI) -----------------------------------------
#
# webview/webview_go's cgo directive asks pkg-config for the fixed module name
# `webkit2gtk-4.0`, but distros now ship `webkit2gtk-4.1` (libsoup3) and will
# ship later 4.x — there is no 4.0 .pc on a current Fedora/Debian. Rather than
# pin a version, discover whichever webkit2gtk-4.x pkg-config knows about and
# generate a build-local alias `.pc` under that fixed name so the dep resolves.
# The `4.0` literal is the dependency's required name, not a version we chose.
PKGCONFIG_DIR := $(CURDIR)/$(BIN_DIR)/pkgconfig
WEBKIT_PC     := $(shell pkg-config --list-all 2>/dev/null | awk '/^webkit2gtk-4\./{print $$1; exit}')

# -- Windows: native build (MSYS/MinGW make + bash) --------------------------
#
# MASS is pure Go on Windows — the webview is jchv/go-webview2 (no CGO, the
# WebView2 runtime ships with the OS), so the build is CGO-free and skips the
# WebKitGTK dance the Linux branch needs. Recipes run under the same MSYS bash
# as the Linux/macOS branch; the only deltas are the .exe suffix and the
# -H windowsgui ldflag (the --headless flag re-attaches a console at runtime —
# cmd/mass/console_windows.go). There is no separate build script.

ifdef IS_WIN

MinGW := /c/msys64/mingw64/bin

# `go test -race` uses Go's race detector, which is implemented in C and so
# requires a C toolchain even though MASS itself is pure Go. On Windows we
# pull in MinGW for that reason — `make build` and `make lint` don't need it.
RACE_ENV := PATH="$(MinGW):$$PATH" CGO_ENABLED=1

BINARY := $(BIN_DIR)/mass.exe
SETUP_BINARY := $(BIN_DIR)/mass-setup.exe
DIST_DIR := dist

.PHONY: build build-setup package build-web run clean help lint test unittest vulncheck fmt tidy

build-web:
	@echo "==> Generating web assets..."
	@for f in internal/web/templates/*.templ; do \
		go tool templ generate -f "$$f"; \
	done
	@echo "    templ generated"
	@# Stage the shared theme from the pinned mass-sdk module so the Tailwind
	@# build can @import it (web/input.tw.css → ./vendor/theme.css). Same recipe
	@# as the Linux/macOS branch — edit the theme in mass-sdk, not here.
	@# `go list -m -f {{.Dir}}` returns an empty .Dir until the module is in the
	@# local cache, so download it first (a no-op once cached) — otherwise a fresh
	@# checkout or a freshly bumped pin fails here with SDK_DIR=''.
	@go mod download github.com/chinese-room-solutions/mass-sdk
	@SDK_DIR="$$(go list -m -f '{{.Dir}}' github.com/chinese-room-solutions/mass-sdk)"; \
		if [ -z "$$SDK_DIR" ] || [ ! -f "$$SDK_DIR/uikit/theme.css" ]; then \
			echo "    error: mass-sdk uikit/theme.css not found (SDK_DIR='$$SDK_DIR')" >&2; \
			exit 1; \
		fi; \
		mkdir -p web/vendor; \
		rm -f web/vendor/theme.css; \
		cp "$$SDK_DIR/uikit/theme.css" web/vendor/theme.css; \
		chmod u+w web/vendor/theme.css; \
		echo "    theme.css staged from $$(go list -m -f '{{.Version}}' github.com/chinese-room-solutions/mass-sdk)"
	@if [ -f web/package.json ]; then \
		cd web && \
		if command -v bun >/dev/null 2>&1; then \
			bun install --frozen-lockfile 2>/dev/null || true; \
			bun run build:css; \
		elif command -v npx >/dev/null 2>&1; then \
			npm install 2>/dev/null || true; \
			npx postcss input.tw.css -o public/dist.css --config postcss.config.js; \
		else \
			echo "    error: no JS toolchain found — install bun or Node to build the CSS" >&2; \
			exit 1; \
		fi; \
		if [ ! -s public/dist.css ]; then \
			echo "    error: CSS build produced no public/dist.css" >&2; \
			exit 1; \
		fi; \
		echo "    CSS built"; \
	fi

# -H windowsgui marks the binary as a GUI app so Windows doesn't allocate a
# console for it; --headless re-attaches to the parent console at runtime.
build: build-web
	@echo "==> Building mass ($(VERSION))..."
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 go build -ldflags '-H windowsgui -X main.version=$(VERSION)' -o $(BINARY) ./cmd/mass
	@echo "    Built: $(BINARY)"

# Server build: the nogui tag drops the webview/tray, and the binary keeps its
# console (no -H windowsgui) — right for services and terminals.
build-headless: build-web
	@echo "==> Building mass headless ($(VERSION))..."
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 go build -tags nogui -ldflags '-X main.version=$(VERSION)' -o $(BINARY) ./cmd/mass
	@echo "    Built: $(BINARY) (headless)"

# The terminal installer. A console app (NO -H windowsgui) — it talks to a TTY.
build-setup:
	@echo "==> Building mass-setup ($(VERSION))..."
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 go build -ldflags '-X main.version=$(VERSION)' -o $(SETUP_BINARY) ./cmd/mass-setup
	@echo "    Built: $(SETUP_BINARY)"

# The single-file self-extracting installer: the mass-setup stub with the MASS
# binary appended as a payload (mass-sdk/selfextract). On Windows the GUI is
# pure-Go (jchv/go-webview2; the WebView2 runtime ships with the OS), so there
# are no sibling DLLs to bundle — the payload is just mass.exe. mass-pack is a
# build-time tool, so it's `go run` transiently rather than left in bin/.
package: build build-setup
	@echo "==> Packaging mass-setup installer ($(VERSION))..."
	@mkdir -p $(DIST_DIR)
	go run ./cmd/mass-pack --host $(SETUP_BINARY) --out $(DIST_DIR)/mass-setup.exe $(BINARY)
	@echo "    Installer: $(DIST_DIR)/mass-setup.exe"

run: build
	@echo "==> Starting mass..."
	./$(BINARY)

clean:
	@rm -rf $(BIN_DIR)
	@echo "    Removed $(BIN_DIR)/"

help:
	@echo "  MASS build (Windows, pure Go — workers handle inference)"
	@echo "  Usage: make <target>"
	@echo "    build      Build $(BINARY) (web assets + Go build)"
	@echo "    build-web  Generate web assets only"
	@echo "    run        Build and start mass"
	@echo "    clean      Remove build outputs"
	@echo "    lint / test / vulncheck / fmt / tidy"

lint:
	@# golangci-lint must be built with a toolchain >= the repo's go directive or it refuses to load.
	GOTOOLCHAIN=go$$(go list -m -f '{{.GoVersion}}') go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.5.0 run --timeout 10m ./...

unittest:
	go test ./internal/... ./pkg/... -short -count=1

test:
	$(RACE_ENV) go test ./internal/... ./pkg/... -race -covermode=atomic -coverprofile=coverage.out -count=1 -timeout 15m

vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

fmt:
	gofmt -s -w .

tidy:
	go mod tidy

# -- Linux / macOS: native build ----------------------------------------------

else

ifneq ($(wildcard $(HOME)/.goenv/shims),)
  export PATH := $(HOME)/.goenv/bin:$(HOME)/.goenv/shims:$(PATH)
endif

.PHONY: build build-setup package build-web site site-serve run bundle-macos bundle-app test unittest vulncheck lint fmt tidy clean help

BINARY := $(BIN_DIR)/mass
SETUP_BINARY := $(BIN_DIR)/mass-setup
DIST_DIR := dist

# Pin the macOS deployment floor. Otherwise cgo bakes minos = the builder's OS,
# so a binary built on macOS 26 won't launch on any older Mac. 12.0 (Monterey)
# is a safe floor.
ifneq ($(IS_MAC),)
export MACOSX_DEPLOYMENT_TARGET := 12.0
endif

# -- Generate web assets (templ + Tailwind CSS) -------------------------------

build-web:
	@echo "==> Generating web assets..."
	@for f in internal/web/templates/*.templ; do \
		go tool templ generate -f "$$f"; \
	done
	@echo "    templ generated"
	@# Stage the shared theme from the pinned mass-sdk module so the Tailwind
	@# build can @import it. The module dir is version-matched and read-only, so
	@# we copy it into the (gitignored) web/vendor/. No sibling checkout — but the
	@# module must be in the local cache for go list to report a non-empty .Dir,
	@# so download it first (a no-op once cached). Edit the theme in mass-sdk, not here.
	@go mod download github.com/chinese-room-solutions/mass-sdk
	@SDK_DIR="$$(go list -m -f '{{.Dir}}' github.com/chinese-room-solutions/mass-sdk)"; \
		if [ -z "$$SDK_DIR" ] || [ ! -f "$$SDK_DIR/uikit/theme.css" ]; then \
			echo "    error: mass-sdk uikit/theme.css not found (SDK_DIR='$$SDK_DIR')" >&2; \
			exit 1; \
		fi; \
		mkdir -p web/vendor; \
		install -m 0644 "$$SDK_DIR/uikit/theme.css" web/vendor/theme.css; \
		echo "    theme.css staged from $$(go list -m -f '{{.Version}}' github.com/chinese-room-solutions/mass-sdk)"
	@if [ -f web/package.json ]; then \
		cd web && \
		if command -v bun >/dev/null 2>&1; then \
			bun install --frozen-lockfile 2>/dev/null || true; \
			bun run build:css; \
		elif command -v npx >/dev/null 2>&1; then \
			npm install 2>/dev/null || true; \
			npx postcss input.tw.css -o public/dist.css --config postcss.config.js; \
		else \
			echo "    error: no JS toolchain found — install bun ('brew install oven-sh/bun/bun') or Node to build the CSS" >&2; \
			exit 1; \
		fi; \
		if [ ! -s public/dist.css ]; then \
			echo "    error: CSS build produced no public/dist.css" >&2; \
			exit 1; \
		fi; \
		echo "    CSS built"; \
	fi

# -- Stage the landing page (site/) -------------------------------------------
#
# The GitHub Pages site reuses the SDK look: theme.css verbatim, synthwave
# wrapped into its overlay selector (the raw file is declarations-only — the
# same wrap uikit/themes.go applies at runtime), and the SDK-vendored Datastar.
# Everything lands in the gitignored site/vendor/; the pages workflow runs this
# before uploading site/ as the Pages artifact.

site:
	@echo "==> Staging site vendor assets..."
	@go mod download github.com/chinese-room-solutions/mass-sdk
	@SDK_DIR="$$(go list -m -f '{{.Dir}}' github.com/chinese-room-solutions/mass-sdk)"; \
		if [ -z "$$SDK_DIR" ] || [ ! -f "$$SDK_DIR/uikit/theme.css" ]; then \
			echo "    error: mass-sdk uikit/theme.css not found (SDK_DIR='$$SDK_DIR')" >&2; \
			exit 1; \
		fi; \
		mkdir -p site/vendor; \
		install -m 0644 "$$SDK_DIR/uikit/theme.css" site/vendor/theme.css; \
		{ printf 'html.sl-theme-synthwave {\n'; \
		  cat "$$SDK_DIR/uikit/themes/synthwave.css"; \
		  printf '}\n'; } > site/vendor/synthwave.css; \
		install -m 0644 "$$SDK_DIR/uikit/assets/datastar/datastar.js" site/vendor/datastar.js; \
		echo "    site/vendor staged from mass-sdk $$(go list -m -f '{{.Version}}' github.com/chinese-room-solutions/mass-sdk)"

# -- Serve the landing page locally -------------------------------------------
#
# Stages the vendor assets, then serves site/ over HTTP (the Datastar module
# script won't load over file://). Override the port with `make site-serve
# SITE_PORT=1234`.
SITE_PORT ?= 8931
site-serve: site
	@echo "==> Serving site/ at http://localhost:$(SITE_PORT) (Ctrl-C to stop)"
	@python3 -m http.server -d site $(SITE_PORT)

# -- Build mass binary --------------------------------------------------------
#
# MASS itself is pure Go, but on Linux/macOS the GUI links webview/webview_go
# (CGO + GTK/WebKit), so CGO must be enabled here. Build-time prerequisites:
# a C toolchain plus gtk+-3.0 and a webkit2gtk-4.x dev package. The webkit
# alias `.pc` (see WEBKIT_PC above) is generated into PKGCONFIG_DIR and put on
# PKG_CONFIG_PATH so the dep's fixed `webkit2gtk-4.0` name resolves to whatever
# the host ships. (On Windows the webview is pure Go via jchv/go-webview2, so
# the Windows branch above builds CGO-free and skips all of this.)

build: build-web
	@echo "==> Building mass ($(VERSION))..."
	@mkdir -p $(BIN_DIR)
	@if [ -n "$(WEBKIT_PC)" ] && [ "$(WEBKIT_PC)" != "webkit2gtk-4.0" ]; then \
		mkdir -p $(PKGCONFIG_DIR); \
		printf 'Name: webkit2gtk-4.0 alias\nDescription: alias for %s\nVersion: 0\nRequires: %s\n' "$(WEBKIT_PC)" "$(WEBKIT_PC)" > $(PKGCONFIG_DIR)/webkit2gtk-4.0.pc; \
		echo "    Using $(WEBKIT_PC) (aliased as webkit2gtk-4.0)"; \
	fi
	PKG_CONFIG_PATH="$(PKGCONFIG_DIR):$$PKG_CONFIG_PATH" \
		CGO_ENABLED=1 go build -ldflags '-X main.version=$(VERSION)' -o $(BINARY) ./cmd/mass
	@echo "    Built: $(BINARY)"
ifneq ($(IS_MAC),)
	@# On macOS the bare binary opens a Terminal window when launched from
	@# Finder; wrap it in MASS.app so `make build` produces a directly
	@# double-clickable GUI app. The binary stays at $(BINARY) for CLI use.
	@$(MAKE) --no-print-directory bundle-app
endif

# Server build: the nogui tag drops the webview/tray — CGO-free, so no C
# toolchain and no GTK/WebKit dev packages needed, and the binary is static.
build-headless: build-web
	@echo "==> Building mass headless ($(VERSION))..."
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 go build -tags nogui -ldflags '-X main.version=$(VERSION)' -o $(BINARY) ./cmd/mass
	@echo "    Built: $(BINARY) (headless, pure Go)"

# The terminal installer. Pure Go (term/tui/install — no webview), so it builds
# CGO-free and needs none of the GTK/WebKit prerequisites the GUI build does.
build-setup:
	@echo "==> Building mass-setup ($(VERSION))..."
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 go build -ldflags '-X main.version=$(VERSION)' -o $(SETUP_BINARY) ./cmd/mass-setup
	@echo "    Built: $(SETUP_BINARY)"

# The single-file self-extracting installer: the mass-setup stub with the MASS
# binary appended as a payload (mass-sdk/selfextract). The GUI links GTK/WebKit
# dynamically against the host's system libraries (not bundled), so the payload
# is just the mass binary; the target machine provides the toolkit, same as a
# distro-packaged app. mass-pack is a build-time tool, so it's `go run`
# transiently rather than left in bin/.
package: build build-setup
	@echo "==> Packaging mass-setup installer ($(VERSION))..."
	@mkdir -p $(DIST_DIR)
	@# --container wraps the installer in a double-clickable .AppImage (Linux) /
	@# .app (macOS), so a user can launch the wizard from their file manager — a
	@# bare binary won't run on double-click. The wrapped installer then creates
	@# the app-menu launcher for the installed MASS app.
	@# mass-pack removes the loose installer stub after wrapping it, so dist/ holds
	@# only the double-clickable artifact (the .AppImage/.app, already executable).
	go run ./cmd/mass-pack --host $(SETUP_BINARY) --out $(DIST_DIR)/mass-setup \
		--container --icon internal/icon/icon.png $(BINARY)
ifneq ($(IS_MAC),)
	@# Zip the .app with ditto so recipients get a transfer-safe archive: a raw
	@# .app sent over chat/AirDrop/cloud loses the executable bits on its launcher
	@# and payload, and the wizard then flashes closed ("operation not permitted").
	@# ditto preserves the bundle tree + perms; the user unzips and it just runs.
	@rm -f "$(DIST_DIR)/MASS-Setup.zip"
	@ditto -c -k --keepParent "$(DIST_DIR)/MASS Setup.app" "$(DIST_DIR)/MASS-Setup.zip"
	@echo "    Installer ready: $(DIST_DIR)/MASS-Setup.zip (share this; unzip on the target Mac)"
else
	@echo "    Installer ready in $(DIST_DIR)/ (double-clickable)"
endif

run: build
	@echo "==> Starting mass..."
	./$(BINARY)

# -- macOS .app bundle --------------------------------------------------------
#
# A bare Mach-O binary launched from Finder opens a Terminal window. Wrapping it
# in a .app bundle (Info.plist + the binary under Contents/MacOS) makes Finder
# treat it as a GUI app — no Terminal. The icon is derived from the shared
# icon.png via the macOS-only iconutil; if that's unavailable the bundle still
# builds without an icon.

APP_BUNDLE := $(BIN_DIR)/MASS.app

# bundle-macos builds the binary then wraps it. bundle-app does only the
# wrapping and has no `build` prereq, so `build` can invoke it on macOS without
# recursing back into itself.
bundle-macos: build bundle-app

bundle-app:
	@echo "==> Bundling $(APP_BUNDLE) ($(VERSION))..."
	@rm -rf $(APP_BUNDLE)
	@mkdir -p $(APP_BUNDLE)/Contents/MacOS $(APP_BUNDLE)/Contents/Resources
	@cp $(BINARY) $(APP_BUNDLE)/Contents/MacOS/mass
	@if command -v iconutil >/dev/null 2>&1 && command -v sips >/dev/null 2>&1; then \
		iconset=$$(mktemp -d)/icon.iconset; mkdir -p "$$iconset"; \
		for s in 16 32 128 256 512; do \
			sips -z $$s $$s internal/icon/icon.png --out "$$iconset/icon_$${s}x$${s}.png" >/dev/null; \
			d=$$((s*2)); \
			sips -z $$d $$d internal/icon/icon.png --out "$$iconset/icon_$${s}x$${s}@2x.png" >/dev/null; \
		done; \
		iconutil -c icns "$$iconset" -o $(APP_BUNDLE)/Contents/Resources/icon.icns; \
		echo "    icon.icns generated"; \
	else \
		echo "    iconutil/sips unavailable — bundling without an icon"; \
	fi
	@printf '%s\n' \
		'<?xml version="1.0" encoding="UTF-8"?>' \
		'<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">' \
		'<plist version="1.0">' \
		'<dict>' \
		'	<key>CFBundleName</key><string>MASS</string>' \
		'	<key>CFBundleDisplayName</key><string>MASS</string>' \
		'	<key>CFBundleIdentifier</key><string>solutions.chineseroom.mass</string>' \
		'	<key>CFBundleVersion</key><string>$(VERSION)</string>' \
		'	<key>CFBundleShortVersionString</key><string>$(VERSION)</string>' \
		'	<key>CFBundlePackageType</key><string>APPL</string>' \
		'	<key>CFBundleExecutable</key><string>mass</string>' \
		'	<key>CFBundleIconFile</key><string>icon</string>' \
		'	<key>NSHighResolutionCapable</key><true/>' \
		'</dict>' \
		'</plist>' > $(APP_BUNDLE)/Contents/Info.plist
	@# Ad-hoc sign (no Developer ID) so Gatekeeper stops reporting the bundle as
	@# "damaged". A downloaded copy still shows the "unidentified developer"
	@# prompt — open it once via right-click > Open (or strip quarantine with
	@# `xattr -dr com.apple.quarantine MASS.app`).
	@if command -v codesign >/dev/null 2>&1; then \
		codesign --force --sign - --timestamp=none $(APP_BUNDLE)/Contents/MacOS/mass; \
		codesign --force --sign - --timestamp=none $(APP_BUNDLE); \
		echo "    ad-hoc signed"; \
	else \
		echo "    codesign unavailable — bundle left unsigned"; \
	fi
	@echo "    Built: $(APP_BUNDLE)"

unittest:
	go test ./internal/... ./pkg/... -short -count=1

test:
	go test ./internal/... ./pkg/... -race -covermode=atomic -coverprofile=coverage.out -count=1 -timeout 15m

vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

lint:
	@# golangci-lint must be built with a toolchain >= the repo's go directive or it refuses to load.
	GOTOOLCHAIN=go$$(go list -m -f '{{.GoVersion}}') go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.5.0 run --timeout 10m ./...

fmt:
	gofmt -s -w .

tidy:
	go mod tidy

clean:
	rm -rf $(BIN_DIR)
	@echo "Cleaned."

help:
	@echo ""
	@echo "  MASS Build System (pure Go — workers handle inference)"
	@echo "  ======================================================"
	@echo ""
	@echo "  Usage: make <target>"
	@echo ""
	@echo "  Targets:"
	@echo "    build       Build mass binary (web assets + Go build)"
	@echo "    build-headless  Server build: no window/tray, CGO-free"
	@echo "    build-web     Generate web assets only (templ + Tailwind)"
	@echo "    site          Stage the landing page's vendor assets (site/)"
	@echo "    site-serve    Stage + serve the landing page locally (SITE_PORT=8931)"
	@echo "    run           Build and start mass"
	@echo "    bundle-macos  Build a MASS.app bundle (macOS, no Terminal)"
	@echo "    test        Run tests with -race"
	@echo "    unittest    Run tests with -short"
	@echo "    lint        Run golangci-lint"
	@echo "    vulncheck   Run govulncheck"
	@echo "    fmt         Format Go code"
	@echo "    tidy        Run go mod tidy"
	@echo "    clean       Remove build outputs"
	@echo ""
	@echo "  Note: MASS no longer links llama.cpp. Install and run"
	@echo "  mass-worker-<runtime> (e.g. mass-worker-llama-cpp) separately"
	@echo "  to provide inference capacity."
	@echo ""

endif
