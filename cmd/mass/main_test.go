package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// TestMain refuses to run the suite when this binary was invoked as the CLI
// rather than as a test run.
//
// spawnDetached launches the on-demand daemon by re-executing os.Executable(),
// which under `go test` is the test binary — and Go's flag parsing stops at the
// first non-flag argument, so `mass.test serve --idle-timeout 5m` would not
// fail: it would run the whole suite again, each copy free to spawn more. No
// test reaches that path today; this keeps it that way.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		fmt.Fprintf(os.Stderr, "mass test binary invoked as %q, not a test run\n",
			strings.Join(os.Args[1:], " "))
		os.Exit(2)
	}
	os.Exit(m.Run())
}
