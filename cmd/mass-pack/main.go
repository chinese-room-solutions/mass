// Command mass-pack builds the self-extracting MASS installer: it appends the
// MASS binary (+ any sibling assets) as a payload onto a copy of the mass-setup
// stub, producing one distributable file. `make package` invokes it. The format
// lives in mass-sdk/selfextract, shared with the setup binary that reads it back
// at install time.
//
// With --container it then wraps that installer in the host OS's double-clickable
// artifact (an .AppImage on Linux, a .app on macOS) via mass-sdk/install, so a
// user can launch the terminal wizard from their file manager — a bare binary
// won't run on double-click.
//
// Usage:
//
//	mass-pack --host <setup-exe> --out <installer> [--container --icon <png>] <payload-file>...
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/chinese-room-solutions/mass-sdk/install"
	"github.com/chinese-room-solutions/mass-sdk/selfextract"
)

func main() {
	host := flag.String("host", "", "path to the mass-setup stub exe")
	out := flag.String("out", "", "path to write the packaged installer")
	container := flag.Bool("container", false, "also build a double-clickable .AppImage/.app around the installer")
	icon := flag.String("icon", "", "PNG icon for the container launcher")
	flag.Parse()
	payload := flag.Args()

	if *host == "" || *out == "" || len(payload) == 0 {
		fmt.Fprintln(os.Stderr, "usage: mass-pack --host <setup-exe> --out <installer> [--container --icon <png>] <payload-file>...")
		os.Exit(2)
	}

	if err := selfextract.Pack(*host, *out, payload); err != nil {
		fmt.Fprintln(os.Stderr, "mass-pack:", err)
		os.Exit(1)
	}
	fmt.Printf("mass-pack: wrote %s (%d payload files)\n", *out, len(payload))

	if *container {
		art, err := install.BuildContainer(install.ContainerSpec{
			Name:     "MASS Setup",
			ID:       "mass-setup",
			BinPath:  *out,
			OutDir:   filepath.Dir(*out),
			IconPath: *icon,
			BundleID: "solutions.chineseroom.mass-setup",
			// Size the launched terminal to the wizard's natural height (the
			// tui form's banner + scope/install/data/listen fields + actions +
			// slack ≈ 21 rows). konsole pins its window to this and ignores the
			// form's later shrink, so guessing taller leaves an empty gutter
			// below the form. The form renders down to its content height, so a
			// row or two of drift here doesn't drop it to the linear fallback.
			Rows: 21,
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, "mass-pack:", err)
			os.Exit(1)
		}
		fmt.Printf("mass-pack: wrote %s\n", art)

		// The raw installer stays beside the container: the release uploads it as
		// its own asset — the one the self-update daemon fetches and execs (a
		// container is not something a daemon can run).
	}
}
