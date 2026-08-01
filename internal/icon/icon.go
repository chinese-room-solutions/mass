// Package icon embeds the MASS application icon for the window/taskbar and the
// system-tray.
package icon

import _ "embed"

// PNG is the MASS icon as PNG bytes, passed to the webview (window/taskbar) and
// the tray.
//
//go:embed icon.png
var PNG []byte
