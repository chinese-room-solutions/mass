// Package huggingface provides MASS-specific HuggingFace download functionality.
// The search client and types live in the SDK: github.com/chinese-room-solutions/mass-sdk/huggingface
package huggingface

import sdkhf "github.com/chinese-room-solutions/mass-sdk/huggingface"

// SanitizeRepoID is re-exported from the SDK for use by Download/TempFilePath
// in this package.
var SanitizeRepoID = sdkhf.SanitizeRepoID
