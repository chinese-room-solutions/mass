// Package templates: HuggingFace search-dialog rendering.
//
// Uses the SDK's [uikit.RenderHFResults] for parity with other MASS surfaces
// (it understands quantisation badges, vision capability hints, the variant
// overlay panel, etc.). We pass DownloadURL=/api/models/install so the
// SDK-emitted Download buttons hit our installer.
package templates

import (
	"fmt"
	"html"

	"github.com/chinese-room-solutions/mass-sdk/huggingface"
	"github.com/chinese-room-solutions/mass-sdk/uikit"
)

// HFSearchEmpty renders the dialog state when no query is set.
func HFSearchEmpty() string {
	return `<div class="text-center py-6 text-sm" style="color:var(--mass-text-muted)">Enter a query and press Search.</div>`
}

// HFSearchError renders an inline error block.
func HFSearchError(msg string) string {
	return fmt.Sprintf(`<sl-alert variant="danger" open class="mb-2">%s</sl-alert>`, html.EscapeString(msg))
}

// HFSearchResults renders the SDK's full search-results block. The SDK
// emits the inline JS for the variant overlay; we override the Download
// URL to MASS's own installer endpoint.
func HFSearchResults(models []huggingface.Model, query string, hasMore bool) string {
	uiModels := toUIKitModels(models)
	opts := uikit.HFResultsOpts{
		HasMore:     hasMore,
		DownloadURL: "/api/models/install",
		// SkipFooter: SDK's footer is bound to Datastar @post, which we
		// don't drive here. We render our own Show More below.
		SkipFooter: true,
	}
	body := uikit.RenderHFResults("", uiModels, opts)
	return body + renderShowMoreFooter(query, hasMore)
}

// HFSearchAppendRows returns the SDK's row-only HTML (no container) used
// for "Show More" — the JS appends each row inside #pe-hf-list. Followed
// by a replacement footer.
func HFSearchAppendRows(models []huggingface.Model, query string, hasMore bool) string {
	uiModels := toUIKitModels(models)
	rows := uikit.RenderHFResultRows(uiModels, nil)
	return `<div id="hf-search-append">` + rows + `</div>` + renderShowMoreFooter(query, hasMore)
}

// renderShowMoreFooter renders a self-contained Show More button that
// calls our window.__massHFShowMore JS helper. Replaces the SDK's
// Datastar-bound footer.
func renderShowMoreFooter(query string, hasMore bool) string {
	if !hasMore {
		return `<div id="hf-search-footer"></div>`
	}
	return fmt.Sprintf(
		`<div id="hf-search-footer" class="mt-2 text-center"><sl-button size="small" `+
			`onclick="window.__massHFShowMore('%s')"><sl-icon slot="prefix" name="chevron-down"></sl-icon>Show More</sl-button></div>`,
		jsStringEscape(query))
}

// toUIKitModels projects mass-sdk huggingface.Model into the SDK's
// HFResultModel render shape. The file list is taken verbatim — runtime
// gateways have already filtered it via FilterHFFiles upstream of this
// call, so MASS holds no extension or filename rules of its own.
func toUIKitModels(models []huggingface.Model) []uikit.HFResultModel {
	out := make([]uikit.HFResultModel, len(models))
	for i, m := range models {
		files := make([]uikit.HFResultFile, len(m.Files))
		for j, f := range m.Files {
			files[j] = uikit.HFResultFile{Filename: f.Filename, SizeBytes: f.SizeBytes}
		}
		out[i] = uikit.HFResultModel{
			RepoID:      m.RepoID,
			Description: m.Description,
			Downloads:   m.Downloads,
			Likes:       m.Likes,
			Params:      m.Params,
			PipelineTag: m.PipelineTag,
			AvatarURL:   m.AvatarURL,
			Files:       files,
		}
	}
	return out
}
