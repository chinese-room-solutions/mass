package web

import (
	"net/http"

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/internal/web/templates"
	"golang.org/x/crypto/bcrypt"
)

// logLevelString converts a config.LogLevel to its string representation.
func logLevelString(lvl config.LogLevel) string {
	b, _ := lvl.MarshalText()
	return string(b)
}

func (h *Handler) handlePageDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	apps := buildAppList(h.cfg, h.orch.GetAllApps())

	active := ""

	theme := h.cfg.Theme
	if theme == "" {
		theme = "dark"
	}

	h.authHashMu.RLock()
	hasToken := len(h.authHash) > 0
	h.authHashMu.RUnlock()

	cfgDir, _ := config.DefaultDir()
	data := templates.DashboardData{
		Apps:             apps,
		ActiveApp:        active,
		ListenAddr:       h.cfg.ListenAddr,
		DataDir:          h.cfg.DataDir,
		AuthTokenSet:     hasToken,
		ModelIdleTimeout: h.cfg.ModelIdleTimeout,
		AppIdleTimeout:   h.cfg.AppIdleTimeout,
		ResultTTL:        h.cfg.ResultTTL,
		LogLevel:         logLevelString(h.cfg.Logger.Level),
		Theme:            theme,
		DevMode:          h.cfg.DevMode,
		ConfigDir:        cfgDir,
		LogsDir:          config.LogsDir(cfgDir),
		AgentsHTML:       templates.RenderAgentsList(h.buildWorkerViews()),
		TLSEnabled:       h.cfg.TLS.Enabled,
		TLSCertFile:      h.cfg.TLS.CertFile,
	}

	page := templates.DashboardPage(data)
	if err := templates.Layout("MASS", page, theme).Render(r.Context(), w); err != nil {
		h.logger.Error().Err(err).Msg("failed to render dashboard")
	}
}

func (h *Handler) handlePageLogin(w http.ResponseWriter, r *http.Request) {
	page := templates.LoginPage("")
	if err := templates.Layout("MASS - Login", page, "dark").Render(r.Context(), w); err != nil {
		h.logger.Error().Err(err).Msg("failed to render login page")
	}
}

func (h *Handler) handlePostLogin(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	token := r.FormValue("token")

	h.authHashMu.RLock()
	hash := h.authHash
	h.authHashMu.RUnlock()

	if len(hash) == 0 {
		// No auth configured — redirect to dashboard.
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}

	if bcrypt.CompareHashAndPassword(hash, []byte(token)) != nil {
		page := templates.LoginPage("Invalid token")
		if err := templates.Layout("MASS - Login", page, "dark").Render(r.Context(), w); err != nil {
			h.logger.Error().Err(err).Msg("failed to render login page")
		}
		return
	}

	sessionID, err := h.sessions.Create()
	if err != nil {
		h.logger.Error().Err(err).Msg("creating session")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	setSessionCookie(w, sessionID)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func infoVersion(mp *scheduler.ManagedApp) string {
	if mp.Info != nil {
		return mp.Info.Version
	}
	if mp.DiskMeta != nil {
		return mp.DiskMeta.Version
	}
	return ""
}

func appHasIcon(mp *scheduler.ManagedApp) bool {
	return mp != nil && mp.DiskMeta != nil && mp.DiskMeta.Icon != ""
}
