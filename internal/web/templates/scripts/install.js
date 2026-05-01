// Install dialog + downloads SSE for the Models tab.
//
// Three responsibilities:
//   1. Open the Install New Model dialog and drive the HF search flow.
//   2. Open a file-browser dialog for "Browse Local" and POST /api/models/import.
//   3. Subscribe to /api/models/downloads/events and render per-file
//      progress rows under #models-downloads.
//
// Server emits SDK-style HF result rows (uikit.RenderHFResults). The SDK
// expects window.__hfOpen / __hfClose / __hfDownload helpers — we define
// them here once because inline <script>s injected via innerHTML don't run.

(function() {
  // --- Runtime selection -------------------------------------------------
  // The Install dialog is always scoped to one runtime. The selector tabs
  // at the top of the dialog drive _selectedRuntime, which threads through
  // search, install, and local-import so the operator always knows where
  // a model will land.
  var _selectedRuntime = '';

  function renderRuntimeTabs(running) {
    var host = document.getElementById('mass-install-runtime-tabs');
    if (!host) return;
    host.innerHTML = '';
    running.forEach(function(rt) {
      var tab = document.createElement('button');
      tab.type = 'button';
      tab.dataset.runtimeName = rt.runtime_name;
      var active = rt.runtime_name === _selectedRuntime;
      tab.className = 'px-3 py-1.5 text-xs font-medium rounded transition-colors';
      tab.style.cssText = active
        ? 'background:var(--mass-bg-active);color:var(--mass-text);'
        : 'background:transparent;color:var(--mass-text-muted);';
      tab.textContent = rt.display_name || rt.runtime_name;
      tab.onclick = function() {
        _selectedRuntime = rt.runtime_name;
        renderRuntimeTabs(running);
        // Re-run any in-flight search so results match the new filter set.
        var input = document.getElementById('mass-install-query');
        if (input && input.value.trim()) window.__massHFSearch();
      };
      host.appendChild(tab);
    });
  }

  // --- Dialog open / search ----------------------------------------------
  window.__massOpenInstall = function() {
    fetch('/api/runtimes').then(function(r) { return r.json(); }).then(function(rs) {
      var running = (Array.isArray(rs) ? rs : []).filter(function(rt) { return rt && rt.running; });
      if (running.length === 0) {
        window.massAlert('no runtime gateway is running - start one in the Runtimes tab',
          {title: 'Cannot Install', variant: 'danger'});
        return;
      }
      running.sort(function(a, b) { return a.runtime_name < b.runtime_name ? -1 : 1; });
      // Default to the first runtime; sticky across opens within one session
      // (the previous selection only persists if it's still running).
      var stillRunning = running.some(function(rt) { return rt.runtime_name === _selectedRuntime; });
      if (!stillRunning) _selectedRuntime = running[0].runtime_name;
      renderRuntimeTabs(running);

      var dlg = document.getElementById('mass-install-model-dialog');
      if (!dlg) return;
      dlg.show();
      var input = document.getElementById('mass-install-query');
      if (input) {
        setTimeout(function() { input.focus(); }, 50);
        input.addEventListener('keydown', function(e) {
          if (e.key === 'Enter') {
            e.preventDefault();
            window.__massHFSearch();
          }
        });
      }
    });
  };

  window.__massHFSearch = function() {
    var input = document.getElementById('mass-install-query');
    var q = (input && input.value || '').trim();
    var resultsEl = document.getElementById('mass-install-results');
    if (!resultsEl) return;
    if (!q) {
      resultsEl.innerHTML = '<div class="text-center py-6 text-sm" style="color:var(--mass-text-muted)">Enter a query and press Search.</div>';
      return;
    }
    resultsEl.innerHTML = '<div class="flex items-center justify-center py-12"><sl-spinner style="font-size:1.5rem;--track-width:3px"></sl-spinner></div>';
    fetch('/api/models/search', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({query: q, runtime_name: _selectedRuntime})
    })
      .then(function(r) { return r.text(); })
      .then(function(html) { resultsEl.innerHTML = html; })
      .catch(function(err) {
        resultsEl.innerHTML = '<sl-alert variant="danger" open>Search failed: ' +
          String(err).replace(/</g, '&lt;') + '</sl-alert>';
      });
  };

  window.__massHFShowMore = function(query) {
    var footer = document.getElementById('hf-search-footer');
    var rowsContainer = document.getElementById('pe-hf-list'); // SDK's row container
    if (!footer || !rowsContainer) return;
    footer.innerHTML = '<div class="flex items-center justify-center py-2"><sl-spinner style="font-size:1rem;--track-width:2px"></sl-spinner></div>';
    fetch('/api/models/search/more', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({query: query, runtime_name: _selectedRuntime})
    })
      .then(function(r) { return r.text(); })
      .then(function(html) {
        var wrap = document.createElement('div');
        wrap.innerHTML = html;
        var append = wrap.querySelector('#hf-search-append');
        if (append) {
          while (append.firstChild) rowsContainer.appendChild(append.firstChild);
        }
        var newFooter = wrap.querySelector('#hf-search-footer');
        if (newFooter) footer.replaceWith(newFooter);
      })
      .catch(function() { footer.innerHTML = ''; });
  };

  // --- SDK-compatible HF row helpers -------------------------------------
  // The SDK's RenderHFResults emits inline <script> for these but inline
  // scripts don't execute when injected via innerHTML — so we own them.
  var _hfOverlay = null;
  var _hfActiveRow = null;
  function ensureOverlay() {
    if (_hfOverlay) return _hfOverlay;
    _hfOverlay = document.getElementById('hf-panel-overlay');
    if (_hfOverlay) return _hfOverlay;
    _hfOverlay = document.createElement('div');
    _hfOverlay.id = 'hf-panel-overlay';
    _hfOverlay.style.cssText = 'display:none;position:fixed;z-index:9999;min-width:340px;max-width:540px;max-height:calc(100vh - 16px);overflow-y:auto';
    _hfOverlay.className = 'bg-neutral-800 border border-neutral-700 rounded-lg shadow-2xl';
    document.body.appendChild(_hfOverlay);
    return _hfOverlay;
  }
  function closeOverlay() {
    var ov = ensureOverlay();
    ov.style.display = 'none';
    ov.innerHTML = '';
    if (_hfActiveRow) { _hfActiveRow.dataset.open = ''; _hfActiveRow = null; }
  }
  document.addEventListener('mousedown', function(e) {
    var ov = _hfOverlay;
    if (!ov || ov.style.display === 'none') return;
    if (ov.contains(e.target)) return;
    if (e.target.closest && e.target.closest('[data-hf-row]')) return;
    closeOverlay();
  }, true);

  window.__hfOpen = function(row) {
    var tpl = document.getElementById(row.dataset.tpl);
    if (!tpl) return;
    if (_hfActiveRow === row) { closeOverlay(); return; }
    closeOverlay();
    var ov = ensureOverlay();
    _hfActiveRow = row;
    row.dataset.open = '1';
    ov.appendChild(tpl.content.cloneNode(true));
    ov.style.display = 'block';
    var rc = row.getBoundingClientRect();
    var vw = window.innerWidth, vh = window.innerHeight;
    var ow = Math.min(540, vw - 16);
    ov.style.width = ow + 'px';
    ov.style.left = Math.max(8, Math.min(rc.left, vw - ow - 8)) + 'px';
    var oh = ov.offsetHeight || 320;
    ov.style.top = ((rc.bottom + oh + 8 <= vh) ? (rc.bottom + 4) : Math.max(8, rc.top - oh - 4)) + 'px';
  };
  window.__hfClose = closeOverlay;

  // SDK download helper: posts {repo_id, filename, name} to the URL
  // in HFResultsOpts.DownloadURL — for us, /api/models/install. The
  // operator-typed Name groups the install into a Model in the Models
  // tab; we prompt for it via the same dialog Browse Local uses.
  window.__hfDownload = function(repoID, filename) {
    closeOverlay();
    promptModelName({title: 'Install ' + filename, okLabel: 'Install'}, function(name) {
      if (!name) return;
      fetch('/api/models/install', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({source: 'huggingface', repo_id: repoID, filename: filename, runtime_name: _selectedRuntime, name: name})
      })
        .then(function(r) {
          if (!r.ok) return r.text().then(function(t) { throw new Error(window.massErrorText(t) || ('HTTP ' + r.status)); });
          var dlg = document.getElementById('mass-install-model-dialog');
          if (dlg) dlg.hide();
        })
        .catch(function(err) { window.massAlert(err.message, {title: 'Install Failed', variant: 'danger'}); });
    });
  };

  // promptModelName opens a tiny dialog asking the operator to type
  // a model name. Used by HF install (no file picker to host the
  // input) and by the rename action on a model row. opts.title sets
  // the dialog label; opts.initial pre-fills the input; opts.okLabel
  // sets the action button text. Callback receives "" when cancelled.
  function promptModelName(opts, cb) {
    var dlg = document.getElementById('mass-name-prompt-dialog');
    var input = document.getElementById('mass-name-prompt-input');
    var ok = document.getElementById('mass-name-prompt-ok');
    var cancel = document.getElementById('mass-name-prompt-cancel');
    if (!dlg || !input || !ok || !cancel) {
      cb('');
      return;
    }
    opts = opts || {};
    input.value = opts.initial || '';
    var nameOptions = [];
    if (typeof window.__massFetchGroupNames === 'function') {
      window.__massFetchGroupNames().then(function(names) { nameOptions = names; });
    }
    var detachAC = typeof window.__massInlineAutocomplete === 'function'
      ? window.__massInlineAutocomplete(input, function() { return nameOptions; })
      : function() {};
    dlg.label = opts.title || 'Install';
    ok.textContent = opts.okLabel || 'OK';
    function refreshOk() {
      var v = input.value.trim();
      ok.disabled = v === '' || v === (opts.initial || '');
    }
    input.oninput = refreshOk;
    input.onkeydown = function(e) {
      if (e.key === 'Enter' && !ok.disabled) {
        e.preventDefault();
        ok.click();
      }
    };
    refreshOk();
    function done(name) {
      input.oninput = null;
      input.onkeydown = null;
      ok.onclick = null;
      cancel.onclick = null;
      detachAC();
      dlg.hide();
      cb(name);
    }
    ok.onclick = function() { var v = input.value.trim(); if (v) done(v); };
    cancel.onclick = function() { done(''); };
    dlg.show();
    setTimeout(function() { input.focus(); input.select(); }, 50);
  }

  // __massBeginRenameGroup turns the group name span into an inline
  // editable input, focused with the text selected. Enter commits
  // via POST /api/groups/rename; Escape cancels and restores the
  // original span. While editing, the input swallows clicks so the
  // surrounding <summary> doesn't toggle the card's open/close
  // state. The Models SSE stream will replace the card on its next
  // tick when the catalogue changes, so this code only owns the
  // optimistic in-place editor — it doesn't re-render the row.
  window.__massBeginRenameGroup = function(span) {
    if (!span || span.dataset.editing === '1') return;
    var runtime = span.dataset.groupRuntime;
    var id = span.dataset.groupId;
    var current = span.dataset.groupName;
    span.dataset.editing = '1';

    var input = document.createElement('input');
    input.type = 'text';
    input.value = current;
    input.className = 'text-sm font-medium text-white px-1 py-0 rounded';
    input.style.cssText = 'background:var(--mass-bg-base);border:1px solid var(--mass-border);outline:none;min-width:8rem;flex:0 1 auto';
    // Stop the surrounding <summary> from toggling on our clicks.
    var swallow = function(e) { e.stopPropagation(); };
    input.addEventListener('mousedown', swallow);
    input.addEventListener('click', swallow);

    var nameOptions = [];
    if (typeof window.__massFetchGroupNames === 'function') {
      // Filter out the current name so the field doesn't keep
      // re-suggesting itself.
      window.__massFetchGroupNames().then(function(names) {
        nameOptions = names.filter(function(n) { return n !== current; });
      });
    }
    var detachAC = typeof window.__massInlineAutocomplete === 'function'
      ? window.__massInlineAutocomplete(input, function() { return nameOptions; })
      : function() {};

    function restore() {
      span.dataset.editing = '';
      detachAC();
      input.replaceWith(span);
    }
    function commit() {
      var newName = input.value.trim();
      if (!newName || newName === current) {
        restore();
        return;
      }
      // Disable while in flight; if the rename fails, restore so the
      // operator can retry. Success path: SSE re-renders the card.
      input.disabled = true;
      fetch('/api/groups/rename', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({runtime_name: runtime, id: id, new_name: newName})
      }).then(function(r) {
        if (!r.ok) return r.text().then(function(t) { throw new Error(window.massErrorText(t) || ('HTTP ' + r.status)); });
        // Server FireStateChange triggers the Models SSE to push a
        // fresh <details> for the renamed group; just close the editor
        // and let the patch land. Avoids stale dataset.groupId on a
        // follow-up rename.
        restore();
      }).catch(function(err) {
        window.massAlert(err.message, {title: 'Rename Failed', variant: 'danger'});
        input.disabled = false;
        input.focus();
        input.select();
      });
    }
    input.addEventListener('keydown', function(e) {
      if (e.key === 'Enter') {
        e.preventDefault();
        commit();
      } else if (e.key === 'Escape') {
        e.preventDefault();
        restore();
      }
    });
    input.addEventListener('blur', restore);

    span.replaceWith(input);
    input.focus();
    input.select();
  };

  // --- Local import ------------------------------------------------------
  // Multi-select: the operator picks any number of files (different
  // quants of one model, chat + projector, etc.) and types one Name.
  // The gateway stamps that Name on every file; same-name imports
  // merge into one Model in the Models tab.
  function postImport(path, runtimeName, name) {
    return fetch('/api/models/import', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({path: path, runtime_name: runtimeName, name: name})
    }).then(function(r) {
      if (!r.ok) return r.text().then(function(t) { throw new Error(window.massErrorText(t) || ('HTTP ' + r.status)); });
      return r.json();
    });
  }
  // Browse Local picks one runtime to scope the file picker AND
  // import target. Multi-runtime selector is deferred — today there's
  // only one runtime kind, so auto-pick the first sorted running one.
  window.__massImportLocal = function() {
    if (typeof window.__massBrowse !== 'function') {
      window.massAlert('File browser not available.', {variant: 'danger'});
      return;
    }
    fetch('/api/runtimes').then(function(r) { return r.json(); }).then(function(rs) {
      var running = (Array.isArray(rs) ? rs : []).filter(function(rt) { return rt && rt.running; });
      if (running.length === 0) {
        window.massAlert('no runtime gateway is running - start one in the Runtimes tab',
          {title: 'Cannot Browse', variant: 'danger'});
        return;
      }
      running.sort(function(a, b) { return a.runtime_name < b.runtime_name ? -1 : 1; });
      var rt = running[0];
      // No extension filter — MASS doesn't know which extensions a runtime
      // claims, the runtime does. The file picker shows all files; the
      // server's PlanLocalImport rejects with a clear error if the
      // selection isn't something the runtime can handle.
      window.__massBrowse({
        ext: '',
        multiple: true,
        nameRequired: true,
        onConfirm: function(paths, name) {
          if (!paths || paths.length === 0) return;
          var errors = [];
          var seq = Promise.resolve();
          paths.forEach(function(p) {
            seq = seq.then(function() {
              return postImport(p, rt.runtime_name, name).catch(function(err) {
                errors.push(p + ': ' + err.message);
              });
            });
          });
          seq.then(function() {
            if (errors.length > 0) {
              window.massAlert('Import failed for:\n' + errors.join('\n'),
                {title: 'Import Failed', variant: 'danger'});
            }
          });
        }
      });
    });
  };

  // --- Downloads SSE + row rendering -------------------------------------
  // Download rows live in #models-downloads, a sibling to #models-list.
  // Keeping them out of the SSE-driven container means an SSE reconnect
  // (which resets #models-list) can't wipe in-flight progress rows.

  function safeID(s) { return s.replace(/[^a-zA-Z0-9_-]/g, '_'); }

  function fmtSize(n) {
    if (!n || n <= 0) return '—';
    if (n < 1024) return n + ' B';
    if (n < 1048576) return (n / 1024).toFixed(1) + ' KB';
    if (n < 1073741824) return (n / 1048576).toFixed(1) + ' MB';
    return (n / 1073741824).toFixed(2) + ' GB';
  }

  function htmlEscape(s) { return String(s).replace(/[&<>"']/g, function(c) {
    return ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c];
  }); }

  // Build a fresh download row matching modelVariantRow's visual rhythm.
  // The runtime gateway groups files by operator-typed name; the row
  // shows the filename and progress, the surrounding group header
  // shows the model name (evt.GroupName).
  function buildDlRow(evt) {
    var rel = evt.RelPath;
    var name = evt.Filename || rel.split('/').pop();
    var row = document.createElement('div');
    row.className = 'flex items-center gap-2 px-3 py-2 rounded bg-neutral-800/60 border border-neutral-700/50';
    row.dataset.dlRel = rel;
    row.dataset.status = 'active';
    row.innerHTML =
      '<span class="text-xs text-neutral-200 truncate flex-1" title="' + htmlEscape(name) + '">' + htmlEscape(name) + '</span>' +
      '<span class="mass-dl-bytes text-xs flex-shrink-0" style="color:var(--mass-text-muted);min-width:7rem;text-align:right">0 / ' + fmtSize(evt.Total || 0) + '</span>' +
      '<div style="position:relative;min-width:6rem;height:1.25rem;border-radius:0.25rem;overflow:hidden;background:rgba(0,0,0,0.3);flex-shrink:0">' +
        '<div class="mass-dl-bar" style="position:absolute;top:0;left:0;height:100%;width:0%;background:var(--mass-blue);transition:width .3s"></div>' +
        '<span class="mass-dl-pct" style="position:relative;z-index:1;display:flex;align-items:center;justify-content:center;height:100%;font-size:0.7rem;font-weight:600;color:#fff">0%</span>' +
      '</div>' +
      '<sl-icon-button class="mass-dl-pause" name="pause-fill" style="font-size:0.85rem;color:var(--mass-blue);margin-left:0.25rem" onclick="window.__massDlToggle(\'' + rel.replace(/'/g, "\\'") + '\')"></sl-icon-button>' +
      '<sl-icon-button name="x-lg" style="font-size:0.75rem;color:var(--sl-color-danger-400)" onclick="window.__massDlCancel(\'' + rel.replace(/'/g, "\\'") + '\')"></sl-icon-button>';
    return row;
  }

  function ensureRow(evt) {
    var rel = evt.RelPath;
    var existing = document.querySelector('#models-downloads [data-dl-rel="' + (rel.replace(/"/g,'\\"')) + '"]');
    if (existing) return existing;
    var host = document.getElementById('models-downloads');
    if (!host) return null;
    var row = buildDlRow(evt);
    host.appendChild(row);
    return row;
  }

  function updateRow(row, evt) {
    if (!row) return;
    var bytes = row.querySelector('.mass-dl-bytes');
    var bar = row.querySelector('.mass-dl-bar');
    var pct = row.querySelector('.mass-dl-pct');
    var pause = row.querySelector('.mass-dl-pause');
    if (bytes) bytes.textContent = fmtSize(evt.Downloaded || 0) + ' / ' + fmtSize(evt.Total || 0);
    if (evt.Total > 0) {
      var p = Math.min(100, Math.floor(100 * (evt.Downloaded || 0) / evt.Total));
      if (bar) bar.style.width = p + '%';
      if (pct) pct.textContent = p + '%';
    }
    if (evt.Status === 'paused') {
      row.dataset.status = 'paused';
      if (pause) { pause.name = 'play-fill'; pause.style.color = 'var(--sl-color-warning-400)'; }
      if (bar) bar.style.background = 'var(--sl-color-warning-500)';
    } else if (evt.Status === 'resumed' || evt.Status === 'started' || evt.Status === 'progress') {
      row.dataset.status = 'active';
      if (pause) { pause.name = 'pause-fill'; pause.style.color = 'var(--mass-blue)'; }
      if (bar) bar.style.background = 'var(--mass-blue)';
    } else if (evt.Status === 'error') {
      // Replace the row's content entirely: a danger-tinted message block.
      // When the download had any bytes on disk (Downloaded > 0) we keep
      // the row with a Resume button so the operator can pick up where
      // they left off after fixing whatever broke (commonly free space).
      // For zero-byte errors there's nothing to resume, so we drop the
      // row after a short delay — long enough to read, short enough not
      // to clutter the list.
      row.dataset.status = 'error';
      var rel = row.dataset.dlRel || '';
      var safeRel = rel.replace(/'/g, "\\'");
      var msg = evt.ErrorMsg || 'Download failed';
      var resumable = (evt.Downloaded || 0) > 0;
      var resumeBtn = resumable
        ? '<sl-icon-button name="arrow-clockwise" style="font-size:0.85rem;color:var(--mass-blue);flex-shrink:0" onclick="window.__massDlToggle(\'' + safeRel + '\')"></sl-icon-button>'
        : '';
      row.innerHTML =
        '<sl-icon name="exclamation-triangle" style="font-size:1rem;color:var(--sl-color-danger-400);flex-shrink:0"></sl-icon>' +
        '<span class="text-xs flex-1" style="color:var(--sl-color-danger-400);min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + htmlEscape(msg) + '">' + htmlEscape(msg) + '</span>' +
        resumeBtn +
        '<sl-icon-button name="x-lg" style="font-size:0.75rem;color:var(--sl-color-danger-400);flex-shrink:0" onclick="window.__massDlCancel(\'' + safeRel + '\')"></sl-icon-button>';
      if (!resumable) {
        // Auto-dismiss after 8s. Stash the timer on the row so a later
        // Resume click could cancel it (not a current path, but cheap to
        // make it future-proof).
        if (row._massErrorTimer) clearTimeout(row._massErrorTimer);
        row._massErrorTimer = setTimeout(function() {
          // Send Cancel so the server forgets about it too — otherwise
          // the row would resurface on the next SSE recover.
          fetch('/api/models/download/cancel', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({rel_path: rel})
          }).catch(function() {});
          if (row.parentNode) row.remove();
        }, 8000);
      }
    }
  }

  function removeRow(relPath) {
    var row = document.querySelector('#models-downloads [data-dl-rel="' + (relPath.replace(/"/g,'\\"')) + '"]');
    if (row) row.remove();
  }

  function handleEvent(evt) {
    if (!evt) return;
    if (evt.Status === 'done' || evt.Status === 'cancelled') {
      removeRow(evt.RelPath);
      return;
    }
    var row = ensureRow(evt);
    updateRow(row, evt);
  }

  window.__massDlToggle = function(relPath) {
    var sel = '#models-downloads [data-dl-rel="' + relPath.replace(/"/g, '\\"') + '"]';
    var row = document.querySelector(sel);
    var status = row ? row.dataset.status : 'active';
    var path = (status === 'paused' || status === 'error') ? '/api/models/download/resume' : '/api/models/download/pause';
    fetch(path, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({rel_path: relPath})
    }).catch(function() {});
  };

  // Cancel uses an sl-dialog (in shell.templ) so the prompt inherits the
  // MASS theme instead of the OS-styled window.confirm popup. The dialog
  // is shared across rows; we stash the target rel-path on the dialog
  // element itself and read it back on confirm.
  window.__massDlCancel = function(relPath) {
    var dlg = document.getElementById('mass-confirm-cancel-dl-dialog');
    var nameEl = document.getElementById('mass-cancel-dl-name');
    if (!dlg || !nameEl) return;
    dlg.dataset.relPath = relPath;
    nameEl.textContent = relPath;
    dlg.show();
  };

  window.__massConfirmCancelDownload = function() {
    var dlg = document.getElementById('mass-confirm-cancel-dl-dialog');
    if (!dlg) return;
    var relPath = dlg.dataset.relPath || '';
    dlg.hide();
    if (!relPath) return;
    fetch('/api/models/download/cancel', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({rel_path: relPath})
    }).catch(function() {});
  };

  var _dlSSE = null;
  function openDownloadsStream() {
    if (_dlSSE) return;
    _dlSSE = new EventSource('/api/models/downloads/events');
    var statuses = ['started', 'progress', 'paused', 'resumed', 'done', 'cancelled', 'error'];
    statuses.forEach(function(s) {
      _dlSSE.addEventListener(s, function(e) {
        var payload;
        try { payload = JSON.parse(e.data); } catch (err) { return; }
        handleEvent(payload);
      });
    });
    _dlSSE.onerror = function() { /* browser auto-reconnects */ };
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', openDownloadsStream);
  } else {
    openDownloadsStream();
  }
})();
