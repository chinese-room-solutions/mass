// Install dialog + downloads SSE for the Models tab.
//
// Three responsibilities:
//   1. Open the per-runtime install UI (gateway-hosted) in an iframe.
//   2. Open a file-browser dialog for "Browse Local" and POST /api/models/import.
//   3. Subscribe to /api/models/downloads/events and render per-file
//      progress rows under #models-downloads.
//
// Registry installs (HF, etc.) are gateway-owned: each runtime gateway
// serves its own /install page under /mass.<runtime_name>.* and we
// mount it in the Install dialog's iframe. The gateway calls back into
// MASS via the MassScheduler.DownloadFiles RPC for the actual fetch;
// progress surfaces in the in-flight panel below the Models list.

(function() {
  // --- Install dialog (iframe-mounted gateway UI) ------------------------
  // The dialog hosts a runtime selector tab strip plus an iframe
  // pointed at the picked runtime's /install page. The gateway's UI
  // does the registry search, file picker, and form, then calls back
  // to MASS via MassScheduler.DownloadFiles. MASS doesn't know what
  // registry the gateway is talking to — that's the whole point.
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
        loadInstallIframe();
      };
      host.appendChild(tab);
    });
  }

  // Each iframe load gets a session id; resize messages from prior
  // sessions are ignored. Without this the previous gateway page's
  // last few postMessages can land after we've already navigated to
  // a fresh src and leave the iframe stuck at the old (tall) size.
  var _installSession = 0;

  function loadInstallIframe() {
    var frame = document.getElementById('mass-install-frame');
    if (!frame || !_selectedRuntime) return;
    var theme = document.documentElement.dataset.theme || 'dark';
    _installSession++;
    // Collapse to zero before the new page starts loading so the
    // previous session's tall height doesn't carry over visually
    // until the new page reports its own.
    frame.style.height = '0';
    // The gateway's /install page is mounted at /mass.<runtime>.install.
    // MASS's runtime proxy strips /mass.<runtime>, so the gateway's
    // route is /.install. Sub-paths (search, submit) are POSTed by the
    // page itself, prefixed with its own location.pathname.
    // Cache-bust on every open: gateway code can change between
    // builds and we don't want a stale cached page (iframes cache
    // aggressively). The same `s` value goes into postMessage events
    // so we can drop messages from prior sessions.
    frame.src = '/mass.' + encodeURIComponent(_selectedRuntime) + '.install?theme=' + theme + '&s=' + _installSession + '&t=' + Date.now();
  }

  // Gateway → MASS messages:
  //   mass-install-resize: the gateway page says how tall it wants
  //     to be; MASS clamps to a viewport ceiling and applies. The
  //     gateway owns the *what* (layout, row caps, internal scroll);
  //     MASS owns the *where* (fits the dialog within the viewport).
  //   mass-install-queued: install accepted, close the dialog.
  window.addEventListener('message', function(e) {
    var data = e.data;
    if (!data || typeof data !== 'object') return;
    if (data.type === 'mass-install-resize' && typeof data.height === 'number') {
      // Drop messages from prior sessions — see loadInstallIframe.
      if (data.session != null && String(data.session) !== String(_installSession)) return;
      var frame = document.getElementById('mass-install-frame');
      if (!frame) return;
      var max = Math.max(200, window.innerHeight - 220);
      // Floor the frame once the page has content. A gateway page's
      // overlays (name prompt, variant list) are position:fixed and sized
      // against the iframe's own viewport, so a one-row result set leaves
      // them nowhere to draw and they render clipped. The floor gives them
      // room without MASS knowing what they are; below it, the collapsed
      // empty/loading states are left alone.
      var h = Math.max(0, data.height);
      if (h > 120) h = Math.max(h, 340);
      frame.style.height = Math.min(h, max) + 'px';
    } else if (data.type === 'mass-install-queued') {
      var dlg = document.getElementById('mass-install-model-dialog');
      if (dlg) dlg.hide();
    }
  });

  // Blank the iframe on close so the previous gateway page is torn
  // down and can't keep running fetches/SSE in the background.
  document.addEventListener('DOMContentLoaded', function() {
    var dlg = document.getElementById('mass-install-model-dialog');
    if (!dlg) return;
    dlg.addEventListener('sl-after-hide', function(e) {
      if (e.target !== dlg) return;
      var frame = document.getElementById('mass-install-frame');
      if (frame) frame.src = 'about:blank';
    });
  });

  window.__massOpenInstall = function() {
    fetch('/api/runtimes').then(function(r) { return r.json(); }).then(function(rs) {
      var running = (Array.isArray(rs) ? rs : []).filter(function(rt) { return rt && rt.running; });
      if (running.length === 0) {
        window.massAlert('no runtime gateway is running - start one in the Runtimes tab',
          {title: 'Cannot Install', variant: 'danger'});
        return;
      }
      running.sort(function(a, b) { return a.runtime_name < b.runtime_name ? -1 : 1; });
      var stillRunning = running.some(function(rt) { return rt.runtime_name === _selectedRuntime; });
      if (!stillRunning) _selectedRuntime = running[0].runtime_name;
      renderRuntimeTabs(running);
      loadInstallIframe();

      var dlg = document.getElementById('mass-install-model-dialog');
      if (dlg) dlg.show();
    });
  };

  // --- Group rename ------------------------------------------------------
  // __massBeginRenameGroup turns the group name span into an inline
  // editable input, focused with the text selected. Enter commits via
  // POST /api/groups/rename; Escape cancels and restores the original
  // span. The Models SSE stream replaces the card on its next tick
  // when the catalogue changes, so this code only owns the optimistic
  // in-place editor — it doesn't re-render the row.
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
    var swallow = function(e) { e.stopPropagation(); };
    input.addEventListener('mousedown', swallow);
    input.addEventListener('click', swallow);

    var nameOptions = [];
    if (typeof window.__massFetchGroupNames === 'function') {
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
      input.disabled = true;
      fetch('/api/groups/rename', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({runtime_name: runtime, id: id, new_name: newName})
      }).then(function(r) {
        if (!r.ok) return r.text().then(function(t) { throw new Error(window.massErrorText(t) || ('HTTP ' + r.status)); });
        // Server FireStateChange triggers the Models SSE to push a fresh
        // <details> for the renamed group; just close the editor and
        // let the patch land. Avoids stale dataset.groupId on a follow-up
        // rename.
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
  // merge into one Group in the Models tab.
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
        '<div class="mass-dl-bar" style="position:absolute;top:0;left:0;height:100%;width:0%;background:var(--mass-accent);transition:width .3s"></div>' +
        '<span class="mass-dl-pct" style="position:relative;z-index:1;display:flex;align-items:center;justify-content:center;height:100%;font-size:0.7rem;font-weight:600;color:#fff">0%</span>' +
      '</div>' +
      '<sl-icon-button class="mass-dl-pause" name="pause-fill" style="font-size:0.85rem;color:var(--mass-accent);margin-left:0.25rem" onclick="window.__massDlToggle(\'' + rel.replace(/'/g, "\\'") + '\')"></sl-icon-button>' +
      '<sl-icon-button name="x-lg" style="font-size:0.75rem;color:var(--mass-danger)" onclick="window.__massDlCancel(\'' + rel.replace(/'/g, "\\'") + '\')"></sl-icon-button>';
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
      if (pause) { pause.name = 'play-fill'; pause.style.color = 'var(--mass-warning)'; }
      if (bar) bar.style.background = 'var(--mass-warning)';
    } else if (evt.Status === 'resumed' || evt.Status === 'started' || evt.Status === 'progress') {
      row.dataset.status = 'active';
      if (pause) { pause.name = 'pause-fill'; pause.style.color = 'var(--mass-accent)'; }
      if (bar) bar.style.background = 'var(--mass-accent)';
    } else if (evt.Status === 'error') {
      row.dataset.status = 'error';
      var rel = row.dataset.dlRel || '';
      var safeRel = rel.replace(/'/g, "\\'");
      var msg = evt.ErrorMsg || 'Download failed';
      var resumable = (evt.Downloaded || 0) > 0;
      var resumeBtn = resumable
        ? '<sl-icon-button name="arrow-clockwise" style="font-size:0.85rem;color:var(--mass-accent);flex-shrink:0" onclick="window.__massDlToggle(\'' + safeRel + '\')"></sl-icon-button>'
        : '';
      row.innerHTML =
        '<sl-icon name="exclamation-triangle" style="font-size:1rem;color:var(--mass-danger);flex-shrink:0"></sl-icon>' +
        '<span class="text-xs flex-1" style="color:var(--mass-danger);min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + htmlEscape(msg) + '">' + htmlEscape(msg) + '</span>' +
        resumeBtn +
        '<sl-icon-button name="x-lg" style="font-size:0.75rem;color:var(--mass-danger);flex-shrink:0" onclick="window.__massDlCancel(\'' + safeRel + '\')"></sl-icon-button>';
      if (!resumable) {
        if (row._massErrorTimer) clearTimeout(row._massErrorTimer);
        row._massErrorTimer = setTimeout(function() {
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
