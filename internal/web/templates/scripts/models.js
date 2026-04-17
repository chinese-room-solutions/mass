(function() {
  // Sync properties panel spacer with first list card position.
  var mScroll = document.getElementById('models-list-scroll');
  var mSpacer = document.getElementById('models-props-spacer');
  if (mScroll && mSpacer) {
    var syncModelTop = function() {
      var tabRect = mSpacer.closest('#models-tab').getBoundingClientRect();
      var scrollTop = mScroll.getBoundingClientRect().top - tabRect.top;
      var scrollPad = parseFloat(getComputedStyle(mScroll).paddingTop) || 0;
      mSpacer.style.height = (scrollTop + scrollPad) + 'px';
    };
    syncModelTop();
    new ResizeObserver(syncModelTop).observe(mScroll.parentElement);
  }

  // Download speed tracking: {filename: {bytes, time}}.
  var dlSpeedState = {};

  // Format bytes to human-readable string.
  function dlFormatBytes(b) {
    if (b < 1024) return b + ' B';
    if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
    if (b < 1073741824) return (b / 1048576).toFixed(1) + ' MB';
    return (b / 1073741824).toFixed(2) + ' GB';
  }

  // Format speed (bytes/sec) to human-readable string.
  function dlFormatSpeed(bps) {
    if (bps < 1024) return bps.toFixed(0) + ' B/s';
    if (bps < 1048576) return (bps / 1024).toFixed(1) + ' KB/s';
    if (bps < 1073741824) return (bps / 1048576).toFixed(1) + ' MB/s';
    return (bps / 1073741824).toFixed(2) + ' GB/s';
  }

  // Loading spinner for HF search.
  window.__massHfLoading = function() {
    var r = document.getElementById('models-hf-results');
    if (r) r.innerHTML = '<div class="text-center py-4"><sl-spinner style="font-size:1.5rem;--track-width:3px"></sl-spinner></div>';
  };

  // Click on empty space to deselect model.
  // Targets: the flex container background, the scrollable list area, or models-content.
  function isEmptySpace(e) {
    var t = e.target;
    var tab = document.getElementById('models-tab');
    var scroll = document.getElementById('models-list-scroll');
    var content = document.getElementById('models-content');
    return t === tab || t === scroll || t === content;
  }
  var tab = document.getElementById('models-tab');
  if (tab) {
    tab.addEventListener('click', function(e) {
      if (isEmptySpace(e)) {
        var btn = document.getElementById('models-deselect-trigger');
        if (btn) btn.click();
      }
    });
  }

  // Debounced model filter. Shoelace sl-input fires 'sl-input' on keystroke.
  // data-bind syncs the value to $modelsFilter; after 300ms we click a hidden
  // Datastar button whose data-on:click reads the signal.
  var _filterTimer;
  var filterInput = document.getElementById('models-filter-input');
  if (filterInput) {
    filterInput.addEventListener('sl-input', function() {
      clearTimeout(_filterTimer);
      _filterTimer = setTimeout(function() {
        var btn = document.getElementById('models-filter-trigger');
        if (btn) btn.click();
      }, 300);
    });
  }

  // --- Download row management ---
  // Safe ID from filename for DOM element IDs.
  function dlSafeId(f) { return f.replace(/[^a-zA-Z0-9\-]/g, '_'); }

  // Extract quantization tag from filename (mirrors Go's ExtractQuant).
  function dlExtractQuant(filename) {
    var base = filename.replace(/\.gguf$/i, '').toLowerCase();
    var parts = base.split(/[.\-]/);
    for (var i = parts.length - 1; i >= 0; i--) {
      var p = parts[i].toUpperCase();
      if (p.length >= 2 && (
        (p[0]==='Q' && p[1]>='0' && p[1]<='9') ||
        (p[0]==='F' && p[1]>='0' && p[1]<='9') ||
        (p.length>=3 && p[0]==='B' && p[1]==='F' && p[2]>='0' && p[2]<='9') ||
        (p.length>=3 && p[0]==='I' && p[1]==='Q' && p[2]>='0' && p[2]<='9') ||
        p.indexOf('MXFP')===0
      )) return p;
    }
    return '';
  }

  // Build downloading row HTML.
  function dlRowHTML(filename, safeId) {
    var quant = dlExtractQuant(filename);
    var quantHTML = quant
      ? '<span class="mass-badge-alt font-mono text-xs font-bold rounded px-1.5 py-0.5" style="min-width:4.5rem;text-align:center">' + quant + '</span>'
      : '<span style="min-width:4.5rem;flex-shrink:0"></span>';
    return '<div id="mass-dl-row-' + safeId + '" class="flex items-center gap-2 px-3 py-2 rounded">' +
      quantHTML +
      '<span class="text-xs text-neutral-300 truncate flex-1" title="' + filename.replace(/"/g,'&quot;') + '">' + filename.replace(/</g,'&lt;') + '</span>' +
      '<span id="mass-dl-speed-' + safeId + '" class="text-xs text-neutral-500 flex-shrink-0" style="min-width:5.5rem;text-align:right"></span>' +
      '<div style="position:relative;min-width:6rem;height:1.5rem;border-radius:0.25rem;overflow:hidden;background:#334155;flex-shrink:0">' +
        '<div id="mass-dl-bar-' + safeId + '" style="position:absolute;top:0;left:0;height:100%;width:0%;background:var(--mass-blue);transition:width .3s"></div>' +
        '<span id="mass-dl-pct-' + safeId + '" style="position:relative;z-index:1;display:flex;align-items:center;justify-content:center;height:100%;font-size:0.7rem;font-weight:600;color:#fff">0%</span>' +
      '</div>' +
      '<sl-icon-button id="mass-dl-pause-' + safeId + '" name="pause-fill" style="font-size:0.85rem;color:var(--mass-blue)" onclick="window.__massModelDlTogglePause(\'' + filename.replace(/'/g,"\\'") + '\')"></sl-icon-button>' +
      '<sl-icon-button name="x-lg" style="font-size:0.75rem;color:var(--sl-color-danger-400)" onclick="window.__massModelDlCancelClick(\'' + filename.replace(/'/g,"\\'") + '\')"></sl-icon-button>' +
    '</div>';
  }

  // Update the download indicator on a group's summary line.
  function dlUpdateGroupIndicator(group) {
    if (!group) return;
    var dlCount = group.querySelectorAll('[id^="mass-dl-row-"]').length;
    var indicator = group.querySelector('.mass-dl-indicator');
    if (dlCount > 0) {
      if (!indicator) {
        // Insert a small spinner after the variant count.
        var countEl = group.querySelector('summary .text-xs.text-neutral-400');
        if (countEl) countEl.insertAdjacentHTML('afterend',
          '<sl-spinner class="mass-dl-indicator" style="font-size:0.65rem;--track-width:2px;margin-left:0.25rem;color:var(--mass-blue)"></sl-spinner>');
      }
    } else {
      if (indicator) indicator.remove();
    }
  }

  // __massModelDlStart: Insert a downloading row into the model list.
  // groupName is the server-computed display name (same as Go's FormatModelName output).
  window.__massModelDlStart = function(filename, groupName) {
    var safeId = dlSafeId(filename);
    // Don't insert if already exists.
    if (document.getElementById('mass-dl-row-' + safeId)) return;

    var displayName = groupName;
    var content = document.getElementById('models-content');
    if (!content) return;

    // Look for existing group with matching name.
    var groups = content.querySelectorAll('details.model-group');
    var targetGroup = null;
    for (var i = 0; i < groups.length; i++) {
      var summary = groups[i].querySelector('summary .text-sm');
      if (summary && summary.textContent.trim() === displayName) {
        targetGroup = groups[i];
        break;
      }
    }

    if (targetGroup) {
      // Insert into existing group's variant list.
      var variantContainer = targetGroup.querySelector('.space-y-px');
      if (variantContainer) {
        variantContainer.insertAdjacentHTML('afterbegin', dlRowHTML(filename, safeId));
      }
      targetGroup.open = true;
      // Update variant count.
      var countEl = targetGroup.querySelector('summary .text-xs.text-neutral-400');
      if (countEl) {
        var cnt = targetGroup.querySelectorAll('[data-model-path], [id^="mass-dl-row-"]').length;
        countEl.textContent = cnt + ' variant' + (cnt === 1 ? '' : 's');
      }
      dlUpdateGroupIndicator(targetGroup);
    } else {
      // Create a new group.
      var spaceDiv = content.querySelector('.space-y-1');
      if (!spaceDiv) {
        // Empty state — replace placeholder.
        content.innerHTML = '<div class="space-y-1"></div>';
        spaceDiv = content.querySelector('.space-y-1');
      }
      var groupHTML = '<details class="model-group bg-neutral-800/60 rounded-lg border border-neutral-700/50 overflow-hidden" open>' +
        '<summary class="flex items-center gap-3 w-full px-4 py-3 cursor-pointer select-none hover:bg-neutral-700/40 list-none" style="-webkit-appearance:none">' +
          '<sl-icon name="chevron-right" class="text-neutral-400" style="font-size:0.75rem;transition:transform 0.2s"></sl-icon>' +
          '<span class="text-sm font-medium text-white">' + displayName.replace(/</g,'&lt;') + '</span>' +
          '<span class="text-xs text-neutral-400 ml-auto">1 variant</span>' +
          '<sl-spinner class="mass-dl-indicator" style="font-size:0.65rem;--track-width:2px;margin-left:0.25rem;color:var(--mass-blue)"></sl-spinner>' +
        '</summary>' +
        '<div class="space-y-px border-t border-neutral-700/50 px-2 py-1 overflow-y-auto" style="max-height:50vh">' +
          dlRowHTML(filename, safeId) +
        '</div>' +
      '</details>';
      spaceDiv.insertAdjacentHTML('afterbegin', groupHTML);
    }
  };

  // __massModelDlProgress: Update progress bar and speed.
  // downloaded/total are in bytes (optional, for speed calculation).
  window.__massModelDlProgress = function(filename, pct, downloaded, total) {
    var safeId = dlSafeId(filename);
    var bar = document.getElementById('mass-dl-bar-' + safeId);
    if (bar) bar.style.width = pct + '%';
    var txt = document.getElementById('mass-dl-pct-' + safeId);
    if (txt) txt.textContent = pct + '%';

    // Compute and display speed.
    var speedEl = document.getElementById('mass-dl-speed-' + safeId);
    if (speedEl && typeof downloaded === 'number' && downloaded > 0) {
      var now = Date.now();
      var prev = dlSpeedState[filename];
      if (prev && now > prev.time) {
        var dt = (now - prev.time) / 1000; // seconds
        var db = downloaded - prev.bytes;
        if (dt > 0 && db > 0) {
          var bps = db / dt;
          speedEl.textContent = dlFormatBytes(downloaded) + ' · ' + dlFormatSpeed(bps);
        }
      } else if (!prev) {
        // First update — show size only.
        speedEl.textContent = dlFormatBytes(downloaded);
      }
      dlSpeedState[filename] = {bytes: downloaded, time: now};
    }
  };

  // __massModelDlPaused: Swap pause icon to play.
  window.__massModelDlPaused = function(filename) {
    var safeId = dlSafeId(filename);
    var btn = document.getElementById('mass-dl-pause-' + safeId);
    if (btn) btn.name = 'play-fill';
    // Stop the spinner.
    var row = document.getElementById('mass-dl-row-' + safeId);
    if (row) {
      var spinner = row.querySelector('sl-spinner');
      if (spinner) spinner.style.visibility = 'hidden';
    }
    // Show downloaded size (drop speed).
    var speedEl = document.getElementById('mass-dl-speed-' + safeId);
    if (speedEl) {
      var prev = dlSpeedState[filename];
      if (prev) speedEl.textContent = dlFormatBytes(prev.bytes);
    }
    delete dlSpeedState[filename];
  };

  // __massModelDlTogglePause: POST to pause or resume.
  window.__massModelDlTogglePause = function(filename) {
    var safeId = dlSafeId(filename);
    var btn = document.getElementById('mass-dl-pause-' + safeId);
    if (!btn) return;
    var isPaused = btn.name === 'play-fill';
    var endpoint = isPaused ? '/internal/models/download/resume' : '/internal/models/download/pause';
    // Optimistic icon swap.
    btn.name = isPaused ? 'pause-fill' : 'play-fill';
    // Reset speed tracking so resumed download doesn't show bogus speed.
    delete dlSpeedState[filename];
    var row = document.getElementById('mass-dl-row-' + safeId);
    if (row) {
      var spinner = row.querySelector('sl-spinner');
      if (spinner) spinner.style.visibility = isPaused ? 'visible' : 'hidden';
    }
    fetch(endpoint, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({filename: filename})
    });
  };

  // __massModelDlCancelClick: POST cancel and optimistically remove row.
  window.__massModelDlCancelClick = function(filename) {
    fetch('/internal/models/download/cancel', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({filename: filename})
    });
    window.__massModelDlCancel(filename);
  };

  // __massModelDlCancel: Remove downloading row from DOM.
  window.__massModelDlCancel = function(filename) {
    delete dlSpeedState[filename];
    var safeId = dlSafeId(filename);
    var row = document.getElementById('mass-dl-row-' + safeId);
    if (!row) return;
    var group = row.closest('details.model-group');
    row.remove();
    if (group) {
      var remaining = group.querySelectorAll('[data-model-path], [id^="mass-dl-row-"]').length;
      if (remaining === 0) {
        group.remove();
      } else {
        var countEl = group.querySelector('summary .text-xs.text-neutral-400');
        if (countEl) countEl.textContent = remaining + ' variant' + (remaining === 1 ? '' : 's');
        dlUpdateGroupIndicator(group);
      }
    }
  };

  // __massModelDlDone: Remove downloading row and refresh list.
  window.__massModelDlDone = function(filename) {
    delete dlSpeedState[filename];
    var safeId = dlSafeId(filename);
    var row = document.getElementById('mass-dl-row-' + safeId);
    if (row) {
      var group = row.closest('details.model-group');
      row.remove();
      dlUpdateGroupIndicator(group);
    }
    // Full refresh to render the real model row.
    var btn = document.getElementById('mass-models-refresh-btn');
    if (btn) btn.click();
  };

  // __massLoadModel: Load a model into the pool.
  window.__massLoadModel = function(path, modelType) {
    var btn = document.getElementById('models-load-btn');
    if (btn) { btn.loading = true; btn.disabled = true; }
    fetch('/api/v1/models/load', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({path: path, type: modelType})
    }).then(function(resp) {
      if (!resp.ok) return resp.json().then(function(d) { throw new Error(d.error || 'Load failed'); });
      return resp.json();
    }).then(function() {
      if (btn) { btn.loading = false; btn.disabled = false; btn.variant = 'success'; }
      setTimeout(function() { if (btn) btn.variant = 'primary'; }, 2000);
    }).catch(function(err) {
      if (btn) { btn.loading = false; btn.disabled = false; }
      alert('Failed to load model: ' + err.message);
    });
  };

  // __massModelDlErr: Show error in progress area.
  window.__massModelDlErr = function(filename, errMsg) {
    var safeId = dlSafeId(filename);
    var row = document.getElementById('mass-dl-row-' + safeId);
    if (!row) return;
    // Replace spinner with error icon.
    var spinner = row.querySelector('sl-spinner');
    if (spinner) spinner.outerHTML = '<sl-icon name="exclamation-triangle-fill" style="color:var(--mass-red);font-size:0.85rem;flex-shrink:0"></sl-icon>';
    // Replace progress bar with error message.
    var bar = row.querySelector('[id^="mass-dl-bar-"]');
    if (bar && bar.parentElement) {
      bar.parentElement.outerHTML = '<span class="text-xs text-red-400 truncate" title="' + errMsg.replace(/"/g,'&quot;') + '">Error</span>';
    }
    // Remove pause button.
    var pauseBtn = document.getElementById('mass-dl-pause-' + safeId);
    if (pauseBtn) pauseBtn.remove();
  };

  // __massImportLocalModel: Open file browser to pick a .gguf file and import it.
  window.__massImportLocalModel = function() {
    var _selectedPath = '';
    var dlg = document.getElementById('mass-file-browser');
    if (!dlg) return;
    dlg.label = 'Import Local Model (.gguf)';
    dlg.show();
    var selectBtn = document.getElementById('mass-fb-select');
    selectBtn.disabled = true;
    selectBtn.onclick = function() {
      if (!_selectedPath) return;
      dlg.hide();
      fetch('/api/v1/models/import', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({path: _selectedPath})
      }).then(function(resp) {
        if (!resp.ok) return resp.json().then(function(d) { throw new Error(d.error || 'Import failed'); });
      }).catch(function(err) {
        alert('Failed to import model: ' + err.message);
      });
    };
    window.__massFileBrowser({
      pathElId: 'mass-fb-path',
      entriesElId: 'mass-fb-entries',
      ext: '.gguf',
      onSelect: function(path) { _selectedPath = path; selectBtn.disabled = false; },
      onNavigate: function() { _selectedPath = ''; selectBtn.disabled = true; }
    });
  };
})();
