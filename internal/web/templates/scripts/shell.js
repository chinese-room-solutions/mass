(function() {
  // Theme: watch for Datastar signal patches and update <html>/<body> classes.
  function applyTheme(t) {
    var isLight = t === 'light';
    document.documentElement.className = isLight ? 'sl-theme-light' : 'sl-theme-dark dark';
    document.body.className = (isLight ? 'bg-neutral-100 text-neutral-900' : 'bg-neutral-950 text-neutral-100') + ' min-h-screen';
    // Apply theme class to all sl-dialog elements so their portalled panels inherit correctly.
    var add = isLight ? 'sl-theme-light' : 'sl-theme-dark';
    var rem = isLight ? 'sl-theme-dark' : 'sl-theme-light';
    document.querySelectorAll('sl-dialog').forEach(function(d) {
      d.classList.add(add);
      d.classList.remove(rem);
    });
  }
  document.addEventListener('datastar-signal-patch', function(e) {
    if (e.detail && e.detail.theme !== undefined) {
      applyTheme(e.detail.theme);
    }
  });
  // Apply theme to dialogs on initial load (read from <html> class).
  applyTheme(document.documentElement.classList.contains('sl-theme-light') ? 'light' : 'dark');

  // Persist activeTab to localStorage so page refresh keeps the current tab.
  // Inject saved tab into data-signals before Datastar processes it.
  (function() {
    var validTabs = {apps:1, models:1, scheduler:1, agents:1, settings:1};
    var saved = localStorage.getItem('mass-active-tab');
    // Drop unknown values so the server-rendered default ("apps") wins.
    if (saved && !validTabs[saved]) saved = null;
    if (saved) {
      var el = document.querySelector('[data-signals]');
      if (el) {
        try {
          var signals = JSON.parse(el.dataset.signals);
          signals.activeTab = saved;
          el.dataset.signals = JSON.stringify(signals);
        } catch(e) {}
      }
    }
    // Save tab on every click of tab buttons.
    document.addEventListener('click', function(e) {
      var btn = e.target.closest('[data-tab-name]');
      if (btn) localStorage.setItem('mass-active-tab', btn.dataset.tabName);
    });
  })();

  // App search filtering is handled by data-show on each .app-row.

  // Shared debounced button click: returns a function that, when called,
  // schedules a click on the given button ID after `ms` milliseconds.
  // Repeated calls reset the timer.
  window.__massDebouncedClick = function(buttonId, ms) {
    var timer = null;
    return function() {
      clearTimeout(timer);
      timer = setTimeout(function() {
        var btn = document.getElementById(buttonId);
        if (btn) btn.click();
      }, ms);
    };
  };

  // Auto-save: debounce 500ms after any input/change inside [data-mass-autosave].
  var triggerAppSave = window.__massDebouncedClick('pe-autosave-trigger', 500);
  function handleAutoSave(e) {
    if (window.__massInitSync) return;
    if (!e.target.closest('[data-mass-autosave]')) return;
    triggerAppSave();
  }
  document.addEventListener('input', handleAutoSave);
  document.addEventListener('sl-change', handleAutoSave);

  // Resizable panels — shared logic for apps and settings tabs.
  // Each entry: [handleId, panelId, barId, minWidth, maxWidth].
  var _resizePanels = [
    ['mass-resize-handle',    'mass-left-panel',      'mass-resize-bar',     270, 700],
    ['settings-resize-handle','settings-left-panel',  'settings-resize-bar', 480, 800]
  ];
  var _dragPanel = null, _dragStartX = 0, _dragStartW = 0, _dragBarId = '';

  document.addEventListener('mousedown', function(e) {
    for (var i = 0; i < _resizePanels.length; i++) {
      var cfg = _resizePanels[i];
      var handle = document.getElementById(cfg[0]);
      if (!handle || handle.offsetWidth === 0) continue;
      var r = handle.getBoundingClientRect();
      if (r.width === 0 || r.height === 0) continue;
      if (e.clientX < r.left - 4 || e.clientX > r.right + 4) continue;
      if (e.clientY < r.top || e.clientY > r.bottom) continue;
      var panel = document.getElementById(cfg[1]);
      if (!panel) continue;
      _dragPanel  = cfg;
      _dragStartX = e.clientX;
      _dragStartW = panel.offsetWidth;
      _dragBarId  = cfg[2];
      panel.style.minWidth = '0';
      document.body.style.cursor     = 'col-resize';
      document.body.style.userSelect = 'none';
      var bar = document.getElementById(_dragBarId);
      if (bar) bar.style.background = 'var(--mass-blue)';
      e.preventDefault();
      e.stopPropagation();
      return;
    }
  }, true);

  document.addEventListener('mousemove', function(e) {
    if (!_dragPanel) return;
    var panel = document.getElementById(_dragPanel[1]);
    if (!panel) return;
    var w = Math.max(_dragPanel[3], Math.min(_dragPanel[4], _dragStartW + e.clientX - _dragStartX));
    panel.style.width = w + 'px';
    e.preventDefault();
  }, true);

  document.addEventListener('mouseup', function(e) {
    if (!_dragPanel) return;
    document.body.style.cursor     = '';
    document.body.style.userSelect = '';
    var bar = document.getElementById(_dragBarId);
    if (bar) bar.style.background = '';
    _dragPanel = null;
  }, true);

  // File browser: used by app UIs via window.__massBrowse(signalName, ext).
  window.__massBrowse = function(signal, ext) {
    var _selectedPath = '';
    var dlg = document.getElementById('mass-file-browser');
    if (!dlg) return;
    dlg.label = 'Browse' + (ext ? ' (' + ext + ' files)' : '');
    dlg.show();
    var selectBtn = document.getElementById('mass-fb-select');
    selectBtn.disabled = true;
    selectBtn.onclick = function() {
      if (_selectedPath) {
        var el = document.querySelector('[data-bind="' + signal + '"]');
        if (el) { el.value = _selectedPath; el.dispatchEvent(new Event('input', {bubbles:true})); }
        dlg.hide();
      }
    };

    window.__massFileBrowser({
      pathElId: 'mass-fb-path',
      entriesElId: 'mass-fb-entries',
      ext: ext || '',
      onSelect: function(path) {
        _selectedPath = path;
        selectBtn.disabled = false;
      },
      onNavigate: function() {
        _selectedPath = '';
        selectBtn.disabled = true;
      }
    });
  };

  // Model select: used by app UIs via window.__massModelSelect(signalName, type).
  // Opens a dialog with local models filtered by type. Clicking a model sets the signal.
  var _modelSelectSignal = '';
  window.__massModelSelect = function(signal, filterType) {
    _modelSelectSignal = signal;
    var dlg = document.getElementById('mass-model-select-dialog');
    if (!dlg) return;
    dlg.label = 'Select ' + (filterType || 'Model');
    // Show spinner while loading.
    var inner = document.getElementById('mass-model-select-inner');
    if (inner) inner.innerHTML = '<div class="text-center py-8"><sl-spinner style="font-size:1.5rem;--track-width:3px"></sl-spinner></div>';
    dlg.show();
    // Fetch model list as HTML.
    var url = '/api/v1/models/select' + (filterType ? '?type=' + encodeURIComponent(filterType) : '');
    fetch(url).then(function(r) { return r.text(); }).then(function(html) {
      if (inner) inner.innerHTML = html;
      // Wire up debounced search filter.
      var si = document.getElementById('mass-model-select-search');
      if (si) {
        // Defer focus — sl-input needs time to upgrade its shadow DOM after innerHTML insertion.
        si.updateComplete ? si.updateComplete.then(function() { si.focus(); }) : setTimeout(function() { si.focus(); }, 0);
        var ft;
        var filterRows = function() {
          clearTimeout(ft);
          ft = setTimeout(function() {
            var terms = si.value.toLowerCase().split(/\s+/).filter(Boolean);
            var rows = document.querySelectorAll('#mass-model-select-entries [data-filename]');
            for (var i = 0; i < rows.length; i++) {
              var hay = (rows[i].getAttribute('data-filename') || '').toLowerCase() + ' ' + (rows[i].getAttribute('data-model-type') || '');
              var match = true;
              for (var j = 0; j < terms.length; j++) { if (hay.indexOf(terms[j]) < 0) { match = false; break; } }
              rows[i].style.display = match ? '' : 'none';
            }
          }, 150);
        };
        si.addEventListener('sl-input', filterRows);
        si.addEventListener('sl-clear', function() { si.value = ''; filterRows(); });
      }
    }).catch(function(err) {
      if (inner) inner.innerHTML = '<p class="text-red-400 text-sm px-3 py-4">Failed to load models.</p>';
    });
  };
  window.__massSelectModel = function(path) {
    if (_modelSelectSignal) {
      var el = document.querySelector('[data-bind="' + _modelSelectSignal + '"]');
      if (el) {
        el.value = path;
        el.dispatchEvent(new Event('input', {bubbles:true}));
        el.dispatchEvent(new Event('change', {bubbles:true}));
      }
    }
    var dlg = document.getElementById('mass-model-select-dialog');
    if (dlg) dlg.hide();
  };

  // Deselect app: click on empty space in sidebar or content area.
  function deselectApp() {
    var btn = document.getElementById('mass-deselect-trigger');
    if (btn) btn.click();
  }
  document.addEventListener('click', function(e) {
    var t = e.target;
    var list = document.getElementById('app-list');
    var wrapper = document.getElementById('app-content-wrapper');
    var content = document.getElementById('app-content');
    if (t === list || t === wrapper || t === content) {
      deselectApp();
    }
  });

  // Collapse: toggle panel width.
  document.addEventListener('click', function(e) {
    var btn = document.getElementById('mass-collapse-btn');
    if (!btn) return;
    var path = e.composedPath ? e.composedPath() : [e.target];
    if (!path.some(function(el) { return el === btn; })) return;
    var panel  = document.getElementById('mass-left-panel');
    var handle = document.getElementById('mass-resize-handle');
    if (!panel || !handle) return;
    if (handle.offsetWidth > 0) {
      // Collapsing: save width, shrink panel, hide handle entirely
      panel.dataset.prevW = panel.offsetWidth;
      panel.style.width = '60px';
      panel.style.minWidth = '0';
      handle.style.width = '0';
      handle.style.cursor = '';
    } else {
      // Expanding: restore panel width, restore handle
      var w = panel.dataset.prevW || 320;
      panel.style.width = w + 'px';
      panel.style.minWidth = '270px';
      handle.style.width = '8px';
      handle.style.cursor = 'col-resize';
    }
  });

  // Keep Apps-tab dialogs centered relative to the right content panel.
  function syncDialogOffset(e) {
    var dlg = e.target.closest('sl-dialog');
    var panel = document.getElementById('mass-left-panel');
    if (!dlg || !panel) return;
    var handle = document.getElementById('mass-resize-handle');
    var hw = handle ? handle.offsetWidth : 0;
    dlg.style.setProperty('--mass-sidebar-w', (panel.offsetWidth + hw) + 'px');
  }
  ['mass-add-app-dialog', 'mass-confirm-uninstall-dialog', 'mass-file-browser',
   'mass-hf-dialog', 'mass-model-select-dialog'].forEach(function(id) {
    var d = document.getElementById(id);
    if (d) d.addEventListener('sl-show', syncDialogOffset);
  });

  // Sync logs when the page regains focus (e.g. after switching to another
  // application like Bruno/Postman).  The SSE connection may have stalled
  // while the window was inactive, so we fetch the current log buffers and
  // replace the DOM.  Uses both 'focus' (covers app switching) and
  // 'visibilitychange' (covers tab switching).
  var _syncPending = false;
  function syncLogs() {
    if (_syncPending) return;
    _syncPending = true;
    var url = '/internal/sync-logs';
    var row = document.querySelector('.app-row.mass-row-active');
    if (row && row.dataset.appName) {
      url += '?app=' + encodeURIComponent(row.dataset.appName);
    }
    fetch(url).then(function(r) { return r.json(); }).then(function(data) {
      if (data.sysLog !== undefined) {
        var el = document.getElementById('syslog-entries');
        if (el) { el.innerHTML = data.sysLog; el.scrollTop = el.scrollHeight; }
      }
      if (data.appLog) {
        var el = document.getElementById('log-entries');
        if (el) { el.innerHTML = data.appLog; el.scrollTop = el.scrollHeight; }
      }
    }).catch(function() {}).finally(function() { _syncPending = false; });
  }
  window.addEventListener('focus', syncLogs);
  document.addEventListener('visibilitychange', function() {
    if (document.visibilityState === 'visible') syncLogs();
  });

  // Disable browser autocomplete on all sl-input elements globally.
  document.querySelectorAll('sl-input').forEach(function(el) { el.setAttribute('autocomplete', 'off'); });

  // Client-side filter: shared helper for Scheduler and Agents tabs.
  // Filters child elements with [data-filter-text] inside a container.
  // Optional onFilter callback receives (total, visible) counts.
  function setupFilter(inputId, containerId, onFilter) {
    var input = document.getElementById(inputId);
    if (!input) return null;
    var timer;
    function doFilter() {
      var container = document.getElementById(containerId);
      if (!container) return;
      var terms = (input.value || '').toLowerCase().split(/\s+/).filter(Boolean);
      var items = container.querySelectorAll('[data-filter-text]');
      var total = items.length, visible = 0;
      for (var i = 0; i < items.length; i++) {
        var hay = items[i].getAttribute('data-filter-text') || '';
        var match = true;
        for (var j = 0; j < terms.length; j++) {
          if (hay.indexOf(terms[j]) < 0) { match = false; break; }
        }
        items[i].style.display = match ? '' : 'none';
        if (match) visible++;
      }
      if (onFilter) onFilter(total, visible, terms.length > 0);
    }
    input.addEventListener('sl-input', function() {
      clearTimeout(timer);
      timer = setTimeout(doFilter, 150);
    });
    input.addEventListener('sl-clear', function() {
      input.value = '';
      doFilter();
    });
    return doFilter;
  }
  setupFilter('scheduler-filter-input', 'scheduler-content');
  var workersFilterFn = setupFilter('workers-filter-input', 'workers-list', function(total, visible, hasFilter) {
    var label = document.getElementById('bench-all-workers-label');
    var btn = document.getElementById('bench-all-workers-btn');
    if (label) label.textContent = hasFilter ? 'Benchmark Selected' : 'Benchmark All';
    if (btn) btn.disabled = hasFilter && visible === 0;
  });
  window.__reapplyWorkersFilter = workersFilterFn || function() {};

  // Collect visible worker IDs for filtered benchmarking.
  window.__visibleWorkerIds = function() {
    var ids = [];
    document.querySelectorAll('#workers-list .worker-card').forEach(function(c) {
      if (c.style.display !== 'none') {
        var id = (c.id || '').replace('worker-card-', '');
        if (id) ids.push(id);
      }
    });
    return ids.join(',');
  };
})();
