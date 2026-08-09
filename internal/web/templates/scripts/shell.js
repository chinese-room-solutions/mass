(function() {
  // ── Theme: watch for Datastar signal patches and update <html>/<body>
  // classes. Registry-driven via window.__massThemes (injected by Layout): a
  // theme carries a base (dark|light) plus an sl-theme-<name> overlay class for
  // pluggable themes; those overlays also carry the mass-theme-custom marker so
  // the generic utility overrides in input.tw.css apply.
  // baseHint is the server's $themeBase signal. It wins over the injected
  // registry because a theme installed since page load isn't in that snapshot
  // (the script tag can't be re-executed over SSE), and the server knows the
  // base of the theme it just registered.
  function applyTheme(t, baseHint) {
    var known = (window.__massThemes || {})[t];
    var isLight = (baseHint || (known && known.base) || 'dark') === 'light';
    var base = isLight ? 'light' : 'dark';
    var custom = t !== base;

    var cls = 'sl-theme-' + base + (isLight ? '' : ' dark');
    if (custom) cls += ' sl-theme-' + t + ' mass-theme-custom';
    document.documentElement.className = cls;
    document.documentElement.dataset.theme = t;

    document.body.className = (isLight ? 'bg-neutral-100 text-neutral-900' : 'bg-neutral-950 text-neutral-100') + ' min-h-screen';

    document.querySelectorAll('sl-dialog').forEach(function(d) {
      Array.prototype.slice.call(d.classList).forEach(function(c) {
        if (c.indexOf('sl-theme-') === 0) d.classList.remove(c);
      });
      d.classList.add('sl-theme-' + base);
      if (custom) d.classList.add('sl-theme-' + t);
    });
  }
  document.addEventListener('datastar-signal-patch', function(e) {
    if (e.detail && e.detail.theme !== undefined) applyTheme(e.detail.theme, e.detail.themeBase);
  });
  applyTheme(document.documentElement.dataset.theme || 'dark');

  // ── Shoelace → native event bridge for Datastar two-way binding.
  // Datastar's data-bind on a custom element listens for native input/change,
  // but Shoelace inputs emit namespaced sl-input/sl-change instead, so the
  // signal never updates from typing. Re-dispatch a native bubbling event on
  // the same element so every data-bind on a Shoelace input syncs DOM→signal.
  [['sl-input', 'input'], ['sl-change', 'change']].forEach(function(pair) {
    document.addEventListener(pair[0], function(e) {
      var el = e.target;
      if (el && el.tagName && el.tagName.indexOf('-') !== -1) {
        el.dispatchEvent(new Event(pair[1], { bubbles: true }));
      }
    }, true);
  });

  // ── Shared debounced button click: returns a function that, when called,
  // schedules a click on the given button ID after `ms` ms. Repeated calls
  // reset the timer. Used by settings_autosave.js to coalesce rapid input.
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

  // ── Persist activeTab to localStorage + lazy-fetch each tab's content.
  // The dashboard ships an empty shell so initial render is fast; tab
  // bodies are filled in here on first activation, then cached.
  var _tabLoaded = {runtimes: true}; // runtimes is server-rendered inline.
  function loadTab(name, force) {
    if (!force && _tabLoaded[name]) {
      if (name === 'workers') openWorkersStream();
      if (name === 'scheduler') openSchedulerStream();
      if (name === 'queue') openQueueStream();
      // Models tab is fully Datastar-driven — its #models-list element opens
      // /api/models/stream via data-on-load, so there's nothing to wire here.
      return;
    }
    var url, target;
    switch (name) {
      case 'scheduler': url = '/api/scheduler/list'; target = 'scheduler-list'; break;
      case 'workers':   url = '/api/workers/list';   target = 'workers-list'; break;
      case 'queue':     url = '/api/queue/list';     target = 'queue-list'; break;
      default: return;
    }
    var el = document.getElementById(target);
    if (!el) return;
    fetch(url)
      .then(function(r) { return r.text(); })
      .then(function(html) {
        el.innerHTML = html;
        _tabLoaded[name] = true;
        if (name === 'workers') openWorkersStream();
        if (name === 'scheduler') openSchedulerStream();
        if (name === 'queue') openQueueStream();
      })
      .catch(function() { /* leave the empty state; user can switch tabs to retry */ });
  }
  (function() {
    var validTabs = {runtimes:1, models:1, scheduler:1, queue:1, workers:1, settings:1};
    var saved = localStorage.getItem('mass-active-tab');
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
    // Load whichever tab will be active on first paint.
    setTimeout(function() { loadTab(saved || 'runtimes'); }, 0);
    document.addEventListener('click', function(e) {
      var btn = e.target.closest('[data-tab-name]');
      if (!btn) return;
      var name = btn.dataset.tabName;
      localStorage.setItem('mass-active-tab', name);
      loadTab(name);
    });
  })();

  // ── Workers tab live SSE: gauge updates in place; full list refetch on change.
  var _workersSSE = null;
  function openWorkersStream() {
    if (_workersSSE) return;
    _workersSSE = new EventSource('/api/workers/events');
    _workersSSE.addEventListener('stats', function(e) {
      var payload;
      try { payload = JSON.parse(e.data); } catch(err) { return; }
      if (!payload || !payload.workers) return;
      var needsRefetch = false;
      payload.workers.forEach(function(w) {
        (w.stats || []).forEach(function(s) {
          var scoped = w.worker_id + '_' + s.device_id;
          // Initial /api/workers/list may have been rendered before the
          // first heartbeat arrived; if so the gauge slots aren't in the
          // DOM yet. Refetch once so future ticks have something to update.
          if (!document.getElementById('gauge-' + scoped + '-mem') &&
              !document.getElementById('gauge-' + scoped + '-util')) {
            needsRefetch = true;
            return;
          }
          if (s.total_mb > 0) {
            var pct = Math.min(100, s.used_mb / s.total_mb * 100);
            updateGauge(scoped + '-mem', pct, formatMem(s.used_mb) + ' / ' + formatMem(s.total_mb));
          }
          updateGauge(scoped + '-util', Math.min(100, s.util_pct || 0), Math.round(s.util_pct || 0) + '%');
        });
      });
      if (needsRefetch) refetchWorkersList();
    });
    _workersSSE.addEventListener('change', refetchWorkersList);
    _workersSSE.onerror = function() {
      // Browser auto-reconnects; nothing to do here.
    };
  }
  // refetchWorkersList re-renders #workers-list while preserving which
  // sl-details cards the user has expanded — mirrors the old
  // patchWorkersList snapshot/restore dance. Exposed as window.__massRefetchWorkers
  // so inline onclick="...fetch(...).then(window.__massRefetchWorkers)" can
  // refresh the icon state immediately after a toggle POST.
  function refetchWorkersList() {
    var open = {};
    document.querySelectorAll('#workers-list .worker-card').forEach(function(c) {
      if (c.open) open[c.id] = true;
    });
    fetch('/api/workers/list')
      .then(function(r) { return r.text(); })
      .then(function(html) {
        var el = document.getElementById('workers-list');
        if (!el) return;
        el.innerHTML = html;
        for (var k in open) {
          var c = document.getElementById(k);
          if (c) c.open = true;
        }
      })
      .catch(function() {});
  }
  window.__massRefetchWorkers = refetchWorkersList;

  // ── Scheduler tab live SSE: refetch list on every "change" event.
  // Unlike Workers, no periodic stats tick — Scheduler-tab values only move
  // when load/active counters change, which the server already detects.
  var _schedulerSSE = null;
  function openSchedulerStream() {
    if (_schedulerSSE) return;
    _schedulerSSE = new EventSource('/api/scheduler/events');
    _schedulerSSE.addEventListener('change', refetchSchedulerList);
    _schedulerSSE.onerror = function() {
      // Browser auto-reconnects; nothing to do here.
    };
  }
  function refetchSchedulerList() {
    fetch('/api/scheduler/list')
      .then(function(r) { return r.text(); })
      .then(function(html) {
        var el = document.getElementById('scheduler-list');
        if (!el) return;
        el.innerHTML = html;
        if (typeof window.__massSchedulerPruneSelection === 'function') {
          window.__massSchedulerPruneSelection(el);
        }
      })
      .catch(function() {});
  }

  // ── Queue tab live SSE: refetch list on every "change" event.
  // Queue mutations (Submit insert, dispatch pop, terminal frame, cancel,
  // disconnect drain) all broadcast a "change" event from the scheduler;
  // refetching the rendered HTML stays cheap because rows are flat and
  // each queue section's body is collapsed until the operator unfolds it.
  var _queueSSE = null;
  function openQueueStream() {
    if (_queueSSE) return;
    _queueSSE = new EventSource('/api/queue/events');
    _queueSSE.addEventListener('change', refetchQueueList);
    _queueSSE.onerror = function() {
      // Browser auto-reconnects; nothing to do here.
    };
  }
  function refetchQueueList() {
    // Preserve which collapsible cards the operator has open, same dance
    // as refetchWorkersList. Queue rows churn frequently; closing every
    // card on every refetch would make the tab unusable.
    var open = {};
    document.querySelectorAll('#queue-list .queue-card').forEach(function(c) {
      if (c.open) open[c.id] = true;
    });
    fetch('/api/queue/list')
      .then(function(r) { return r.text(); })
      .then(function(html) {
        var el = document.getElementById('queue-list');
        if (!el) return;
        el.innerHTML = html;
        for (var k in open) {
          var c = document.getElementById(k);
          if (c) c.open = true;
        }
      })
      .catch(function() {});
  }

  // ── Cross-tab navigation helpers used by the Scheduler tab.
  //
  // Defined on window because the Scheduler list is injected via innerHTML —
  // any <script> tag the server emits inside that HTML doesn't execute, so
  // templ's per-row script blocks would never bind. These globals always
  // exist (shell.js is a real page <script>), so inline onclick="" handlers
  // can call them safely after any list refetch.
  window.__massGoToWorker = function(workerID) {
    var tabBtn = document.querySelector('[data-tab-name="workers"]');
    if (tabBtn) tabBtn.click();
    var attempt = 0;
    (function tryOpen() {
      attempt++;
      var card = document.getElementById('worker-card-' + workerID);
      if (card) {
        card.open = true;
        card.scrollIntoView({behavior: 'smooth', block: 'nearest'});
        return;
      }
      // Workers list is lazy-fetched on first activation; retry until it lands.
      if (attempt < 20) setTimeout(tryOpen, 100);
    })();
  };

  window.__massGoToModel = function(modelID) {
    // Gateway IDs are "<store-relative-path>#<fingerprint>"; the model
    // rows only carry the path component on data-row-id.
    var idx = modelID.lastIndexOf('#');
    var storeID = idx >= 0 ? modelID.substring(0, idx) : modelID;
    var tabBtn = document.querySelector('[data-tab-name="models"]');
    if (tabBtn) tabBtn.click();
    var attempt = 0;
    (function tryOpen() {
      attempt++;
      var rows = document.querySelectorAll('#models-list [data-row-id]');
      var row = null;
      for (var i = 0; i < rows.length; i++) {
        if (rows[i].getAttribute('data-row-id') === storeID) { row = rows[i]; break; }
      }
      if (!row) {
        if (attempt < 20) setTimeout(tryOpen, 100);
        return;
      }
      var group = row.closest('details');
      if (group) group.open = true;
      row.click();
      row.scrollIntoView({behavior: 'smooth', block: 'nearest'});
    })();
  };
  // Match the SVG ring geometry in templates.writeGauge (r=28).
  var _GAUGE_CIRC = 2 * Math.PI * 28;
  function updateGauge(id, pct, subtitle) {
    var svg = document.getElementById('gauge-' + id);
    if (!svg) return;
    var circles = svg.querySelectorAll('circle');
    if (circles.length >= 2) {
      var c = circles[1];
      c.setAttribute('stroke-dashoffset', (_GAUGE_CIRC * (1 - pct/100)).toFixed(2));
      c.setAttribute('stroke', barColor(pct));
    }
    var t = svg.querySelector('text');
    if (t) t.textContent = Math.round(pct) + '%';
    var spans = svg.parentNode ? svg.parentNode.querySelectorAll('span') : [];
    if (spans.length >= 2) spans[1].textContent = subtitle;
  }
  // Mirrors templates.BarColor: success → warning → danger via theme tokens.
  function barColor(pct) {
    if (pct < 0) pct = 0;
    if (pct > 100) pct = 100;
    if (pct <= 50) {
      return 'color-mix(in srgb, var(--mass-warning) ' + Math.round(pct * 2) + '%, var(--mass-success))';
    }
    return 'color-mix(in srgb, var(--mass-danger) ' + Math.round((pct - 50) * 2) + '%, var(--mass-warning))';
  }
  function formatMem(mb) {
    if (mb >= 1024) return (mb / 1024).toFixed(1) + ' GB';
    return mb + ' MB';
  }

  // ── Resizable panels.
  var _resizePanels = [
    ['mass-resize-handle',     'mass-left-panel',     'mass-resize-bar',     270, 700],
    ['settings-resize-handle', 'settings-left-panel', 'settings-resize-bar', 480, 800],
    ['models-resize-handle',   'models-left-panel',   'models-resize-bar',   320, 720],
    ['scheduler-resize-handle','scheduler-left-panel','scheduler-resize-bar',320, 720]
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
      _dragPanel = cfg;
      _dragStartX = e.clientX;
      _dragStartW = panel.offsetWidth;
      _dragBarId = cfg[2];
      panel.style.minWidth = '0';
      document.body.style.cursor = 'col-resize';
      document.body.style.userSelect = 'none';
      var bar = document.getElementById(_dragBarId);
      if (bar) bar.style.background = 'var(--mass-accent)';
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
  document.addEventListener('mouseup', function() {
    if (!_dragPanel) return;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
    var bar = document.getElementById(_dragBarId);
    if (bar) bar.style.background = '';
    _dragPanel = null;
  }, true);

  // Show/hide the dialog's name row. Settings pickers hide it; the
  // model-import flow shows it (required input).
  function setGroupInputVisible(visible) {
    var row = document.getElementById('mass-fb-group-row');
    var name = document.getElementById('mass-fb-name');
    if (row) row.style.display = visible ? '' : 'none';
    if (name) name.value = '';
  }
  function readImportName() {
    var name = document.getElementById('mass-fb-name');
    var v = name ? name.value.trim() : '';
    return v || '';
  }

  // window.__massInlineAutocomplete attaches ghost-text autocomplete
  // to a text input. While the operator types forward, the helper
  // looks for the first option whose lowercased prefix matches the
  // typed value, fills the rest of the option into the input, and
  // selects from caret..end so further typing replaces the suffix
  // and Enter / Tab accepts the suggestion. Backspace and Delete
  // skip the auto-fill so deletion behaves normally.
  //
  // getOptions is a function returning the current option list;
  // resolved fresh on each keystroke so callers can swap the list
  // (useful when names change while a dialog is open).
  //
  // Returns a detach function the caller invokes to remove the
  // listeners — important for inline editors that get disposed.
  window.__massInlineAutocomplete = function(input, getOptions) {
    if (!input) return function() {};
    var lastKey = '';
    function onKeydown(e) {
      lastKey = e.key;
      // Tab accepts the inline suggestion: collapse the selected
      // suffix into the value and keep focus on the field. Without
      // this Tab would just move focus and the highlighted suffix
      // would feel like a half-applied suggestion.
      if (e.key === 'Tab' && !e.shiftKey && input.selectionStart < input.selectionEnd && input.selectionEnd === input.value.length) {
        e.preventDefault();
        input.setSelectionRange(input.value.length, input.value.length);
      }
    }
    function onInput() {
      // Deletion shouldn't pull a suggestion back in — that fights
      // the operator's intent.
      if (lastKey === 'Backspace' || lastKey === 'Delete') return;
      var typed = input.value;
      if (!typed) return;
      // Only suggest when the caret is at the end — otherwise the
      // operator is editing the middle of an existing name and a
      // ghost-text fill would scramble it.
      if (input.selectionStart !== typed.length || input.selectionEnd !== typed.length) return;
      var lc = typed.toLowerCase();
      var opts = (typeof getOptions === 'function' ? getOptions() : null) || [];
      for (var i = 0; i < opts.length; i++) {
        var opt = opts[i];
        if (!opt || opt.length <= typed.length) continue;
        if (opt.toLowerCase().slice(0, typed.length) !== lc) continue;
        input.value = typed + opt.slice(typed.length);
        input.setSelectionRange(typed.length, opt.length);
        return;
      }
    }
    input.addEventListener('keydown', onKeydown);
    input.addEventListener('input', onInput);
    return function() {
      input.removeEventListener('keydown', onKeydown);
      input.removeEventListener('input', onInput);
    };
  };

  // window.__massFetchGroupNames returns a Promise resolving to the
  // current list of installed group names — the autocomplete source
  // for the import / rename / HF-install name fields. Cached for one
  // second so successive opens don't re-hit the server.
  var _namesCache = null;
  var _namesAt = 0;
  window.__massFetchGroupNames = function() {
    var now = Date.now();
    if (_namesCache && now - _namesAt < 1000) return Promise.resolve(_namesCache);
    return fetch('/api/groups/names')
      .then(function(r) { return r.ok ? r.json() : []; })
      .then(function(names) {
        _namesCache = (names || []).slice().sort();
        _namesAt = Date.now();
        return _namesCache;
      })
      .catch(function() { return []; });
  };

  // ── File browser. Two call shapes:
  //
  //   1. Settings (single-select, no name input):
  //      __massBrowse(signal, ext)          — pick a file
  //      __massBrowse(signal, {dirsOnly:true}) — pick a directory
  //      Writes the picked path into the element matching
  //      [data-bind="<signal>"] when the operator clicks Select. For a
  //      directory pick, the current folder is the selection, so Select
  //      enables as soon as a folder is open.
  //
  //   2. Model import (multi-select, required name input):
  //      __massBrowse({
  //        ext: '',
  //        multiple: true,
  //        nameRequired: true,
  //        onConfirm: fn(paths, name) // called on Select
  //      })
  //      The Select button stays disabled until at least one file is
  //      picked AND the name field is non-empty (when nameRequired).
  window.__massBrowse = function(arg, ext) {
    var dlg = document.getElementById('mass-file-browser');
    if (!dlg) return;
    var selectBtn = document.getElementById('mass-fb-select');
    var nameInput = document.getElementById('mass-fb-name');

    // Shape 1: two-arg form (Settings). ext may be a string (file filter)
    // or an options object {dirsOnly:true} to pick a directory instead.
    if (typeof arg === 'string') {
      var signal = arg;
      var dirsOnly = ext && typeof ext === 'object' ? !!ext.dirsOnly : false;
      var extFilter = typeof ext === 'string' ? ext : '';
      var _selectedPath = '';
      dlg.label = dirsOnly ? 'Choose a folder' : 'Browse' + (extFilter ? ' (' + extFilter + ' files)' : '');
      setGroupInputVisible(false);
      dlg.show();
      selectBtn.disabled = true;
      selectBtn.onclick = function() {
        if (!_selectedPath) return;
        var el = document.querySelector('[data-bind="' + signal + '"]');
        if (el) { el.value = _selectedPath; el.dispatchEvent(new Event('input', {bubbles:true})); }
        dlg.hide();
      };
      window.__massFileBrowser({
        pathElId: 'mass-fb-path',
        entriesElId: 'mass-fb-entries',
        ext: extFilter,
        dirsOnly: dirsOnly,
        // For a directory pick the open folder IS the selection, so Select
        // tracks navigation; for a file pick it tracks the clicked file.
        onSelect: dirsOnly ? undefined : function(path) { _selectedPath = path; selectBtn.disabled = false; },
        onNavigate: function(dir) {
          if (dirsOnly) { _selectedPath = dir; selectBtn.disabled = !dir; }
          else { _selectedPath = ''; selectBtn.disabled = true; }
        }
      });
      return;
    }

    // Shape 2: options-object form (model import).
    var opts = arg || {};
    var _picked = opts.multiple ? [] : '';
    var multiple = !!opts.multiple;
    dlg.label = 'Browse' + (opts.ext ? ' (' + opts.ext + ' files)' : '');
    setGroupInputVisible(true);
    dlg.show();
    function refreshSelectEnabled() {
      var hasPath = multiple ? (_picked.length > 0) : (_picked !== '');
      var hasName = !opts.nameRequired || readImportName() !== '';
      selectBtn.disabled = !(hasPath && hasName);
    }
    var detachAC = function() {};
    if (nameInput) {
      nameInput.oninput = refreshSelectEnabled;
      var nameOptions = [];
      window.__massFetchGroupNames().then(function(names) { nameOptions = names; });
      detachAC = window.__massInlineAutocomplete(nameInput, function() { return nameOptions; });
      nameInput.onkeydown = function(e) {
        if (e.key === 'Enter' && !selectBtn.disabled) {
          e.preventDefault();
          selectBtn.click();
        }
      };
    }
    refreshSelectEnabled();
    function teardownNameInput() {
      detachAC();
      if (nameInput) {
        nameInput.oninput = null;
        nameInput.onkeydown = null;
      }
    }
    selectBtn.onclick = function() {
      if (selectBtn.disabled) return;
      var name = readImportName();
      var picked = multiple ? _picked.slice() : _picked;
      teardownNameInput();
      dlg.hide();
      if (typeof opts.onConfirm === 'function') opts.onConfirm(picked, name);
    };
    dlg.addEventListener('sl-after-hide', function once() {
      teardownNameInput();
      dlg.removeEventListener('sl-after-hide', once);
    });
    window.__massFileBrowser({
      pathElId: 'mass-fb-path',
      entriesElId: 'mass-fb-entries',
      ext: opts.ext || '',
      multiple: multiple,
      onSelect: function(picked) {
        _picked = multiple ? (picked || []) : (picked || '');
        refreshSelectEnabled();
      },
      onNavigate: function() { _picked = multiple ? [] : ''; refreshSelectEnabled(); }
    });
  };

  // ── Scheduler / Models deselect: click on empty space inside the tab
  // (anywhere outside a row and outside the open properties panel) clears
  // the selection signal. Mirrors the Modules-tab deselect pattern; the
  // hidden trigger button owns the Datastar signal write so we don't
  // hand-roll signal mutation here.
  function isInside(target, selector) {
    return target.closest && target.closest(selector) != null;
  }
  document.addEventListener('click', function(e) {
    var t = e.target;
    if (!t || !t.closest) return;

    // Scheduler.
    var schedTab = document.getElementById('scheduler-tab');
    if (schedTab && schedTab.contains(t)) {
      var insideRow   = isInside(t, '.scheduler-row');
      var insidePanel = isInside(t, '#scheduler-props-panel');
      var insideEvict = isInside(t, '[name="x-circle"]') || isInside(t, '[name="x-lg"]');
      if (!insideRow && !insidePanel && !insideEvict) {
        var btn = document.getElementById('scheduler-deselect-trigger');
        if (btn) btn.click();
      }
    }

    // Models.
    var modelsTab = document.getElementById('models-tab');
    if (modelsTab && modelsTab.contains(t)) {
      var insideRow    = isInside(t, '.model-row');
      var insideGroup  = isInside(t, 'details.group-card > summary');
      var insideMPanel = isInside(t, '#models-props-panel') || isInside(t, '#models-bench-panel');
      if (!insideRow && !insideGroup && !insideMPanel) {
        var btn2 = document.getElementById('models-deselect-trigger');
        if (btn2) btn2.click();
      }
    }
  });

  // ── Runtimes deselect: click empty space in sidebar or content area.
  function deselectRuntime() {
    var btn = document.getElementById('mass-deselect-trigger');
    if (btn) btn.click();
  }
  document.addEventListener('click', function(e) {
    var t = e.target;
    var list = document.getElementById('runtime-list');
    var wrapper = document.getElementById('runtime-content-wrapper');
    var welcome = document.getElementById('runtime-welcome');
    var logs = document.getElementById('runtime-logs');
    if (t === list || t === wrapper || t === welcome || t === logs) deselectRuntime();
  });

  // ── Sidebar collapse toggle.
  document.addEventListener('click', function(e) {
    var btn = document.getElementById('mass-collapse-btn');
    if (!btn) return;
    var path = e.composedPath ? e.composedPath() : [e.target];
    if (!path.some(function(el) { return el === btn; })) return;
    var panel  = document.getElementById('mass-left-panel');
    var handle = document.getElementById('mass-resize-handle');
    if (!panel || !handle) return;
    if (handle.offsetWidth > 0) {
      panel.dataset.prevW = panel.offsetWidth;
      panel.style.width = '60px';
      panel.style.minWidth = '0';
      handle.style.width = '0';
    } else {
      var w = panel.dataset.prevW || 320;
      panel.style.width = w + 'px';
      panel.style.minWidth = '270px';
      handle.style.width = '8px';
    }
  });

  // ── Sync system logs after focus regained.
  var _syncPending = false;
  function syncSysLogs() {
    if (_syncPending) return;
    _syncPending = true;
    fetch('/api/sync-logs').then(function(r) { return r.json(); }).then(function(data) {
      if (data && data.sysLog !== undefined) {
        var el = document.getElementById('syslog-entries');
        if (el) { el.innerHTML = data.sysLog; el.scrollTop = el.scrollHeight; }
      }
    }).catch(function() {}).finally(function() { _syncPending = false; });
  }
  window.addEventListener('focus', syncSysLogs);
  document.addEventListener('visibilitychange', function() {
    if (document.visibilityState === 'visible') syncSysLogs();
  });

  // ── Disable autocomplete on all sl-input.
  document.querySelectorAll('sl-input').forEach(function(el) { el.setAttribute('autocomplete', 'off'); });

  // ── Generic data-filter-text filter helper. Used by Workers/Models/Scheduler/Runtimes.
  function setupFilter(inputId, containerId) {
    var input = document.getElementById(inputId);
    if (!input) return;
    var timer;
    function doFilter() {
      var container = document.getElementById(containerId);
      if (!container) return;
      var terms = (input.value || '').toLowerCase().split(/\s+/).filter(Boolean);
      var items = container.querySelectorAll('[data-filter-text]');
      for (var i = 0; i < items.length; i++) {
        var hay = items[i].getAttribute('data-filter-text') || '';
        var match = true;
        for (var j = 0; j < terms.length; j++) {
          if (hay.indexOf(terms[j]) < 0) { match = false; break; }
        }
        items[i].style.display = match ? '' : 'none';
      }
    }
    input.addEventListener('sl-input', function() {
      clearTimeout(timer);
      timer = setTimeout(doFilter, 150);
    });
    input.addEventListener('sl-clear', function() { input.value = ''; doFilter(); });
    doFilter();
  }
  // Panels that re-render their own filter input (the models tab's benchmark
  // card) re-bind through this after each swap.
  window.__massSetupFilter = setupFilter;
  setupFilter('runtime-search-input',   'runtime-list');
  setupFilter('models-filter-input',    'models-list');
  setupFilter('scheduler-filter-input', 'scheduler-list');
  setupFilter('workers-filter-input',   'workers-list');
  setupFilter('queue-filter-input',     'queue-list');
})();
