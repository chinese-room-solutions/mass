// Models tab: handles delete confirmation + selected-model detail fetch.
(function() {
  // ── Sync both side-panel spacers with the first list card's vertical
  // position so the panel headers line up with the first row.
  var mScroll = document.getElementById('models-list-scroll');
  var mSpacers = ['models-props-spacer', 'models-bench-spacer']
    .map(function(id) { return document.getElementById(id); })
    .filter(Boolean);
  if (mScroll && mSpacers.length) {
    var syncTop = function() {
      var tab = mScroll.closest('#models-tab');
      if (!tab) return;
      var tabRect = tab.getBoundingClientRect();
      var scrollTop = mScroll.getBoundingClientRect().top - tabRect.top;
      var scrollPad = parseFloat(getComputedStyle(mScroll).paddingTop) || 0;
      mSpacers.forEach(function(s) { s.style.height = (scrollTop + scrollPad) + 'px'; });
    };
    syncTop();
    new ResizeObserver(syncTop).observe(mScroll.parentElement);
  }

  // Right-pane detail (model props panel). The runtime that owns the
  // currently-selected row serves the rendered HTML at
  //   /mass.<runtime>.v1/Models/Detail?id=<store-relative-id>
  //
  // Datastar's signal-patch event only carries signals that *changed* in
  // the latest patch, so we mirror the runtime here as the user clicks
  // through rows. First click sets both id+runtime; subsequent clicks
  // within the same runtime only patch id, but our cached runtime stays
  // valid.
  var _lastRuntime = '';

  // MASS's own benchmark card fills the left panel and is served by MASS.
  // It moves on its own (a bench concludes, a re-bench starts), so it
  // refreshes on a timer for as long as a model is selected. The refresh
  // swaps the whole card, so it carries the reader's place across — filter
  // text and row scroll — and stands down while they're typing into it.
  var _benchTimer = null;
  var _benchID = '';
  function loadBenchCard() {
    var pane = document.getElementById('model-bench-pane');
    if (!pane) return;
    if (!_benchID || !_lastRuntime) { pane.innerHTML = ''; return; }
    var input = pane.querySelector('#model-bench-filter-input');
    if (input && document.activeElement === input) return;
    var filter = input ? input.value : '';
    var rows = pane.querySelector('#model-bench-rows');
    var scrolled = rows ? rows.scrollTop : 0;
    fetch('/api/models/benchmarks?runtime=' + encodeURIComponent(_lastRuntime) +
          '&id=' + encodeURIComponent(_benchID))
      .then(function(r) { return r.text(); })
      .then(function(html) {
        pane.innerHTML = html;
        bindBenchFilter(filter);
        var fresh = pane.querySelector('#model-bench-rows');
        if (fresh && scrolled) fresh.scrollTop = scrolled;
      })
      .catch(function() {});
  }
  // The card's filter input is part of the swapped HTML, so its binding is
  // re-made whenever a new one appears — after our own fetch, and after the
  // re-bench SSE patch, which replaces the card without going through us.
  // Binding rides the shell's generic data-filter-text helper.
  var _boundFilter = null;
  function bindBenchFilter(value) {
    var input = document.getElementById('model-bench-filter-input');
    if (!input || input === _boundFilter) return;
    if (typeof window.__massSetupFilter !== 'function') return;
    if (value) input.value = value;
    window.__massSetupFilter('model-bench-filter-input', 'model-bench-rows');
    _boundFilter = input;
  }
  var benchPane = document.getElementById('model-bench-pane');
  if (benchPane) {
    new MutationObserver(function() { bindBenchFilter(''); }).observe(benchPane, {childList: true});
  }
  function watchBench(id) {
    _benchID = id;
    if (_benchTimer) { clearInterval(_benchTimer); _benchTimer = null; }
    loadBenchCard();
    if (id) _benchTimer = setInterval(loadBenchCard, 5000);
  }

  document.addEventListener('datastar-signal-patch', function(e) {
    if (!e.detail) return;
    if ('selectedModelRuntime' in e.detail) {
      _lastRuntime = e.detail.selectedModelRuntime || '';
    }
    if (!('selectedModelID' in e.detail)) return;
    var pane = document.getElementById('model-detail-pane');
    if (!pane) return;
    var id = e.detail.selectedModelID || '';
    if (!id) { pane.innerHTML = ''; watchBench(''); return; }
    if (!_lastRuntime) { pane.innerHTML = ''; watchBench(''); return; }
    watchBench(id);
    fetch('/mass.' + _lastRuntime + '.v1/Models/Detail?id=' + encodeURIComponent(id))
      .then(function(r) { return r.text(); })
      .then(function(html) { pane.innerHTML = html; })
      .catch(function() {
        pane.innerHTML = '<div class="text-sm text-red-400 text-center py-12 px-4">Failed to load model.</div>';
      });
  });

  // Track which group card the mouse is over so F2 knows what to
  // rename. Matches Windows Explorer's F2-on-hover behaviour.
  var _hoverGroup = null;
  document.addEventListener('mouseover', function(e) {
    var g = e.target.closest && e.target.closest('details.group-card');
    if (g) _hoverGroup = g;
  });
  document.addEventListener('mouseout', function(e) {
    if (!e.relatedTarget) { _hoverGroup = null; return; }
    var g = e.relatedTarget.closest && e.relatedTarget.closest('details.group-card');
    if (!g) _hoverGroup = null;
  });

  // F2 renames the hovered group card. Skipped while the operator is
  // typing into a field — otherwise F2 in HF search or the rename
  // input itself would steal the keystroke.
  document.addEventListener('keydown', function(e) {
    if (e.key !== 'F2') return;
    var t = e.target;
    if (t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.isContentEditable)) return;
    var span = _hoverGroup && _hoverGroup.querySelector('.group-name');
    if (!span || typeof window.__massBeginRenameGroup !== 'function') return;
    e.preventDefault();
    window.__massBeginRenameGroup(span);
  });

  // Delete a single model owned by the given runtime. Routes to MASS's
  // own /api/models/delete: the runtime decides which files make up the
  // model, MASS removes them from its store (byte ops stay MASS-side).
  // Returns true on success, false on failure (alert already shown).
  async function deleteOne(kind, id) {
    if (!kind) {
      window.massAlert('Cannot delete: missing runtime kind.', {title: 'Delete Failed', variant: 'danger'});
      return false;
    }
    var resp = await fetch('/api/models/delete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ runtime_name: kind, id: id }),
    });
    if (!resp.ok) {
      window.massAlert(window.massErrorText(await resp.text()) || ('HTTP ' + resp.status), {title: 'Delete Failed', variant: 'danger'});
      return false;
    }
    // Drop the row from the DOM directly — the per-row SSE stream is
    // one-shot per walk (rebuilt on next page open / tab activation), so
    // no remove event will arrive on its own. Selector mirrors the
    // [data-row-id="..."] the gateway emits on stream connect.
    var rowSelector = '[data-row-id="' + id.replace(/"/g, '\\"') + '"]';
    document.querySelectorAll(rowSelector).forEach(function(el) { el.remove(); });
    return true;
  }

  window.__massConfirmDeleteModel = async function(kind, id) {
    var ok = await deleteOne(kind, id);
    if (!ok) return;
    var pane = document.getElementById('model-detail-pane');
    if (pane) pane.innerHTML = '';
    watchBench('');
    // Remove any group card whose last row just left. Same sweep
    // __massConfirmDeleteGroup uses; the per-row SSE stream is one-shot
    // so we own this teardown here.
    document.querySelectorAll('details.group-card').forEach(function(g) {
      if (!g.querySelector('[data-row-id]')) g.remove();
    });
  };

  // Delete every variant of a group sequentially. Payload is the JSON
  // string the server stamped on the group's trash icon. Each entry is
  // {kind, id}. Failures surface via massAlert per-file but the loop
  // keeps going — partial deletes are OK; the user can retry.
  window.__massConfirmDeleteGroup = async function(payload) {
    var entries;
    try { entries = JSON.parse(payload || '[]'); } catch (e) { entries = []; }
    if (!Array.isArray(entries) || entries.length === 0) return;
    var firstID = '';
    for (var i = 0; i < entries.length; i++) {
      var e = entries[i];
      if (!firstID) firstID = e.id || '';
      await deleteOne(e.kind, e.id);
    }
    // Clear detail pane if the user had any of this group's variants
    // selected. Cheapest correct check: clear unconditionally — there's
    // no other detail to lose context on after a bulk delete.
    var pane = document.getElementById('model-detail-pane');
    if (pane) pane.innerHTML = '';
    watchBench('');
    // Remove the now-empty group container. Its rows already left via
    // deleteOne(); the surrounding <details> just needs to go too.
    if (firstID) {
      var stray = document.querySelector('[data-row-id="' + firstID.replace(/"/g, '\\"') + '"]');
      // unlikely to still exist; the parent walk below is the real cleanup
      void stray;
    }
    document.querySelectorAll('details.group-card').forEach(function(g) {
      if (!g.querySelector('[data-row-id]')) g.remove();
    });
  };
})();
