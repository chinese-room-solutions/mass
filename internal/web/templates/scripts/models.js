// Models tab: handles delete confirmation + selected-model detail fetch.
(function() {
  // ── Sync the right-pane spacer with the first list card's vertical
  // position so the props panel header lines up with the first row.
  var mScroll = document.getElementById('models-list-scroll');
  var mSpacer = document.getElementById('models-props-spacer');
  if (mScroll && mSpacer) {
    var syncTop = function() {
      var tab = mSpacer.closest('#models-tab');
      if (!tab) return;
      var tabRect = tab.getBoundingClientRect();
      var scrollTop = mScroll.getBoundingClientRect().top - tabRect.top;
      var scrollPad = parseFloat(getComputedStyle(mScroll).paddingTop) || 0;
      mSpacer.style.height = (scrollTop + scrollPad) + 'px';
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
  document.addEventListener('datastar-signal-patch', function(e) {
    if (!e.detail) return;
    if ('selectedModelRuntime' in e.detail) {
      _lastRuntime = e.detail.selectedModelRuntime || '';
    }
    if (!('selectedModelID' in e.detail)) return;
    var pane = document.getElementById('model-detail-pane');
    if (!pane) return;
    var id = e.detail.selectedModelID || '';
    if (!id) { pane.innerHTML = ''; return; }
    if (!_lastRuntime) { pane.innerHTML = ''; return; }
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

  // Delete a single model owned by the given runtime. Routes to the
  // runtime's own DELETE endpoint via the /mass.<kind>.<rest> proxy —
  // MASS just forwards bytes; the runtime owns the model on disk.
  // Returns true on success, false on failure (alert already shown).
  async function deleteOne(kind, id) {
    if (!kind) {
      window.massAlert('Cannot delete: missing runtime kind.', {title: 'Delete Failed', variant: 'danger'});
      return false;
    }
    var encoded = id.split('/').map(encodeURIComponent).join('/');
    var resp = await fetch('/mass.' + kind + '.v1/Models/' + encoded, { method: 'DELETE' });
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
