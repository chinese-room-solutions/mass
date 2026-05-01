// Runtimes-tab actions: install, start/stop, auto-start toggle, uninstall,
// and live-log streaming for the selected runtime.

(function() {
  // Manage the live-log EventSource for #runtime-logs whenever
  // $activeRuntime changes. The welcome state is server-rendered into a
  // sibling div and toggled via data-show, so JS never overwrites it.
  var _es = null;
  document.addEventListener('datastar-signal-patch', function(e) {
    if (!e.detail || !('activeRuntime' in e.detail)) return;
    var kind = e.detail.activeRuntime || '';
    var pane = document.getElementById('runtime-logs');
    if (!pane) return;
    if (_es) { _es.close(); _es = null; }
    if (!kind) {
      pane.innerHTML = '';
      return;
    }
    pane.innerHTML = '<div class="space-y-3 p-4">' +
      '<div class="flex items-center gap-2 text-sm font-medium text-neutral-300">' +
      '<sl-icon name="terminal"></sl-icon><span>' + kind + ' &mdash; Live Logs</span></div>' +
      '<div id="log-entries" class="font-mono text-xs rounded-lg p-4 min-h-64 max-h-[calc(100vh-12rem)] overflow-y-auto space-y-px" ' +
      'style="background:var(--mass-bg-panel);border:1px solid var(--mass-border)">' +
      '<p id="log-placeholder" class="text-neutral-500">Waiting for log output...</p>' +
      '</div></div>';
    _es = new EventSource('/api/runtimes/' + encodeURIComponent(kind) + '/logs');
    _es.addEventListener('log', function(ev) {
      var el = document.getElementById('log-entries');
      if (!el) return;
      var ph = document.getElementById('log-placeholder');
      if (ph) ph.remove();
      el.insertAdjacentHTML('beforeend', ev.data);
      while (el.children.length > 1000) el.removeChild(el.firstChild);
      el.scrollTop = el.scrollHeight;
    });
  });
})();

