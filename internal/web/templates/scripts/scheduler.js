// Scheduler tab: spacer alignment + fetch right-pane detail HTML for the
// selected loaded-model entry.
(function() {
  // ── Sync the right-pane spacer with the first list card position.
  var sScroll = document.getElementById('scheduler-list-scroll');
  var sSpacer = document.getElementById('scheduler-props-spacer');
  if (sScroll && sSpacer) {
    var syncTop = function() {
      var tab = sSpacer.closest('#scheduler-tab');
      if (!tab) return;
      var tabRect = tab.getBoundingClientRect();
      var scrollTop = sScroll.getBoundingClientRect().top - tabRect.top;
      var scrollPad = parseFloat(getComputedStyle(sScroll).paddingTop) || 0;
      sSpacer.style.height = (scrollTop + scrollPad) + 'px';
    };
    syncTop();
    new ResizeObserver(syncTop).observe(sScroll.parentElement);
  }

  document.addEventListener('datastar-signal-patch', function(e) {
    if (!e.detail || !('selectedSchedulerKey' in e.detail)) return;
    var key = e.detail.selectedSchedulerKey || '';
    var pane = document.getElementById('scheduler-detail-pane');
    if (!pane) return;
    if (!key) {
      pane.innerHTML = '';
      return;
    }
    fetch('/api/scheduler/detail?key=' + encodeURIComponent(key))
      .then(function(r) { return r.text(); })
      .then(function(html) { pane.innerHTML = html; })
      .catch(function() {
        pane.innerHTML = '<div class="text-sm text-red-400 text-center py-12 px-4">Failed to load entry.</div>';
      });
  });
})();
