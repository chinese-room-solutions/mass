(function() {
  // Self-update: the Settings > About "Install" button plus a one-per-load
  // announcement. The announcement is a toast rather than a modal — nothing is
  // broken — with the install offered right in it; the About row is the
  // affordance that remains after the toast clears.
  //
  // The fleet gate is the MASS-specific part. When the daemon reports connected
  // workers the new build would strand, the toast says so and the install goes
  // through an explicit confirm before it re-sends with force — a stranded
  // worker exits at Register and stays down until it is upgraded.
  var btn = document.getElementById('mass-update-btn');
  if (!btn) return;

  var tag = btn.getAttribute('data-mass-update-tag') || '';
  var incompatible = parseInt(btn.getAttribute('data-mass-update-incompatible') || '0', 10) || 0;

  function workersPhrase(n) {
    return n === 1 ? '1 connected worker' : n + ' connected workers';
  }

  function post(force) {
    btn.disabled = true;
    return fetch('api/update/apply', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({force: !!force}),
    }).then(function(r) {
      if (r.ok) {
        // The daemon retires behind this: the page's streams drop, and the app
        // comes back on the new build a moment later.
        window.massToast('Updating MASS to ' + tag + ' — restarting…', {duration: Infinity});
        return;
      }
      return r.text().then(function(t) {
        btn.disabled = false;
        window.massAlert(window.massErrorText(t) || ('Update failed (HTTP ' + r.status + ').'),
          {title: 'Update Failed', variant: 'danger'});
      });
    }, function() {
      btn.disabled = false;
      window.massAlert("Couldn't start the update: MASS isn't responding.",
        {title: 'Update Failed', variant: 'danger'});
    });
  }

  function apply() {
    if (incompatible === 0) {
      post(false);
      return;
    }
    var dlg = document.getElementById('mass-confirm-update-dialog');
    if (!dlg) {
      // No dialog to confirm with: refuse rather than force silently.
      window.massAlert('Updating to ' + tag + ' would strand ' + workersPhrase(incompatible) +
        '. Upgrade the workers first, or run `mass update --apply --force`.',
        {title: 'Incompatible Workers', variant: 'danger'});
      return;
    }
    document.getElementById('mass-confirm-update-text').textContent =
      'Updating to ' + tag + ' would strand ' + workersPhrase(incompatible) +
      '. A stranded worker is rejected when it reconnects, exits, and stays down until it is upgraded.';
    dlg.show();
  }

  btn.addEventListener('click', apply);

  var confirmDlg = document.getElementById('mass-confirm-update-dialog');
  if (confirmDlg) {
    document.getElementById('mass-confirm-update-cancel')
      .addEventListener('click', function() { confirmDlg.hide(); });
    document.getElementById('mass-confirm-update-ok')
      .addEventListener('click', function() { confirmDlg.hide(); post(true); });
  }

  // 15s rather than the default: the toast carries the action, and the default
  // is short enough to vanish mid-reach.
  var msg = 'MASS ' + tag + ' is available';
  if (incompatible > 0) msg += ' — ' + workersPhrase(incompatible) + ' would be stranded';
  window.massToast(msg, {
    duration: 15000,
    variant: incompatible > 0 ? 'warning' : undefined,
    action: {label: 'Install', onClick: apply},
  });
})();
