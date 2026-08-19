(function() {
  // Self-update: the two icon buttons beside Settings > About > Version, plus a
  // one-per-load announcement. Both buttons are always in the page — this script
  // hydrates them from api/update/check, so an answer the daemon hadn't got yet
  // when the page rendered no longer hides the affordance. Everything they have
  // to say goes in their tooltips; the row carries no text of its own. The
  // announcement is a toast rather than a modal — nothing is broken — with the
  // install offered right in it; the buttons are what remain after it clears.
  //
  // The fleet gate is the MASS-specific part. When the daemon reports connected
  // workers the new build would strand, the toast says so and the install goes
  // through an explicit confirm before it re-sends with force — a stranded
  // worker exits at Register and stays down until it is upgraded.
  // The window process calls this over the webview bridge when the daemon says
  // an update is being installed: MASS is about to be replaced and the window
  // closed behind this notice. Sticky (duration Infinity) — the window goes away
  // before any timeout would matter, and a notice that vanished first would
  // leave the closing window unexplained. Defined before the row guard: the
  // update may have been applied from the CLI, or from another browser tab.
  // Raised at most once: in the desktop app the clicking page shows it on the
  // 200 and the daemon pushes the same event to the window process a moment
  // later, and two identical sticky toasts would stack.
  var restartingShown = false;
  window.massUpdateRestarting = function(tag) {
    if (restartingShown) return;
    restartingShown = true;
    window.massToast('Updating MASS to ' + tag + ' — restarting…', {duration: Infinity});
  };

  var checkBtn = document.getElementById('mass-update-check-btn');
  if (!checkBtn) return;
  var checkTip = document.getElementById('mass-update-check-tip');
  var installBtn = document.getElementById('mass-update-btn');
  var installTip = document.getElementById('mass-update-install-tip');

  // What the last answer said, so a failed live check can be shown over it
  // rather than throwing the known tag away.
  var last = {};
  var tag = '';
  var incompatible = 0;
  var announced = false;

  function workersPhrase(n) {
    return n === 1 ? '1 connected worker' : n + ' connected workers';
  }

  // checkedLabel is the relative age of an answer. A never-taken one carries
  // Go's zero time, which parses to a negative epoch — hence the > 0 guard.
  function checkedLabel(ts) {
    var t = Date.parse(ts || '');
    if (!(t > 0)) return '';
    var s = (Date.now() - t) / 1000;
    if (s < 90) return 'checked just now';
    if (s < 3600) return 'checked ' + Math.round(s / 60) + 'm ago';
    if (s < 86400) return 'checked ' + Math.round(s / 3600) + 'h ago';
    return 'checked ' + Math.round(s / 86400) + 'd ago';
  }

  function render(st) {
    last = st;
    tag = st.available || '';
    incompatible = st.incompatible || 0;
    installBtn.disabled = !tag;

    if (st.error) {
      // An unreachable release host must not read as "up to date". With no text
      // in the row, the tint is what says so at a glance and the tooltip says
      // why; a check the operator asked for also answers out loud (see check).
      checkBtn.style.color = 'var(--mass-danger)';
      checkTip.content = "Couldn't check for updates — " + st.error;
      installTip.content = tag ? installLabel() : 'No update available';
      return;
    }
    checkBtn.style.color = '';
    var when = checkedLabel(st.checked_at);
    if (!tag) {
      checkTip.content = when ? 'Up to date — ' + when : 'Check for updates';
      installTip.content = 'No update available';
      return;
    }
    checkTip.content = when ? 'Check for updates — ' + when : 'Check for updates';
    installTip.content = installLabel();
    announce();
  }

  // installLabel is the Install button's tooltip when there is something to
  // install. The stranded-worker count rides along: it is the one thing the
  // operator must know before pressing, and the confirm dialog behind the press
  // says it again.
  function installLabel() {
    var msg = 'Install ' + tag;
    if (incompatible > 0) msg += ' — ' + workersPhrase(incompatible) + ' would be stranded';
    return msg;
  }

  function announce() {
    if (announced) return;
    announced = true;
    // 15s rather than the default: the toast carries the action, and the
    // default is short enough to vanish mid-reach.
    var msg = 'MASS ' + tag + ' is available';
    if (incompatible > 0) msg += ' — ' + workersPhrase(incompatible) + ' would be stranded';
    window.massToast(msg, {
      duration: 15000,
      variant: incompatible > 0 ? 'warning' : undefined,
      action: {label: 'Install', onClick: apply},
    });
  }

  // check asks the daemon to go to the network now. The daemon answers 200 with
  // an error field when it couldn't reach the release host, so only a broken
  // request lands in the rejection path.
  function check(loud) {
    checkBtn.disabled = true;
    return fetch('api/update/check', {method: 'POST'}).then(function(r) {
      if (!r.ok) return Promise.reject(new Error('HTTP ' + r.status));
      return r.json();
    }, function() {
      return {version: last.version, available: last.available, error: "MASS isn't responding."};
    }).then(function(st) {
      render(st);
      // A check the operator pressed for gets an answer they can't miss. A
      // hydration on page load doesn't — it would fire on every load.
      if (!loud) return;
      if (st.error) {
        window.massAlert("Couldn't check for updates — " + st.error,
          {title: 'Update Check Failed', variant: 'danger'});
      } else if (!st.available) {
        window.massToast('MASS ' + (st.version || '') + ' is up to date');
      }
    }).then(function() {
      checkBtn.disabled = false;
    });
  }

  // hydrate reads the daemon's cached answer — no network on its side. When it
  // has never taken one (the daemon started moments ago, which is exactly the
  // race this page used to lose), ask for a live one instead of showing nothing.
  function hydrate() {
    fetch('api/update/check').then(function(r) {
      if (!r.ok) return Promise.reject(new Error('HTTP ' + r.status));
      return r.json();
    }).then(function(st) {
      if (!(Date.parse(st.checked_at || '') > 0)) {
        check(false);
        return;
      }
      render(st);
    }, function() {
      checkTip.content = "Couldn't reach MASS to ask about updates.";
      checkBtn.style.color = 'var(--mass-danger)';
    });
  }

  function post(force) {
    installBtn.disabled = true;
    return fetch('api/update/apply', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({force: !!force}),
    }).then(function(r) {
      if (r.ok) {
        // The daemon retires behind this: the page's streams drop, and MASS
        // comes back on the new build a moment later. In the desktop app the
        // daemon also pushes update-restarting to the window process, which
        // calls this same function — so the notice reads identically whether it
        // was raised here or from there.
        window.massUpdateRestarting(tag);
        return;
      }
      return r.text().then(function(t) {
        installBtn.disabled = false;
        window.massAlert(window.massErrorText(t) || ('Update failed (HTTP ' + r.status + ').'),
          {title: 'Update Failed', variant: 'danger'});
      });
    }, function() {
      installBtn.disabled = false;
      window.massAlert("Couldn't start the update: MASS isn't responding.",
        {title: 'Update Failed', variant: 'danger'});
    });
  }

  function apply() {
    if (!tag) return;
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

  checkBtn.addEventListener('click', function() { check(true); });
  installBtn.addEventListener('click', apply);

  var confirmDlg = document.getElementById('mass-confirm-update-dialog');
  if (confirmDlg) {
    document.getElementById('mass-confirm-update-cancel')
      .addEventListener('click', function() { confirmDlg.hide(); });
    document.getElementById('mass-confirm-update-ok')
      .addEventListener('click', function() { confirmDlg.hide(); post(true); });
  }

  hydrate();
})();
