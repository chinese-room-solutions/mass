(function() {
	var triggerSave = window.__massDebouncedClick('settings-autosave-trigger', 500);
	var panel = document.getElementById('settings-left-panel');
	if (panel) {
		panel.addEventListener('input', function(e) {
			if (e.target.closest('sl-select')) return;
			triggerSave();
		});
		panel.addEventListener('sl-input', function(e) {
			if (e.target.closest('sl-select')) return;
			triggerSave();
		});
		panel.addEventListener('sl-change', triggerSave);
	}

	// Data Directory is resolved once, at daemon startup. Saving a different
	// one changes nothing until MASS restarts, and About goes on showing the
	// directory actually in use — so say which one that is, right at the field.
	var notice = document.getElementById('data-dir-restart');
	var input = document.getElementById('settings-data-dir');
	if (!notice || !input) return;
	var running = notice.dataset.running || '';
	var fallback = notice.dataset.default || '';
	// Seeded from the server rather than read off the input: Datastar binds the
	// field from a deferred module, so at this point input.value is still empty
	// and would resolve to the platform default.
	var configured = notice.dataset.configured || '';

	// Windows paths reach here from two sources (config file, folder picker)
	// that disagree on slash and case; compare them normalized so a no-op edit
	// doesn't read as pending.
	function samePath(a, b) {
		var norm = function(p) {
			return p.trim().replace(/[\\/]+$/, '').replace(/\\/g, '/').toLowerCase();
		};
		return norm(a) === norm(b);
	}

	function refresh() {
		var want = configured.trim() || fallback;
		var pending = want !== '' && running !== '' && !samePath(want, running);
		notice.classList.toggle('hidden', !pending);
		if (pending) notice.textContent = 'Still using ' + running + ' — restart MASS to apply.';
	}

	function onEdit() {
		configured = input.value || '';
		refresh();
	}
	input.addEventListener('sl-input', onEdit);
	input.addEventListener('sl-change', onEdit);
	refresh();
})();
