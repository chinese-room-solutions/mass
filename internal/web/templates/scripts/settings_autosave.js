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
})();
