(function() {
	var _targetSignal = '';
	var _currentDir = '';

	window.__massBrowseDir = function(signal) {
		_targetSignal = signal;
		_currentDir = '';
		var dlg = document.getElementById('settings-file-browser');
		dlg.show();
		var selectBtn = document.getElementById('sfb-select');
		selectBtn.onclick = function() {
			if (_currentDir) {
				var el = document.querySelector('[data-bind="' + _targetSignal + '"]');
				if (el) { el.value = _currentDir; el.dispatchEvent(new Event('input', {bubbles:true})); }
				dlg.hide();
			}
		};

		window.__massFileBrowser({
			pathElId: 'sfb-path',
			entriesElId: 'sfb-entries',
			dirsOnly: true,
			onNavigate: function(dir) { _currentDir = dir; }
		});
	};

	window.__massBrowseFile = function(signal, ext) {
		_targetSignal = signal;
		_currentDir = '';
		var dlg = document.getElementById('settings-file-browser');
		dlg.show();
		var selectBtn = document.getElementById('sfb-select');
		selectBtn.onclick = function() {};

		window.__massFileBrowser({
			pathElId: 'sfb-path',
			entriesElId: 'sfb-entries',
			ext: ext || '',
			onSelect: function(path) {
				var el = document.querySelector('[data-bind="' + _targetSignal + '"]');
				if (el) { el.value = path; el.dispatchEvent(new Event('input', {bubbles:true})); }
				dlg.hide();
			},
			onNavigate: function(dir) { _currentDir = dir; }
		});
	};
})();
