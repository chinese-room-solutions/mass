(function(){
	// Sync properties panel spacer with first list card position.
	var scroll = document.getElementById('scheduler-list-scroll');
	var spacer = document.getElementById('scheduler-props-spacer');
	if (scroll && spacer) {
		var syncTop = function() {
			var tabRect = spacer.closest('#scheduler-tab').getBoundingClientRect();
			// Use the scroll area top + its padding to find where the first card starts.
			var scrollTop = scroll.getBoundingClientRect().top - tabRect.top;
			var scrollPad = parseFloat(getComputedStyle(scroll).paddingTop) || 0;
			spacer.style.height = (scrollTop + scrollPad) + 'px';
		};
		syncTop();
		new ResizeObserver(syncTop).observe(scroll.parentElement);
	}

	var tab = document.getElementById('scheduler-tab');
	if (!tab) return;
	tab.addEventListener('click', function(e) {
		var t = e.target;
		var content = document.getElementById('scheduler-content');
		if (t === tab || t === scroll || t === content) {
			var btn = document.getElementById('scheduler-deselect-trigger');
			if (btn) btn.click();
		}
	});
})();

(function(){
	var _selectedPath = '';
	var _selectedType = 'chat';

	function setDevice(device) {
		var isGpu = device !== 'cpu';
		['sched-load-gpu-wrap','sched-load-flash-wrap','sched-load-maingpu-wrap','sched-load-tensorsplit-wrap'].forEach(function(id) {
			var el = document.getElementById(id);
			if (el) el.style.display = isGpu ? '' : 'none';
		});
	}

	function setType(modelType) {
		if (!modelType) return;
		_selectedType = modelType;
		var isChat = modelType === 'chat';
		var els = document.querySelectorAll('.sched-load-chat-field');
		for (var i = 0; i < els.length; i++) els[i].style.display = isChat ? '' : 'none';
		var chatTplWrap = document.getElementById('sched-load-chat-template-wrap');
		if (chatTplWrap) chatTplWrap.style.display = isChat ? '' : 'none';
	}

	function applyFilter(q) {
		var container = document.getElementById('sched-load-models');
		if (!container) return;
		var rows = container.querySelectorAll('[data-filename]');
		var terms = q.split(/\s+/).filter(Boolean);
		var visible = 0;
		for (var i = 0; i < rows.length; i++) {
			var hay = (rows[i].getAttribute('data-filename') || '').toLowerCase() + ' ' + (rows[i].getAttribute('data-model-type') || '');
			var show = true;
			for (var j = 0; j < terms.length; j++) { if (hay.indexOf(terms[j]) < 0) { show = false; break; } }
			rows[i].style.display = show ? '' : 'none';
			if (show) visible++;
		}
		// Hide list entirely when exactly one match (selected model).
		container.style.display = (visible <= 1 && _selectedPath) ? 'none' : '';
	}

	// state: 'valid' (green), 'invalid' (red), or '' (default)
	function setFilterState(state) {
		var filterEl = document.getElementById('sched-load-filter');
		if (!filterEl) return;
		var color = state === 'valid' ? '#22c55e' : state === 'invalid' ? '#ef4444' : '';
		if (color) {
			filterEl.style.setProperty('--sl-input-border-color', color);
			filterEl.style.setProperty('--sl-input-border-color-hover', color);
			filterEl.style.setProperty('--sl-input-border-color-focus', color);
		} else {
			filterEl.style.removeProperty('--sl-input-border-color');
			filterEl.style.removeProperty('--sl-input-border-color-hover');
			filterEl.style.removeProperty('--sl-input-border-color-focus');
		}
	}

	function clearSelection() {
		_selectedPath = '';
		_selectedType = 'chat';
		document.getElementById('sched-load-path').value = '';
		var cfg = document.getElementById('sched-load-config');
		if (cfg) cfg.style.display = 'none';
		var container = document.getElementById('sched-load-models');
		if (container) {
			container.querySelectorAll('.sched-load-selected').forEach(function(el) {
				el.classList.remove('sched-load-selected', 'bg-blue-900/40', 'border', 'border-blue-500/30');
			});
			container.style.display = '';
		}
		var filterEl = document.getElementById('sched-load-filter');
		if (filterEl) { filterEl.value = ''; filterEl.focus(); }
		setFilterState('');
		setType('chat');
		applyFilter('');
	}

	function selectRow(row) {
		// Deselect previous.
		var container = document.getElementById('sched-load-models');
		if (container) {
			container.querySelectorAll('.sched-load-selected').forEach(function(el) {
				el.classList.remove('sched-load-selected', 'bg-blue-900/40', 'border', 'border-blue-500/30');
			});
		}
		row.classList.add('sched-load-selected', 'bg-blue-900/40', 'border', 'border-blue-500/30');
		_selectedPath = row.getAttribute('data-model-id') || row.getAttribute('data-path') || '';
		document.getElementById('sched-load-path').value = _selectedPath;
		var modelType = row.getAttribute('data-model-type') || '';
		if (modelType) setType(modelType);
		var cfg = document.getElementById('sched-load-config');
		if (cfg) cfg.style.display = '';
		// Hide the model list after selection.
		if (container) container.style.display = 'none';
		// Fill filter with model name and mark as selected.
		var name = row.getAttribute('data-filename') || '';
		var filterEl = document.getElementById('sched-load-filter');
		if (filterEl) filterEl.value = name;
		setFilterState('valid');
		// Fetch model capabilities to show/hide thinking toggle.
		var thinkWrap = document.getElementById('sched-load-thinking-wrap');
		var thinkSwitch = document.getElementById('sched-load-thinking');
		if (thinkWrap && _selectedPath) {
			fetch('/api/v1/models?id=' + encodeURIComponent(_selectedPath))
				.then(function(r) { return r.ok ? r.json() : null; })
				.then(function(model) {
					var caps = model && model.capabilities ? model.capabilities : {};
					thinkWrap.style.display = caps.thinking ? '' : 'none';
					if (!caps.thinking && thinkSwitch) thinkSwitch.checked = false;
				})
				.catch(function() { thinkWrap.style.display = 'none'; });
		}
	}

	function loadModelList() {
		var container = document.getElementById('sched-load-models');
		if (!container) return;
		container.innerHTML = '<div class="text-center py-4"><sl-spinner style="font-size:1rem;--track-width:2px"></sl-spinner></div>';
		fetch('/api/models/select').then(function(r) { return r.text(); }).then(function(html) {
			container.innerHTML = html;
			// Remove the embedded search input — we have our own filter above.
			var embeddedSearch = container.querySelector('#mass-model-select-search');
			if (embeddedSearch) embeddedSearch.remove();
			// Remove max-height on inner entries div — parent container handles scrolling.
			var entriesDiv = container.querySelector('#mass-model-select-entries');
			if (entriesDiv) entriesDiv.style.maxHeight = 'none';
			// Wire click handlers on model rows.
			var rows = container.querySelectorAll('[data-filename]');
			for (var i = 0; i < rows.length; i++) {
				rows[i].addEventListener('click', function(e) { e.stopPropagation(); selectRow(this); });
			}
			// Focus filter.
			var filterEl = document.getElementById('sched-load-filter');
			if (filterEl) filterEl.focus();
		}).catch(function() {
			container.innerHTML = '<p class="text-red-400 text-sm px-3 py-4">Failed to load models.</p>';
		});
	}

	// Initialize / reset the dialog fields.
	window.__schedLoadInit = function() {
		_selectedPath = '';
		_selectedType = 'chat';
		['sched-load-path','sched-load-ctx','sched-load-gpu','sched-load-threads',
		 'sched-load-concurrent','sched-load-batch','sched-load-maxtokens',
		 'sched-load-maingpu','sched-load-tensorsplit','sched-load-chat-template','sched-load-cachetype'].forEach(function(id) {
			var el = document.getElementById(id);
			if (el) el.value = '';
		});
		var deviceSel = document.getElementById('sched-load-device');
		if (deviceSel) deviceSel.value = 'gpu';
		setDevice('gpu');
		var flash = document.getElementById('sched-load-flash');
		if (flash) flash.value = '';
		var thinking = document.getElementById('sched-load-thinking');
		if (thinking) thinking.checked = false;
		var thinkWrap = document.getElementById('sched-load-thinking-wrap');
		if (thinkWrap) thinkWrap.style.display = 'none';
		var filterEl = document.getElementById('sched-load-filter');
		if (filterEl) filterEl.value = '';
		setFilterState('');
		setType('chat');
		var cfg = document.getElementById('sched-load-config');
		if (cfg) cfg.style.display = 'none';
		var errEl = document.getElementById('sched-load-error');
		if (errEl) { errEl.style.display = 'none'; errEl.textContent = ''; }
		var container = document.getElementById('sched-load-models');
		if (container) container.style.display = '';
		loadModelList();
	};

	// Debounced filter for the embedded model list.
	var filterEl = document.getElementById('sched-load-filter');
	if (filterEl) {
		var ft;
		filterEl.addEventListener('sl-input', function() {
			// If user edits the filter text, clear the selection.
			if (_selectedPath) {
				_selectedPath = '';
				document.getElementById('sched-load-path').value = '';
				var container = document.getElementById('sched-load-models');
				if (container) {
					container.querySelectorAll('.sched-load-selected').forEach(function(el) {
						el.classList.remove('sched-load-selected', 'bg-blue-900/40', 'border', 'border-blue-500/30');
					});
					container.style.display = '';
				}
				var cfg = document.getElementById('sched-load-config');
				if (cfg) cfg.style.display = 'none';
				setFilterState('');
			}
			clearTimeout(ft);
			ft = setTimeout(function() {
				applyFilter(filterEl.value.toLowerCase());
			}, 150);
		});
		filterEl.addEventListener('sl-clear', function() {
			clearSelection();
		});
		filterEl.addEventListener('sl-blur', function() {
			if (_selectedPath) {
				setFilterState('valid');
			} else if (filterEl.value) {
				setFilterState('invalid');
			} else {
				setFilterState('');
			}
		});
		filterEl.addEventListener('sl-focus', function() {
			// Reset to default while editing.
			if (!_selectedPath) setFilterState('');
		});
	}

	// Device toggle handler.
	var deviceEl = document.getElementById('sched-load-device');
	if (deviceEl) {
		deviceEl.addEventListener('sl-change', function() { setDevice(deviceEl.value); });
	}

	// Submit handler.
	var submitBtn = document.getElementById('sched-load-submit');
	if (submitBtn) {
		submitBtn.addEventListener('click', function() {
			var path = _selectedPath || (document.getElementById('sched-load-path') || {}).value || '';
			var type = _selectedType;
			var errEl = document.getElementById('sched-load-error');

			if (!path) {
				if (errEl) { errEl.textContent = 'Please select a model.'; errEl.style.display = ''; }
				return;
			}

			var intVal = function(id) { var v = parseInt((document.getElementById(id) || {}).value); return isNaN(v) ? 0 : v; };
			var strVal = function(id) { return (document.getElementById(id) || {}).value || ''; };

			var isCpu = (document.getElementById('sched-load-device') || {}).value === 'cpu';
			var body = {
				path: path,
				type: type,
				contextSize: intVal('sched-load-ctx'),
				gpuLayers: isCpu ? -1 : intVal('sched-load-gpu'),
				threads: intVal('sched-load-threads'),
				maxConcurrent: intVal('sched-load-concurrent'),
				mainGpu: isCpu ? '' : strVal('sched-load-maingpu'),
				tensorSplit: isCpu ? '' : strVal('sched-load-tensorsplit')
			};

			if (type === 'chat') {
				body.batchSize = intVal('sched-load-batch');
				body.maxTokens = intVal('sched-load-maxtokens');
				body.flashAttn = strVal('sched-load-flash');
				body.cacheType = strVal('sched-load-cachetype');
				body.chatTemplate = strVal('sched-load-chat-template');
				var thinkEl = document.getElementById('sched-load-thinking');
				body.thinking = thinkEl ? thinkEl.checked : false;
			}

			submitBtn.loading = true;
			submitBtn.disabled = true;
			if (errEl) errEl.style.display = 'none';

			fetch('/api/v1/models/load', {
				method: 'POST',
				headers: {'Content-Type': 'application/json'},
				body: JSON.stringify(body)
			}).then(function(resp) {
				if (!resp.ok) return resp.json().then(function(d) { throw new Error(d.error || 'Load failed'); });
				return resp.json();
			}).then(function() {
				submitBtn.loading = false;
				submitBtn.disabled = false;
				var dlg = document.getElementById('scheduler-load-dialog');
				if (dlg) dlg.hide();
			}).catch(function(err) {
				submitBtn.loading = false;
				submitBtn.disabled = false;
				if (errEl) { errEl.textContent = err.message; errEl.style.display = ''; }
			});
		});
	}
})();
