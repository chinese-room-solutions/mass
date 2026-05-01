// Shared file browser logic used by both the app file picker (__massBrowse)
// and the settings directory picker (__massBrowseDir).
//
// Usage:
//   window.__massFileBrowser(opts)
//
// opts:
//   pathElId      — breadcrumb container element ID
//   entriesElId   — entries container element ID
//   ext           — file extension filter (empty string = show all)
//   dirsOnly      — true to hide files and only show directories
//   multiple      — true to allow multi-select (clicking files toggles them
//                   without unselecting siblings); onSelect is called with an
//                   array of paths on every change
//   onSelect(path | paths) — called when selection changes. With multiple=true
//                            receives the full array of currently-selected paths;
//                            otherwise receives the single picked path.
//   onNavigate(dir) — optional, called when the current directory changes
window.__massFileBrowser = function(opts) {
	var pathElId = opts.pathElId;
	var entriesElId = opts.entriesElId;
	var ext = opts.ext || '';
	var dirsOnly = opts.dirsOnly || false;
	var multiple = opts.multiple || false;
	var onSelect = opts.onSelect || function() {};
	var onNavigate = opts.onNavigate || function() {};
	// Multi-select state lives on the browser instance so navigation
	// across folders preserves selections — pre-refactor MASS only had
	// single-select so this is fresh ground.
	var selected = {}; // path -> true

	function esc(s) {
		var d = document.createElement('div');
		d.textContent = s;
		return d.innerHTML;
	}

	function renderBreadcrumb(dir) {
		var pathEl = document.getElementById(pathElId);
		pathEl.innerHTML = '';
		var drivesBtn = document.createElement('button');
		drivesBtn.className = 'px-2 py-0.5 rounded text-xs font-medium mass-badge flex-shrink-0';
		drivesBtn.textContent = 'Drives';
		drivesBtn.onclick = function() { loadRoots(); };
		pathEl.appendChild(drivesBtn);
		if (!dir) return;
		var sep = dir.indexOf('/') >= 0 ? '/' : '\\';
		var parts = dir.replace(/[/\\]+$/, '').split(/[/\\]/);
		var accumulated = '';
		parts.forEach(function(part, i) {
			if (part === '') { accumulated = sep; return; }
			accumulated = i === 0 ? part + sep : accumulated + part + sep;
			var seg = accumulated;
			var span = document.createElement('span');
			span.className = 'text-neutral-500 mx-1';
			span.textContent = '/';
			pathEl.appendChild(span);
			var btn = document.createElement('button');
			btn.className = 'px-1.5 py-0.5 rounded text-xs hover:bg-neutral-700/40 text-neutral-300 truncate max-w-[140px]';
			btn.title = seg;
			btn.textContent = part || sep;
			var target = seg;
			if (/^[A-Za-z]:[/\\]$/.test(seg)) { target = seg; }
			else { target = seg.replace(/[/\\]$/, '') || sep; }
			btn.onclick = (function(t) { return function() { loadDir(t); }; })(target);
			pathEl.appendChild(btn);
		});
	}

	function renderEntries(entries) {
		var container = document.getElementById(entriesElId);
		container.innerHTML = '';
		var hasParent = entries.some(function(e) { return e.name === '..'; });
		if (!hasParent) {
			entries = [{name: '..', path: '__roots__', is_dir: true}].concat(entries);
		}
		entries.forEach(function(e) {
			if (dirsOnly && !e.is_dir) return;
			var row = document.createElement('div');
			row.className = 'flex items-center gap-2 px-2 py-1.5 rounded cursor-pointer mass-row-hover text-sm';
			var icon = e.is_dir ? 'folder' : 'file-earmark';
			var nameSpan = e.name === '..' ? '<span class="text-neutral-400">..</span>' :
				'<span class="truncate">' + esc(e.name) + '</span>';
			row.innerHTML = '<sl-icon name="' + icon + '" class="text-neutral-400 flex-shrink-0"></sl-icon>' + nameSpan;
			if (multiple && !e.is_dir && selected[e.path]) {
				row.classList.add('mass-fb-selected');
			}
			row.onclick = function() {
				if (e.is_dir) {
					if (e.path === '__roots__') { loadRoots(); }
					else { loadDir(e.path); }
					return;
				}
				if (multiple) {
					if (selected[e.path]) {
						delete selected[e.path];
						row.classList.remove('mass-fb-selected');
					} else {
						selected[e.path] = true;
						row.classList.add('mass-fb-selected');
					}
					onSelect(Object.keys(selected));
					return;
				}
				container.querySelectorAll('.mass-fb-selected').forEach(function(el) {
					el.classList.remove('mass-fb-selected');
				});
				row.classList.add('mass-fb-selected');
				onSelect(e.path);
			};
			container.appendChild(row);
		});
	}

	function loadRoots() {
		fetch('/api/v1/browse/roots').then(function(r) { return r.json(); }).then(function(roots) {
			onNavigate('');
			renderBreadcrumb('');
			var container = document.getElementById(entriesElId);
			container.innerHTML = '';
			roots.forEach(function(r) {
				var row = document.createElement('div');
				row.className = 'flex items-center gap-2 px-2 py-1.5 rounded cursor-pointer mass-row-hover text-sm';
				row.innerHTML = '<sl-icon name="device-hdd" class="text-neutral-400 flex-shrink-0"></sl-icon><span>' + esc(r.name) + '</span>';
				row.onclick = function() { loadDir(r.path); };
				container.appendChild(row);
			});
		}).catch(function(err) {
			document.getElementById(entriesElId).innerHTML = '<p class="text-red-400 text-sm">Error: ' + err.message + '</p>';
		});
	}

	function loadDir(dir) {
		var url = '/api/v1/browse?dir=' + encodeURIComponent(dir) + '&ext=' + encodeURIComponent(ext);
		fetch(url).then(function(r) { return r.json(); }).then(function(data) {
			if (!Array.isArray(data)) {
				var msg = (data && data.error) ? data.error : 'Path does not exist or is not accessible.';
				renderBreadcrumb(dir);
				document.getElementById(entriesElId).innerHTML =
					'<p class="text-red-400 text-sm py-2 px-1">' + esc(msg) + '</p>';
				onNavigate(dir);
				return;
			}
			var resolved = dir;
			if (!resolved && data.length > 0) {
				var first = data[0].name === '..' ? data[1] : data[0];
				if (first) {
					var idx = Math.max(first.path.lastIndexOf('/'), first.path.lastIndexOf('\\'));
					if (idx > 0) resolved = first.path.substring(0, idx);
				}
			}
			onNavigate(resolved);
			renderBreadcrumb(resolved);
			renderEntries(data);
		}).catch(function(err) {
			document.getElementById(entriesElId).innerHTML = '<p class="text-red-400 text-sm py-2 px-1">Error: ' + err.message + '</p>';
		});
	}

	loadDir('');
};
