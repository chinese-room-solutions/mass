// MASS landing page behaviors: the SDK-style theme switcher and live release
// data. Static-host friendly — everything fails soft when the GitHub API is
// unreachable (offline, rate-limited, or the repos are still private).
(function () {
  "use strict";

  // --- Themes -----------------------------------------------------------------
  // Mirrors mass-sdk/uikit/theme.js, minus the light base: this page is
  // dark-only, so a theme is the Shoelace dark class plus an optional overlay
  // class for pluggable themes (synthwave layers on dark).
  var THEMES = {
    dark: {},
    synthwave: { overlay: "sl-theme-synthwave" },
  };

  function applyTheme(name) {
    // Unknown or retired name (a returning visitor's stored "light") falls back
    // to dark, and is normalized so it does not get written back to storage.
    if (!THEMES[name]) name = "dark";
    var info = THEMES[name];
    var h = document.documentElement;
    Array.prototype.slice.call(h.classList).forEach(function (c) {
      if (c.indexOf("sl-theme-") === 0) h.classList.remove(c);
    });
    h.classList.add("sl-theme-dark");
    if (info.overlay) h.classList.add(info.overlay);
    // Let canvas painters (the starfield) re-read the theme's colors.
    document.dispatchEvent(new CustomEvent("mass-theme"));
    document.querySelectorAll("[data-theme-pick]").forEach(function (b) {
      b.setAttribute("aria-pressed", String(b.getAttribute("data-theme-pick") === name));
    });
    try { localStorage.setItem("mass-site-theme", name); } catch (e) { /* private mode */ }
  }

  function initTheme() {
    var fromQuery = new URLSearchParams(location.search).get("theme");
    var saved = null;
    try { saved = localStorage.getItem("mass-site-theme"); } catch (e) { /* private mode */ }
    applyTheme(fromQuery || saved || "dark");
    document.querySelectorAll("[data-theme-pick]").forEach(function (b) {
      b.addEventListener("click", function () { applyTheme(b.getAttribute("data-theme-pick")); });
    });
  }

  // --- Deep-space starfield -----------------------------------------------------
  // Fixed full-viewport canvas behind the page: three parallax depth layers of
  // slowly drifting stars, a few tinted with the theme accent. Density scales
  // with the viewport (~350 stars at 1080p); DPR capped at 2. Under
  // prefers-reduced-motion it draws one static frame; rAF pauses in background
  // tabs on its own.
  function initStarfield() {
    var canvas = document.querySelector("[data-starfield]");
    if (!canvas) return;
    var ctx = canvas.getContext("2d");
    var reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    // layer: [stars per 10000 px², radius, drift px/frame]; depth 0..1 drives
    // parallax shift and brightness.
    var LAYERS = [[3.5, 1.0, 0.008], [2.0, 1.5, 0.018], [0.85, 2.2, 0.034]];
    var PARALLAX = 14, TWINKLE = 0.12;
    var dpr = Math.min(devicePixelRatio || 1, 2);
    var stars = [], W = 0, H = 0, px = 0, py = 0, accent = "", t = 0;
    var base = "235, 235, 245"; // star white, constant across the dark themes

    function palette() {
      accent = getComputedStyle(document.documentElement)
        .getPropertyValue("--mass-accent").trim() || "#888";
    }

    function resize() {
      W = innerWidth; H = innerHeight;
      canvas.width = W * dpr; canvas.height = H * dpr;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      stars = [];
      LAYERS.forEach(function (l, li) {
        var depth = (li + 1) / LAYERS.length;
        var n = Math.round((W * H / 10000) * l[0]);
        for (var i = 0; i < n; i++) {
          stars.push({
            x: Math.random() * W, y: Math.random() * H,
            r: l[1] * (0.6 + Math.random() * 0.7), v: l[2], d: depth,
            tw: Math.random() < TWINKLE ? 1.2 + Math.random() * 2 : 0,
            ph: Math.random() * 6.28, accent: Math.random() < 0.16,
          });
        }
      });
      if (reduced) frame();
    }

    function frame() {
      t += 1;
      ctx.clearRect(0, 0, W, H);
      for (var i = 0; i < stars.length; i++) {
        var s = stars[i];
        s.x -= s.v * s.d; // slow leftward drift; far layers drift slower
        if (s.x < -2) { s.x = W + 2; s.y = Math.random() * H; }
        var alpha = 0.25 + s.d * 0.55;
        if (s.tw) alpha *= 0.55 + 0.45 * Math.sin(t * 0.05 * s.tw + s.ph);
        if (s.accent) {
          ctx.globalAlpha = alpha; ctx.fillStyle = accent;
        } else {
          ctx.globalAlpha = 1;
          ctx.fillStyle = "rgba(" + base + "," + alpha.toFixed(3) + ")";
        }
        ctx.beginPath();
        ctx.arc(s.x + px * PARALLAX * s.d, s.y + py * PARALLAX * s.d, s.r, 0, 6.28);
        ctx.fill();
      }
      ctx.globalAlpha = 1;
    }

    palette();
    resize();
    addEventListener("resize", resize);
    document.addEventListener("mass-theme", function () { palette(); if (reduced) frame(); });
    if (!reduced) {
      addEventListener("pointermove", function (e) {
        px = e.clientX / W - 0.5;
        py = e.clientY / H - 0.5;
      });
      (function loop() { frame(); requestAnimationFrame(loop); })();
    }
  }

  // --- Fleet sector map ---------------------------------------------------------
  // Builds the #how constellation: the hub (a relay — glowing core inside two
  // counter-rotating arcs) linked to worker nodes, with job pulses riding the
  // links on CSS motion paths (styling + animation live in site.css).
  function initFleet() {
    var svg = document.querySelector("[data-fleet]");
    if (!svg) return;
    var NS = "http://www.w3.org/2000/svg";
    var HX = 360, HY = 150;
    // Each worker is annotated with the runtime it represents — llama.cpp is
    // shipped, the rest sketch the multi-runtime roadmap (text LLMs, diffusion
    // LLMs, media gen, speech, raw Python inferencers).
    var WORKERS = [
      [85, 70, "llama-cpp"], [150, 235, "diffusion"], [295, 45, "vllm"],
      [555, 60, "dllm"], [645, 205, "tts"], [425, 250, "python"],
      [195, 165, "colibri"],
    ];

    function el(name, attrs) {
      var e = document.createElementNS(NS, name);
      for (var k in attrs) e.setAttribute(k, attrs[k]);
      svg.appendChild(e);
      return e;
    }

    WORKERS.forEach(function (w, i) {
      var d = "M" + HX + " " + HY + " L" + w[0] + " " + w[1];
      el("path", { d: d, "class": "link" });
      var p = el("circle", { r: 2.4, "class": "pulse" });
      p.style.setProperty("--p", 'path("' + d + '")');
      p.style.setProperty("--d", (i * 0.55) + "s");
      el("circle", { cx: w[0], cy: w[1], r: 3, "class": "node" });
      el("circle", { cx: w[0], cy: w[1], r: 7, "class": "node-ring", "stroke-dasharray": "2 3" });
      el("text", { x: w[0] + 11, y: w[1] + 3 }).textContent = w[2];
    });

    el("circle", { cx: HX, cy: HY, r: 14, "class": "hub-glow" });
    el("circle", { cx: HX, cy: HY, r: 5, "class": "hub-core" });
    [el("path", { "class": "arc a1", "stroke-width": 1.6, d: arc(HX, HY, 13, -40, 140) }),
     el("path", { "class": "arc a2", "stroke-width": 1.2, d: arc(HX, HY, 19, 90, 240) })]
      .forEach(function (a) {
        a.style.setProperty("--cx", HX + "px");
        a.style.setProperty("--cy", HY + "px");
      });
    el("text", { x: HX - 16, y: HY + 34 }).textContent = "MASS";

    function arc(cx, cy, r, a0, a1) {
      function pt(a) {
        var rad = (a - 90) * Math.PI / 180;
        return [cx + r * Math.cos(rad), cy + r * Math.sin(rad)];
      }
      var s = pt(a0), e = pt(a1), large = a1 - a0 > 180 ? 1 : 0;
      return "M" + s[0] + " " + s[1] + " A" + r + " " + r + " 0 " + large + " 1 " + e[0] + " " + e[1];
    }
  }

  // --- Brand diffusion cycle ----------------------------------------------------
  // The hero mark rolls MASS → ∈ Δ § → 3455 with a per-character diffusion:
  // during a transition every character churns through the target variant's
  // glyph pool and resolves left-to-right. Between cycles it throws brief
  // glitch flickers. Static under prefers-reduced-motion.
  function initBrandCycle() {
    var el = document.querySelector("[data-brand-cycle]");
    if (!el) return;
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;

    // Each variant scrambles in its own alphabet: letters condense into MASS,
    // math notation into ∈ Δ §, digits into 3455.
    var VARIANTS = [
      { text: "MASS", pool: "ABCDEFGHIJKLMNOPQRSTUVWXYZ" },
      { text: "∈ Δ §", pool: "∈∉∋∀∃∅ΔΛ∇§¶∑∏∫≡≈≠ΩΨΦΞλπμ" },
      { text: "3455", pool: "0123456789" },
    ];
    var TICK = 45;        // ms per scramble frame
    var RESOLVE = 220;    // ms between successive characters locking in
    var DWELL = 3400;     // ms a variant stays before diffusing to the next
    var idx = 0;

    function glyph(pool) {
      return pool[(Math.random() * pool.length) | 0];
    }

    function scrambled(pool, n) {
      var s = "";
      for (var i = 0; i < n; i++) s += glyph(pool);
      return s;
    }

    // Two phases so different-length variants never snap: first the churning
    // mark grows/shrinks one cell at a time to the target length, then the
    // cells resolve left-to-right into the target.
    function diffuseTo(next) {
      var len = el.textContent.length;
      var tick = 0;
      var morph = setInterval(function () {
        if (len === next.text.length) { clearInterval(morph); resolve(); return; }
        if (++tick % 2 === 0) len += len < next.text.length ? 1 : -1;
        el.textContent = scrambled(next.pool, len);
      }, TICK);
      function resolve() {
        var start = Date.now();
        var timer = setInterval(function () {
          var t = Date.now() - start, out = "", done = true;
          for (var i = 0; i < next.text.length; i++) {
            if (t > i * RESOLVE + RESOLVE) {
              out += next.text[i];
            } else {
              done = false;
              out += glyph(next.pool);
            }
          }
          el.textContent = out;
          if (done) { el.textContent = next.text; clearInterval(timer); }
        }, TICK);
      }
    }

    setInterval(function () {
      idx = (idx + 1) % VARIANTS.length;
      diffuseTo(VARIANTS[idx]);
    }, DWELL);

    (function flicker() {
      setTimeout(function () {
        el.classList.add("flicker");
        setTimeout(function () { el.classList.remove("flicker"); flicker(); }, 60 + Math.random() * 140);
      }, 1200 + Math.random() * 2600);
    })();
  }

  // --- Live release data ------------------------------------------------------
  // Show the current version on the hero button, and light up any download that
  // exists in the latest release ("coming soon" rows flip to links the moment
  // the asset is uploaded — no site change needed).
  function wireReleases() {
    var repos = {};
    document.querySelectorAll("[data-asset], [data-asset-cell]").forEach(function (el) {
      var key = el.getAttribute("data-asset") || el.getAttribute("data-asset-cell");
      repos[key.split("/")[0]] = true;
    });
    Object.keys(repos).forEach(function (repo) {
      fetch("https://api.github.com/repos/chinese-room-solutions/" + repo + "/releases/latest", {
        headers: { Accept: "application/vnd.github+json" },
      })
        .then(function (r) { return r.ok ? r.json() : null; })
        .then(function (rel) {
          if (!rel) return;
          var have = {};
          (rel.assets || []).forEach(function (a) { have[a.name] = true; });
          if (repo === "mass") {
            document.querySelectorAll("[data-version]").forEach(function (el) {
              el.textContent = rel.tag_name.replace(/^v/, "");
            });
          }
          // Links whose asset is missing from the latest release degrade to
          // "coming soon"; placeholder cells with a present asset become links.
          document.querySelectorAll('[data-asset^="' + repo + '/"]').forEach(function (a) {
            if (!have[a.getAttribute("data-asset").split("/")[1]]) {
              var s = document.createElement("span");
              s.className = "soon";
              s.textContent = "coming soon";
              a.replaceWith(s);
            }
          });
          document.querySelectorAll('[data-asset-cell^="' + repo + '/"]').forEach(function (td) {
            var name = td.getAttribute("data-asset-cell").split("/")[1];
            if (have[name]) {
              var a = document.createElement("a");
              a.href = td.getAttribute("data-href");
              a.textContent = name;
              td.classList.remove("soon");
              td.replaceChildren(a);
            }
          });
        })
        .catch(function () { /* offline / rate-limited / private — keep defaults */ });
    });
  }

  // --- Click-to-copy quickstart commands ---------------------------------------

  // popCopied floats a "copied" label up from the pointer and lets it vaporize.
  // The inline tick can be scrolled out of a long command's view; this can't.
  // Anchored to the element's left edge when the click carries no coordinates
  // (keyboard activation reports 0,0).
  function popCopied(ev, el) {
    var x = ev.clientX, y = ev.clientY;
    if (!x && !y) {
      var r = el.getBoundingClientRect();
      x = r.left + Math.min(r.width, 90) / 2;
      y = r.top;
    }
    var pop = document.createElement("span");
    pop.className = "copy-pop";
    pop.textContent = "copied";
    pop.style.left = x + "px";
    pop.style.top = y - 14 + "px";
    document.body.appendChild(pop);
    pop.addEventListener("animationend", function () { pop.remove(); });
  }

  function initCopy() {
    document.querySelectorAll("pre .cmd").forEach(function (el) {
      el.title = "Click to copy";
      el.addEventListener("click", function (ev) {
        var text = el.textContent;
        // Race a short timeout: writeText can hang without ever settling when
        // the browser withholds clipboard permission.
        var write = navigator.clipboard
          ? Promise.race([
              navigator.clipboard.writeText(text),
              new Promise(function (_, reject) { setTimeout(reject, 250); }),
            ])
          : Promise.reject();
        write
          .catch(function () {
            // http:// preview or denied permission — legacy fallback.
            var ta = document.createElement("textarea");
            ta.value = text;
            document.body.appendChild(ta);
            ta.select();
            document.execCommand("copy");
            ta.remove();
          })
          .then(function () {
            el.classList.add("copied");
            setTimeout(function () { el.classList.remove("copied"); }, 1300);
            popCopied(ev, el);
          });
      });
    });
  }

  function initPage() { initStarfield(); initFleet(); initBrandCycle(); wireReleases(); initCopy(); }
  initTheme();
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initPage);
  } else {
    initPage();
  }
})();
