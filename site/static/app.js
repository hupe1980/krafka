/*
 * The site's only script. Deferred, dependency-free, ~4 KB.
 *
 * Everything here is a progressive enhancement: with JS disabled the docs are
 * fully readable and navigable, the theme still follows the OS preference, and
 * only search and the copy buttons are unavailable. That is deliberate — a
 * reference people consult while debugging a production incident should not
 * depend on a bundle loading.
 */
(function () {
  "use strict";

  // ── Theme toggle ───────────────────────────────────────────────────────
  //
  // Three states, not two. "auto" follows the OS; the explicit states exist
  // for readers whose OS preference does not match how they want to read
  // documentation. The initial value is applied by a blocking inline script in
  // <head> so there is no flash of the wrong theme.

  var root = document.documentElement;
  var KEY = "krafka-theme";
  var order = ["auto", "light", "dark"];

  function setTheme(next) {
    root.setAttribute("data-theme", next);
    // The syntax highlighting themes are two stylesheets with identical class
    // names, so they are switched by `media` rather than by a CSS variable.
    if (window.krafkaSyntaxTheme) window.krafkaSyntaxTheme(next);
    try {
      if (next === "auto") localStorage.removeItem(KEY);
      else localStorage.setItem(KEY, next);
    } catch (e) {
      /* Private browsing denies localStorage; the toggle still works for
         this page load, which is the useful part. */
    }
  }

  var toggle = document.querySelector(".theme-toggle");
  if (toggle) {
    toggle.addEventListener("click", function () {
      var current = root.getAttribute("data-theme") || "auto";
      setTheme(order[(order.indexOf(current) + 1) % order.length]);
    });
  }

  // ── Copy buttons ───────────────────────────────────────────────────────

  function attachCopy(button, getText) {
    button.addEventListener("click", function () {
      var done = function () {
        var original = button.textContent;
        button.textContent = "Copied";
        button.setAttribute("data-copied", "");
        setTimeout(function () {
          button.textContent = original;
          button.removeAttribute("data-copied");
        }, 1600);
      };
      if (navigator.clipboard) {
        navigator.clipboard.writeText(getText()).then(done, function () {});
      }
    });
  }

  document.querySelectorAll("[data-copy-target]").forEach(function (btn) {
    var target = document.getElementById(btn.getAttribute("data-copy-target"));
    if (target) attachCopy(btn, function () { return target.textContent; });
  });

  // Every code block gets one. Readers copy commands and snippets constantly,
  // and selecting a wrapped multi-line block by hand is error-prone.
  document.querySelectorAll(".prose pre").forEach(function (pre) {
    var btn = document.createElement("button");
    btn.className = "copy-btn code-copy";
    btn.type = "button";
    btn.textContent = "Copy";
    btn.setAttribute("aria-label", "Copy code to clipboard");
    pre.style.position = "relative";
    btn.style.cssText = "position:absolute;top:.5rem;right:.5rem;opacity:0;transition:opacity .12s";
    pre.appendChild(btn);
    pre.addEventListener("mouseenter", function () { btn.style.opacity = "1"; });
    pre.addEventListener("mouseleave", function () {
      if (document.activeElement !== btn) btn.style.opacity = "0";
    });
    btn.addEventListener("focus", function () { btn.style.opacity = "1"; });
    btn.addEventListener("blur", function () { btn.style.opacity = "0"; });
    attachCopy(btn, function () {
      var code = pre.querySelector("code");
      return (code || pre).textContent;
    });
  });

  // ── Wide tables ────────────────────────────────────────────────────────
  //
  // The configuration and protocol references have tables far wider than a
  // phone. Wrapping them lets the table scroll instead of the page.

  document.querySelectorAll(".prose table").forEach(function (table) {
    if (table.parentElement.classList.contains("table-scroll")) return;
    var wrap = document.createElement("div");
    wrap.className = "table-scroll";
    wrap.setAttribute("tabindex", "0");
    wrap.setAttribute("role", "region");
    wrap.setAttribute("aria-label", "Scrollable table");
    table.parentNode.insertBefore(wrap, table);
    wrap.appendChild(table);
  });

  // ── Mobile navigation ──────────────────────────────────────────────────

  var navToggle = document.querySelector(".nav-toggle");
  var sidebar = document.getElementById("docs-sidebar");
  if (navToggle && sidebar) {
    navToggle.addEventListener("click", function () {
      var open = sidebar.hasAttribute("data-open");
      if (open) sidebar.removeAttribute("data-open");
      else sidebar.setAttribute("data-open", "");
      navToggle.setAttribute("aria-expanded", String(!open));
    });
  }

  // ── Search ─────────────────────────────────────────────────────────────
  //
  // Zola builds an elasticlunr index at compile time. Rather than ship the
  // elasticlunr runtime, this does a scored substring match over the same
  // index: for a corpus of fourteen documents that is both smaller and more
  // predictable than a stemmed relevance ranking, and it works offline.
  //
  // The index is fetched on first interaction, not on page load, so a reader
  // who never searches never downloads it.

  var input = document.getElementById("docs-search-input");
  var results = document.getElementById("docs-search-results");
  if (!input || !results) return;

  var docs = null;
  var loading = false;

  function load() {
    if (docs || loading) return;
    loading = true;
    // The URL comes from Zola via a data attribute, so it is correct under any
    // `base_url` — including the `/krafka/` sub-path this site is served from,
    // where a relative fetch would resolve against the current page instead.
    fetch(input.getAttribute("data-index"))
      .then(function (r) { return r.json(); })
      .then(function (data) {
        var store = data.documentStore ? data.documentStore.docs : {};
        docs = Object.keys(store).map(function (id) {
          var d = store[id];
          return {
            id: id,
            title: d.title || "",
            body: (d.body || "").replace(/\s+/g, " "),
          };
        });
      })
      .catch(function () { docs = []; })
      .then(function () { loading = false; render(); });
  }

  function escapeHtml(s) {
    return s.replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function render() {
    var q = input.value.trim().toLowerCase();
    if (q.length < 2) {
      results.hidden = true;
      results.innerHTML = "";
      return;
    }
    if (!docs) {
      results.hidden = false;
      results.innerHTML = '<p class="r-empty">Loading index…</p>';
      return;
    }

    var hits = [];
    docs.forEach(function (d) {
      var title = d.title.toLowerCase();
      var body = d.body.toLowerCase();
      var at = body.indexOf(q);
      // A title match outranks any body match; an earlier body match outranks
      // a later one, which approximates "the page is about this" well enough
      // at this corpus size.
      var score = title.indexOf(q) !== -1 ? 1000 - title.indexOf(q) : (at !== -1 ? 100 - Math.min(at / 100, 99) : 0);
      if (score > 0) {
        var snippet = at !== -1 ? d.body.slice(Math.max(0, at - 40), at + 90) : d.body.slice(0, 120);
        hits.push({ score: score, title: d.title, id: d.id, snippet: snippet });
      }
    });

    hits.sort(function (a, b) { return b.score - a.score; });
    hits = hits.slice(0, 8);

    results.hidden = false;
    if (!hits.length) {
      results.innerHTML = '<p class="r-empty">No matches for “' + escapeHtml(input.value) + '”.</p>';
      return;
    }
    results.innerHTML = hits
      .map(function (h) {
        return (
          '<a href="' + escapeHtml(h.id) + '">' +
          '<span class="r-title">' + escapeHtml(h.title) + "</span>" +
          '<span class="r-snippet">' + escapeHtml(h.snippet) + "…</span></a>"
        );
      })
      .join("");
  }

  input.addEventListener("focus", load);
  input.addEventListener("input", render);
  input.addEventListener("keydown", function (e) {
    if (e.key === "Escape") {
      input.value = "";
      render();
      input.blur();
    }
    if (e.key === "ArrowDown") {
      var first = results.querySelector("a");
      if (first) { e.preventDefault(); first.focus(); }
    }
  });

  document.addEventListener("click", function (e) {
    if (!results.contains(e.target) && e.target !== input) results.hidden = true;
  });

  // `/` focuses search, the convention every developer-facing docs site uses.
  document.addEventListener("keydown", function (e) {
    if (e.key === "/" && !/^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement.tagName)) {
      e.preventDefault();
      input.focus();
    }
  });
})();
