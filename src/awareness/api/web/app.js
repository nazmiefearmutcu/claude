// awareness — research workbench
// Vanilla ES module. Router + 5 views + command palette + reader + live feed
// + keyboard navigation + a11y. All dynamic DOM via createElement+textContent
// so server values never reach an HTML parser.

const $ = (q, root = document) => root.querySelector(q);
const $$ = (q, root = document) => Array.from(root.querySelectorAll(q));

// ── DOM builder ───────────────────────────────────────────────
function el(tag, props, ...children) {
  const node = document.createElement(tag);
  if (props) {
    for (const [k, v] of Object.entries(props)) {
      if (v == null || v === false) continue;
      if (k === "class") node.className = v;
      else if (k === "text") node.textContent = v;
      else if (k === "html") {} // intentionally unsupported
      else if (k === "dataset") for (const [dk, dv] of Object.entries(v)) node.dataset[dk] = dv;
      else if (k === "style") Object.assign(node.style, v);
      else if (k.startsWith("on") && typeof v === "function") node.addEventListener(k.slice(2), v);
      else node.setAttribute(k, v === true ? "" : String(v));
    }
  }
  for (const c of children) {
    if (c == null || c === false) continue;
    node.appendChild(typeof c === "string" ? document.createTextNode(c) : c);
  }
  return node;
}
function clear(node) { while (node.firstChild) node.removeChild(node.firstChild); }

// ── Helpers ───────────────────────────────────────────────────
const fmt = (n) => (n == null ? "—" : new Intl.NumberFormat("en-US").format(n));
const ago = (iso, short = true) => {
  if (!iso) return "—";
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return String(iso);
  const d = Math.max(0, (Date.now() - t) / 1000);
  if (d < 60) return short ? Math.max(1, Math.round(d)) + "s" : Math.max(1, Math.round(d)) + "s ago";
  if (d < 3600) return short ? Math.round(d / 60) + "m" : Math.round(d / 60) + "m ago";
  if (d < 86400) return short ? Math.round(d / 3600) + "h" : Math.round(d / 3600) + "h ago";
  return short ? Math.round(d / 86400) + "d" : Math.round(d / 86400) + "d ago";
};
const isoDay = (d) => new Date(d).toISOString().slice(0, 10);

// ── Toast ─────────────────────────────────────────────────────
let toastTimer;
function toast(msg, kind = "ok") {
  const t = $("#toast");
  if (!t) return;
  t.textContent = msg;
  t.className = "toast show " + kind;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => (t.className = "toast"), 3400);
}

// ── API ───────────────────────────────────────────────────────
// Connection health: if N consecutive fetches fail with a network error
// (server down, CORS, DNS, etc.) show a persistent offline banner; clear
// it as soon as one fetch succeeds.
const apiHealth = { failures: 0, threshold: 2, offline: false };

function setOffline(offline, reason) {
  if (offline === apiHealth.offline) return;
  apiHealth.offline = offline;
  const banner = $("#api-offline");
  if (!banner) return;
  if (offline) {
    banner.hidden = false;
    const msg = banner.querySelector(".api-offline-msg");
    if (msg) msg.textContent = "API unreachable — start it with `awareness-api`, or check the port.";
    const why = banner.querySelector(".api-offline-why");
    if (why) why.textContent = reason || "";
  } else {
    banner.hidden = true;
  }
}

async function api(path, opts = {}) {
  let res;
  try {
    res = await fetch(path, {
      headers: { "Content-Type": "application/json", ...(opts.headers || {}) },
      ...opts,
    });
  } catch (netErr) {
    // Network-level failure: server down, CORS, DNS. fetch() rejects with
    // a TypeError "Failed to fetch" — not an HTTP status we can read.
    apiHealth.failures += 1;
    if (apiHealth.failures >= apiHealth.threshold) {
      setOffline(true, netErr.message || "network error");
    }
    throw new Error("API unreachable: " + (netErr.message || "network error"));
  }
  if (!res.ok) {
    // We DID reach the server; not an "offline" condition.
    apiHealth.failures = 0;
    setOffline(false);
    let detail = res.statusText;
    try { detail = (await res.json()).detail || detail; } catch (_) {}
    throw new Error(res.status + " " + detail);
  }
  // Healthy response — clear any offline state.
  apiHealth.failures = 0;
  setOffline(false);
  if (res.status === 204) return null;
  return await res.json();
}

// ── KPI animation (count-up) ──────────────────────────────────
const kpiState = new Map();
function setKPI(id, target, opts = {}) {
  const node = $("#" + id);
  if (!node) return;
  const prev = kpiState.get(id) ?? 0;
  if (target === prev) {
    node.textContent = fmt(target);
    node.classList.toggle("is-zero", target === 0);
    return;
  }
  const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  if (reduce) {
    node.textContent = fmt(target);
    kpiState.set(id, target);
    node.classList.toggle("is-zero", target === 0);
    return;
  }
  const start = performance.now();
  const dur = 700;
  function frame(now) {
    const k = Math.min(1, (now - start) / dur);
    const eased = 1 - Math.pow(1 - k, 3);
    const v = Math.round(prev + (target - prev) * eased);
    node.textContent = fmt(v);
    if (k < 1) requestAnimationFrame(frame);
    else { kpiState.set(id, target); node.classList.toggle("is-zero", target === 0); }
  }
  requestAnimationFrame(frame);
}

// ── Router ────────────────────────────────────────────────────
const ROUTES = ["dashboard", "captures", "jobs", "tail", "settings"];
let currentRoute = "dashboard";
function navigate(route, { push = true } = {}) {
  if (!ROUTES.includes(route)) route = "dashboard";
  currentRoute = route;
  $$(".view").forEach((v) => {
    const match = v.dataset.view === route;
    v.toggleAttribute("hidden", !match);
    v.classList.toggle("is-active", match);
  });
  $$(".nav-item").forEach((n) => {
    const match = n.dataset.route === route;
    if (match) n.setAttribute("aria-current", "page");
    else n.removeAttribute("aria-current");
  });
  // Hide rail except on dashboard.
  $(".app").classList.toggle("no-rail", route !== "dashboard");

  if (push) history.pushState({ route }, "", "#" + route);
  // Move keyboard focus to main heading for screen readers.
  setTimeout(() => $("#main").focus({ preventScroll: false }), 50);

  // Lazy-load views' data on activation.
  if (route === "captures") void loadCaptures(true);
  if (route === "jobs") void loadJobs();
  if (route === "tail") startTailPolling();
  if (route === "settings") void loadSettings();
}
window.addEventListener("popstate", (e) => {
  const r = (location.hash || "#dashboard").slice(1);
  navigate(r, { push: false });
});

// ── Header / Dashboard refresh ────────────────────────────────
let lastFeedCaptureId = null;
async function refreshDashboard() {
  let status, dedup;
  try {
    [status, dedup] = await Promise.all([api("/status"), api("/dedup-stats")]);
  } catch (e) { console.error(e); return; }

  const tail = status.tail || {};
  const jobsTotal = (status.jobs || []).length;
  const docsTotal = (status.jobs || []).reduce((a, j) => a + (j.docs_emitted || 0), 0);

  setKPI("kpi-captures", dedup.total_captures_seen || 0);
  setKPI("kpi-distinct", dedup.distinct_content_hashes || 0);
  setKPI("kpi-folds", Math.max(0, (dedup.total_captures_seen || 0) - (dedup.distinct_content_hashes || 0)));
  setKPI("kpi-jobs", jobsTotal);

  $("#kpi-captures-sub").textContent = (docsTotal ? `${fmt(docsTotal)} emitted across jobs` : "across the corpus");
  $("#kpi-distinct-sub").textContent = "unique content";
  $("#kpi-folds-sub").textContent = `${fmt(dedup.near_dup_index_rows || 0)} simhash rows`;
  $("#kpi-jobs-sub").textContent = "backfill & tail runs";

  // Tail strip (dashboard)
  const strip = $("#tail-strip");
  const pulse = strip.querySelector(".tail-pulse");
  pulse.dataset.state = tail.running ? "on" : "off";
  $("#tail-strip-state").textContent = tail.running ? "running" : "stopped";
  $("#tail-strip-detail").textContent = tail.running
    ? `Reading public feeds. Job ${tail.job_id || "—"}.`
    : (tail.stopped_at ? `Last stopped ${ago(tail.stopped_at, false)}.` : "No live capture running.");
  $("#tail-strip-meta").textContent = tail.running ? `started ${ago(tail.started_at, false)}` : "";

  // Sidebar tail card
  $("#sidebar-tail .tail-led").dataset.state = tail.running ? "on" : "off";
  $("#sidebar-tail-status").textContent = tail.running ? "running" : "stopped";
  $("#sidebar-tail-meta").textContent = tail.running
    ? `since ${ago(tail.started_at, true)}`
    : (tail.stopped_at ? `since ${ago(tail.stopped_at, true)} ago` : "—");

  // Recent jobs strip
  renderJobStrip($("#jobs-strip"), (status.jobs || []).slice(0, 4));

  return { status, dedup };
}

function renderJobStrip(root, jobs) {
  clear(root);
  if (!jobs.length) {
    root.appendChild(el("p", { class: "muted", style: { padding: "22px 24px" } }, "No jobs yet."));
    return;
  }
  for (const j of jobs) {
    const pct = j.tasks_total ? Math.round(100 * j.tasks_completed / j.tasks_total) : 0;
    const idCell = el("div", { class: "job-id" });
    idCell.appendChild(document.createTextNode(j.job_id));
    idCell.appendChild(el("span", { class: "kind", text: j.kind }));

    const progress = el("div", { class: "job-progress", "aria-label": `progress ${pct}%`, role: "progressbar", "aria-valuenow": pct, "aria-valuemin": 0, "aria-valuemax": 100 });
    progress.appendChild(el("div", { class: "job-progress-bar", style: { width: pct + "%" } }));

    const counters = el("div", { class: "job-counters" });
    counters.appendChild(document.createTextNode(`${j.tasks_completed}/${j.tasks_total} tasks · `));
    counters.appendChild(el("b", { text: fmt(j.docs_emitted) }));
    counters.appendChild(document.createTextNode(" docs · "));
    counters.appendChild(el("b", { text: fmt(j.docs_dedup_dropped) }));
    counters.appendChild(document.createTextNode(" folded"));

    const badge = el("span", { class: "badge badge-" + j.status, text: j.status });
    const row = el("div", { class: "job-row" }, idCell, progress, counters, badge);
    root.appendChild(row);
  }
}

// ── Captures view ─────────────────────────────────────────────
const caps = { limit: 30, offset: 0, total: 0 };
let capsSearchTimer = null;

async function loadCaptures(reset = false) {
  if (reset) caps.offset = 0;
  const q = $("#caps-search").value.trim();
  const source = $("#caps-source").value;
  const domain = $("#caps-domain").value.trim();
  const start = $("#caps-start").value;
  const end = $("#caps-end").value;

  const list = $("#caps-list");
  const meta = $("#caps-meta");
  meta.textContent = "loading…";

  // Search-mode hits /search (BM25 ranked, with snippets); browse-mode hits
  // /captures (chronological).
  const params = new URLSearchParams();
  params.set("limit", caps.limit);
  params.set("offset", caps.offset);
  if (source) params.set("source", source);
  if (domain) params.set("domain", domain);
  if (start) params.set("start", start);
  if (end) params.set("end", end);

  const isSearch = !!q;
  let url;
  if (isSearch) {
    params.set("q", q);
    url = "/search?" + params.toString();
  } else {
    url = "/captures?" + params.toString();
  }

  try {
    const data = await api(url);
    caps.total = data.total;
    renderCaps(list, data.rows || [], { search: q, ranked: !!data.ranked });
    const from = data.total ? caps.offset + 1 : 0;
    const to = Math.min(caps.offset + (data.rows || []).length, data.total);
    if (isSearch) {
      const mode = data.ranked ? "BM25-ranked" : "fallback substring";
      meta.textContent = `${from}–${to} of ${fmt(data.total)} matches · ${mode}`;
    } else {
      meta.textContent = `${from}–${to} of ${fmt(data.total)} captures · chronological`;
    }
    $("#caps-pos").textContent = data.total ? `${from}–${to} of ${fmt(data.total)}` : "—";
    $("#caps-prev").disabled = caps.offset <= 0;
    $("#caps-next").disabled = caps.offset + caps.limit >= data.total;
  } catch (err) {
    console.error(err);
    meta.textContent = "query failed: " + err.message;
  }
}

// Build DOM fragment with <mark> tags around every match of any term, case
// insensitive, word-boundary. Uses matchAll (no innerHTML on values).
function highlightedFragment(text, terms) {
  const frag = document.createDocumentFragment();
  if (!text) return frag;
  if (!terms || terms.length === 0) {
    frag.appendChild(document.createTextNode(text));
    return frag;
  }
  const pattern = new RegExp(
    "\\b(" + terms.map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|") + ")\\b",
    "ig"
  );
  let lastIndex = 0;
  for (const m of text.matchAll(pattern)) {
    if (m.index > lastIndex) frag.appendChild(document.createTextNode(text.slice(lastIndex, m.index)));
    frag.appendChild(el("mark", { class: "hl" }, m[0]));
    lastIndex = m.index + m[0].length;
  }
  if (lastIndex < text.length) frag.appendChild(document.createTextNode(text.slice(lastIndex)));
  return frag;
}

function renderCaps(root, rows, { search = "", ranked = false } = {}) {
  clear(root);
  for (const r of rows) {
    const li = el("li", {
      class: "cap-row" + (search ? " has-snippet" : ""),
      tabindex: "0",
      role: "button",
      "aria-label": `${r.title || "untitled"} — ${r.source_type}`,
      dataset: { cid: r.capture_id },
      onkeydown: (ev) => { if (ev.key === "Enter") openReader(r.capture_id); },
      onclick: () => openReader(r.capture_id),
    });

    // Left column: score chip in ranked search mode, otherwise time.
    if (search && ranked && typeof r.score === "number") {
      li.appendChild(el("div", { class: "cap-score", title: "BM25 score" }, r.score.toFixed(2)));
    } else {
      li.appendChild(el("div", { class: "cap-time", title: String(r.fetch_ts || "") }, ago(r.fetch_ts, true)));
    }

    // Main: title + (snippet) + meta.
    const main = el("div", { class: "cap-main" });
    const titleNode = el("h3", { class: "cap-title" });
    if (search && r.terms && r.terms.length) {
      titleNode.appendChild(highlightedFragment(r.title || "(untitled)", r.terms));
    } else {
      titleNode.textContent = r.title || "(untitled)";
    }
    main.appendChild(titleNode);

    if (search && r.snippet) {
      const snip = el("p", { class: "cap-snippet" });
      snip.appendChild(highlightedFragment(r.snippet, r.terms || []));
      main.appendChild(snip);
    }

    const m = el("div", { class: "cap-meta" });
    m.appendChild(el("span", { class: "domain", text: r.domain || "—" }));
    if (r.url) m.appendChild(el("span", { class: "url", text: r.url }));
    if (search) m.appendChild(el("span", { class: "when", text: ago(r.fetch_ts, true) + " ago" }));
    main.appendChild(m);
    li.appendChild(main);

    li.appendChild(el("div", { class: "cap-source", text: r.source_type }));
    li.appendChild(el("div", { class: "cap-size", text: fmt(r.text_len) + " ch" }));
    root.appendChild(li);
  }
}

// ── Reader drawer ─────────────────────────────────────────────
let readerLastFocus = null;
async function openReader(cid) {
  const reader = $("#reader");
  const scrim = $("#reader-scrim");
  const body = $("#reader-body");
  readerLastFocus = document.activeElement;
  scrim.hidden = false;
  reader.setAttribute("aria-hidden", "false");

  clear(body);
  body.appendChild(el("p", { class: "muted", text: "loading…" }));
  let d;
  try { d = await api("/captures/" + encodeURIComponent(cid)); }
  catch (err) {
    clear(body);
    body.appendChild(el("p", { class: "muted", text: "failed: " + err.message }));
    return;
  }

  clear(body);
  body.appendChild(el("div", { class: "reader-eyebrow-source", text: (d.source_type || "") + " · " + (d.source_name || "") }));
  body.appendChild(el("h1", { class: "reader-title", text: d.title || "(untitled)" }));

  const byline = el("div", { class: "reader-byline" });
  function bk(label, value, opts = {}) {
    if (value == null || value === "") return;
    const span = el("span");
    span.appendChild(el("span", { class: "b-key", text: label }));
    if (opts.link) {
      const a = el("a", { href: value, target: "_blank", rel: "noopener" });
      a.textContent = value;
      span.appendChild(a);
    } else {
      span.appendChild(document.createTextNode(String(value)));
    }
    byline.appendChild(span);
  }
  bk("domain", d.domain);
  bk("fetched", new Date(d.fetch_ts).toLocaleString());
  if (d.published_ts) bk("published", new Date(d.published_ts).toLocaleString());
  if (d.language) bk("lang", d.language);
  if (d.url) bk("source", d.url, { link: true });
  body.appendChild(byline);

  body.appendChild(el("div", { class: "reader-text", text: d.text || "(empty)" }));

  const meta = el("div", { class: "reader-meta" });
  meta.appendChild(el("div", { class: "reader-meta-title", text: "provenance & identity" }));
  const dl = el("dl");
  function metaRow(k, v) {
    if (v == null || v === "") return;
    dl.appendChild(el("dt", { text: k }));
    dl.appendChild(el("dd", { text: String(v) }));
  }
  metaRow("doc_id", d.doc_id);
  metaRow("capture_id", d.capture_id);
  if (d.parent_doc_or_dup_group && d.parent_doc_or_dup_group !== d.doc_id)
    metaRow("dup_group", d.parent_doc_or_dup_group);
  metaRow("discovery", d.discovery_channel);
  metaRow("canonical_url", d.canonical_url);
  metaRow("content_hash", d.content_hash);
  if (d.near_dup_hash != null) metaRow("near_dup_hash", d.near_dup_hash);
  metaRow("robots", d.robots_decision);
  meta.appendChild(dl);
  body.appendChild(meta);

  // Related captures (sibling dup_group entries).
  const relatedSection = el("section", { class: "reader-related" });
  relatedSection.appendChild(el("div", { class: "reader-meta-title", text: "related captures" }));
  const relatedBody = el("div", { class: "related-body" });
  relatedBody.appendChild(el("p", { class: "muted", text: "loading…" }));
  relatedSection.appendChild(relatedBody);
  body.appendChild(relatedSection);

  void loadRelated(cid, relatedBody);

  setTimeout(() => $("#reader-close").focus(), 80);
}

async function loadRelated(cid, host) {
  try {
    const r = await api("/captures/" + encodeURIComponent(cid) + "/related?limit=12");
    clear(host);
    const sibs = r.siblings || [];
    if (sibs.length === 0) {
      host.appendChild(el("p", { class: "muted", text: "No related captures — this is the only member of its dup-group." }));
      return;
    }
    const list = el("ul", { class: "related-list" });
    for (const s of sibs) {
      const li = el("li", { class: "related-item" });
      const btn = el("button", {
        class: "related-link",
        onclick: () => openReader(s.capture_id),
        "aria-label": "Open " + (s.title || "related capture"),
      });
      btn.appendChild(el("span", { class: "related-when", text: ago(s.fetch_ts, true) + " ago" }));
      btn.appendChild(el("span", { class: "related-title", text: s.title || "(untitled)" }));
      const meta = el("span", { class: "related-meta" });
      meta.appendChild(el("span", { class: "src", text: s.source_type }));
      meta.appendChild(document.createTextNode(" · "));
      meta.appendChild(el("span", { class: "dom", text: s.domain || "—" }));
      meta.appendChild(document.createTextNode(" · "));
      meta.appendChild(document.createTextNode(fmt(s.text_len) + " ch"));
      btn.appendChild(meta);
      li.appendChild(btn);
      list.appendChild(li);
    }
    host.appendChild(list);
  } catch (err) {
    clear(host);
    host.appendChild(el("p", { class: "muted", text: "failed: " + err.message }));
  }
}
function closeReader() {
  const reader = $("#reader");
  const scrim = $("#reader-scrim");
  reader.setAttribute("aria-hidden", "true");
  scrim.hidden = true;
  if (readerLastFocus && readerLastFocus.focus) readerLastFocus.focus();
}

// ── Jobs view (full) ──────────────────────────────────────────
async function loadJobs() {
  try {
    const status = await api("/status");
    renderJobsFull(status.jobs || []);
  } catch (err) { console.error(err); }
}

function renderJobsFull(jobs) {
  const root = $("#jobs-full");
  clear(root);
  if (!jobs.length) {
    root.appendChild(el("p", { class: "muted", style: { padding: "22px 24px" } }, "No jobs yet — submit a backfill above or start the tail."));
    return;
  }
  for (const j of jobs) {
    const pct = j.tasks_total ? Math.round(100 * j.tasks_completed / j.tasks_total) : 0;
    const idCell = el("div", { class: "job-id" });
    idCell.appendChild(document.createTextNode(j.job_id));
    idCell.appendChild(el("span", { class: "kind", text: j.kind }));

    const progress = el("div", { class: "job-progress", role: "progressbar", "aria-valuenow": pct, "aria-valuemin": 0, "aria-valuemax": 100 });
    progress.appendChild(el("div", { class: "job-progress-bar", style: { width: pct + "%" } }));

    const counters = el("div", { class: "job-counters" });
    counters.appendChild(document.createTextNode(`${j.tasks_completed}/${j.tasks_total} tasks · `));
    counters.appendChild(el("b", { text: fmt(j.docs_emitted) }));
    counters.appendChild(document.createTextNode(" docs · "));
    counters.appendChild(el("b", { text: fmt(j.docs_dedup_dropped) }));
    counters.appendChild(document.createTextNode(" folded · "));
    counters.appendChild(document.createTextNode(j.started_at ? "started " + ago(j.started_at, false) : "queued"));

    const badge = el("span", { class: "badge badge-" + j.status, text: j.status });

    root.appendChild(el("div", { class: "job-row" }, idCell, progress, counters, badge));
  }
}

// ── Backfill form ─────────────────────────────────────────────
$("#bf-form")?.addEventListener("submit", async (e) => {
  e.preventDefault();
  const submit = e.target.querySelector('button[type=submit]');
  submit.disabled = true;
  try {
    const start = $("#bf-start").value;
    if (!start) { toast("pick a start date", "err"); return; }
    const sources = $$("#bf-form input[type=checkbox]").filter((x) => x.checked).map((x) => x.value);
    const domains = $("#bf-domains").value.split(",").map((s) => s.trim()).filter(Boolean);
    const langs = $("#bf-langs").value.split(",").map((s) => s.trim()).filter(Boolean);
    const max = parseInt($("#bf-max").value, 10);
    const body = {
      start: start,
      end_str: $("#bf-end").value || "now",
      sources: sources,
      domains: domains.length ? domains : null,
      languages: langs.length ? langs : null,
      max_tasks: Number.isFinite(max) ? max : null,
    };
    const resp = await api("/backfill", { method: "POST", body: JSON.stringify(body) });
    toast(`job ${resp.job_id} submitted (${resp.tasks_total} tasks)`, "ok");
    await api(`/backfill/${encodeURIComponent(resp.job_id)}/run`, { method: "POST", body: "{}" });
    toast(`job ${resp.job_id} running…`, "ok");
    void loadJobs();
    void refreshDashboard();
  } catch (err) {
    toast("backfill failed: " + err.message, "err");
  } finally { submit.disabled = false; }
});

// ── Tail page (rich live status) ──────────────────────────────
let tailPollTimer = null;
function startTailPolling() {
  if (tailPollTimer) return;
  loadTailView();
  tailPollTimer = setInterval(() => {
    if (currentRoute === "tail") loadTailView();
    else { clearInterval(tailPollTimer); tailPollTimer = null; }
  }, 2000);
}

async function loadTailView() {
  let data;
  try { data = await api("/tail/status"); } catch (e) { console.error(e); return; }
  const t = data.tail || {};
  const job = data.job || {};
  const counts = data.task_status_counts || {};
  const seed = data.per_seed || { feeds: [], fetch: {} };

  // Hero
  const big = $("#tail-big");
  big.dataset.state = t.running ? "on" : "off";
  $("#tail-big-state").textContent = t.running ? "running" : "stopped";
  $("#tail-big-detail").textContent = t.running
    ? "Reading public feeds. Newly discovered URLs are fetched politely, normalized to text, deduped, and written to the corpus."
    : (t.stopped_at ? `Last live run ended ${ago(t.stopped_at, false)}.` : "No live capture has been started yet.");
  $("#tail-big-meta").textContent = t.running
    ? `job ${t.job_id || "—"} · started ${ago(t.started_at, false)}`
    : (t.stopped_at ? `stopped at ${new Date(t.stopped_at).toLocaleString()}` : "");

  // Progress bar + counters
  const total = Number(job.tasks_total || 0);
  const done = Number(job.tasks_completed || 0);
  const pct = total ? Math.round(100 * done / total) : 0;
  $("#tail-progress-fill").style.width = pct + "%";
  $("#tail-progress-meta").textContent = total
    ? `${done}/${total} tasks · ${pct}%`
    : (t.running ? "queue empty" : "no active job");

  const cwrap = $("#tail-counters");
  clear(cwrap);
  function ctr(label, value, kind = "") {
    const c = el("div", { class: "ctr " + kind });
    c.appendChild(el("span", { class: "ctr-num", text: fmt(value) }));
    c.appendChild(el("span", { class: "ctr-lbl", text: label }));
    return c;
  }
  cwrap.appendChild(ctr("pending", counts.pending || 0, "ctr-pending"));
  cwrap.appendChild(ctr("fetching", counts.running || 0, "ctr-running"));
  cwrap.appendChild(ctr("completed", counts.completed || 0, "ctr-done"));
  cwrap.appendChild(ctr("docs captured", job.docs_emitted || 0, "ctr-docs"));
  cwrap.appendChild(ctr("folded", job.docs_dedup_dropped || 0, "ctr-folded"));
  if (counts.failed) cwrap.appendChild(ctr("failed", counts.failed, "ctr-failed"));

  // Now fetching
  const nowList = $("#tail-now-list");
  const running = data.running_tasks || [];
  const engine = data.engine || {};
  const pollSec = data.tail_poll_seconds || 60;
  clear(nowList);
  // Compute "next poll" countdown.
  let nextPollIn = null;
  if (t.running && engine.next_reseed_at) {
    const remaining = Math.max(0, Math.round(engine.next_reseed_at - Date.now() / 1000));
    nextPollIn = remaining;
  }
  $("#tail-now-meta").textContent = running.length
    ? `${running.length} in flight`
    : (t.running ? (nextPollIn !== null ? `next poll in ${nextPollIn}s` : `idle · poll every ${pollSec}s`) : "tail stopped");
  if (running.length === 0) {
    const idleMsg = t.running
      ? (engine.last_reseed_at
          ? `Idle. Last poll ${ago(engine.last_reseed_at, false)} found ${engine.last_reseed_count || 0} feed${engine.last_reseed_count === 1 ? "" : "s"} to re-arm. Next poll in ~${nextPollIn ?? pollSec}s.`
          : `Idle. First poll in ~${nextPollIn ?? pollSec}s.`)
      : "Tail is stopped.";
    nowList.appendChild(el("li", { class: "muted-li" }, idleMsg));
  } else {
    for (const r of running) {
      const li = el("li", { class: "tn-row" });
      li.appendChild(el("span", { class: "tn-spin", "aria-hidden": "true" }));
      li.appendChild(el("span", { class: "tn-source", text: r.source_type }));
      li.appendChild(el("span", { class: "tn-target", title: r.partition_key, text: shortPartition(r.partition_key) }));
      li.appendChild(el("span", { class: "tn-elapsed", text: r.started_at ? ago(r.started_at, true) : "" }));
      nowList.appendChild(li);
    }
  }

  // Just captured
  const doneList = $("#tail-done-list");
  const recent = data.recent_completed || [];
  clear(doneList);
  $("#tail-done-meta").textContent = recent.length ? `${recent.length} recent` : "—";
  if (recent.length === 0) {
    doneList.appendChild(el("li", { class: "muted-li" }, "No completed fetches yet."));
  } else {
    for (const r of recent) {
      const li = el("li", { class: "tn-row" });
      li.appendChild(el("span", { class: "tn-tick", "aria-hidden": "true" }, "✓"));
      li.appendChild(el("span", { class: "tn-source", text: r.source_type }));
      li.appendChild(el("span", { class: "tn-target", title: r.partition_key, text: shortPartition(r.partition_key) }));
      const meta = el("span", { class: "tn-result" });
      meta.appendChild(document.createTextNode(`${r.docs_emitted || 0} doc`));
      if (r.docs_dedup_dropped) meta.appendChild(document.createTextNode(` · ${r.docs_dedup_dropped} folded`));
      li.appendChild(meta);
      li.appendChild(el("span", { class: "tn-elapsed", text: r.completed_at ? ago(r.completed_at, true) : "" }));
      doneList.appendChild(li);
    }
  }

  // Recent chunks
  const chunksList = $("#tail-chunks-list");
  const chunks = data.recent_chunks || [];
  clear(chunksList);
  if (chunks.length === 0) {
    chunksList.appendChild(el("li", { class: "muted-li" }, "No JSONL chunks committed yet."));
  } else {
    for (const c of chunks) {
      const li = el("li", { class: "tc-row" });
      li.appendChild(el("span", { class: "tc-ic", "aria-hidden": "true" }, "▢"));
      li.appendChild(el("span", { class: "tc-records", text: fmt(c.records) + " records" }));
      li.appendChild(el("span", { class: "tc-bytes", text: fmtBytes(c.bytes) }));
      li.appendChild(el("span", { class: "tc-path", title: c.path, text: shortPath(c.path) }));
      li.appendChild(el("span", { class: "tc-when", text: c.committed_at ? ago(c.committed_at, true) : "" }));
      chunksList.appendChild(li);
    }
  }

  // Seeds
  const seedsBlock = $("#seeds-block");
  clear(seedsBlock);
  $("#seeds-meta").textContent = `${seed.feeds.length} configured · fetch: ${
    Object.entries(seed.fetch || {}).map(([k, v]) => `${v} ${k}`).join(", ") || "none"
  }`;
  if (!seed.feeds.length) {
    seedsBlock.appendChild(el("p", { class: "muted" },
      "Edit ", el("code", { text: "configs/tail_seeds.yaml" }),
      " to change which feeds are read."));
  } else {
    const ul = el("ul", { class: "seeds-list" });
    for (const f of seed.feeds) {
      const li = el("li", { class: "seed-row" });
      const kind = f.partition_key.split(":", 1)[0];
      const url = f.partition_key.slice(kind.length + 1);
      li.appendChild(el("span", { class: "seed-kind", text: kind }));
      li.appendChild(el("span", { class: "seed-url", title: url, text: url }));
      li.appendChild(el("span", { class: "seed-status badge badge-" + f.status, text: f.status }));
      ul.appendChild(li);
    }
    seedsBlock.appendChild(ul);
  }
}

function shortPartition(pk) {
  if (!pk) return "";
  const idx = pk.indexOf("://");
  if (idx < 0) return pk;
  const url = pk.slice(pk.indexOf(":") + 1);
  try {
    const u = new URL(url);
    return u.host + (u.pathname.length > 30 ? u.pathname.slice(0, 27) + "…" : u.pathname);
  } catch { return url.slice(0, 80); }
}
function shortPath(p) {
  if (!p) return "";
  const parts = p.split("/");
  return parts.slice(-3).join("/");
}
function fmtBytes(n) {
  if (n == null) return "—";
  if (n < 1024) return n + " B";
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + " KB";
  return (n / 1024 / 1024).toFixed(2) + " MB";
}

// Bind tail buttons (both strip + big)
for (const id of ["tail-start-btn", "tail-big-start"]) {
  $("#" + id)?.addEventListener("click", async (e) => {
    e.target.disabled = true;
    try { await api("/tail/start", { method: "POST", body: "{}" }); toast("tail started", "ok"); }
    catch (err) { toast("tail start failed: " + err.message, "err"); }
    finally { e.target.disabled = false; void refreshDashboard(); void loadTailView(); }
  });
}
for (const id of ["tail-stop-btn", "tail-big-stop"]) {
  $("#" + id)?.addEventListener("click", async (e) => {
    e.target.disabled = true;
    try { await api("/tail/stop", { method: "POST", body: "{}" }); toast("tail paused", "ok"); }
    catch (err) { toast("tail stop failed: " + err.message, "err"); }
    finally { e.target.disabled = false; void refreshDashboard(); void loadTailView(); }
  });
}

// ── Settings ──────────────────────────────────────────────────
async function loadSettings() {
  const root = $("#settings-block");
  clear(root);
  try {
    const [health, dedup] = await Promise.all([api("/healthz"), api("/dedup-stats")]);
    for (const [k, v] of Object.entries(health)) {
      const row = el("div", { class: "kv-row" });
      row.appendChild(el("div", { class: "kv-key", text: k }));
      row.appendChild(el("div", { class: "kv-val", text: typeof v === "object" ? JSON.stringify(v) : String(v) }));
      root.appendChild(row);
    }
    const dblock = $("#dedup-block");
    clear(dblock);
    for (const [k, v] of Object.entries(dedup)) {
      const row = el("div", { class: "kv-row" });
      row.appendChild(el("div", { class: "kv-key", text: k }));
      row.appendChild(el("div", { class: "kv-val", text: String(v) }));
      dblock.appendChild(row);
    }
  } catch (err) {
    root.appendChild(el("p", { class: "muted", text: "failed: " + err.message }));
  }
}

// ── Live activity feed (dashboard rail) ───────────────────────
const FEED_MAX = 25;
async function refreshFeed() {
  try {
    const data = await api(`/captures?limit=${FEED_MAX}&offset=0`);
    const rows = data.rows || [];
    if (rows.length === 0) {
      $("#feed").replaceChildren();
      $("#rail-empty").hidden = false;
      $("#rail-sub").textContent = "no captures yet";
      return;
    }
    $("#rail-empty").hidden = true;
    $("#rail-sub").textContent = `${fmt(data.total)} captures total`;
    renderFeed(rows);
  } catch (err) { console.error("feed", err); }
}

function renderFeed(rows) {
  const root = $("#feed");
  // Diff: if first row's capture_id is new, animate it.
  const prevFirst = lastFeedCaptureId;
  const newFirst = rows[0]?.capture_id;
  lastFeedCaptureId = newFirst;
  const newCaptureIds = new Set();
  if (prevFirst && newFirst !== prevFirst) {
    for (const r of rows) {
      if (r.capture_id === prevFirst) break;
      newCaptureIds.add(r.capture_id);
    }
  }
  clear(root);
  for (const r of rows) {
    const isNew = newCaptureIds.has(r.capture_id);
    const item = el("li", { class: "feed-item" + (isNew ? " is-new" : "") });
    item.appendChild(el("div", { class: "feed-bullet" }));
    const inner = el("div");
    inner.appendChild(el("div", { class: "feed-when", text: ago(r.fetch_ts, false) }));
    const title = el("button", {
      class: "feed-title",
      style: { background: "transparent", border: "none", padding: 0, textAlign: "left" },
      "aria-label": "Open " + (r.title || "capture"),
      onclick: () => openReader(r.capture_id),
    }, r.title || "(untitled)");
    inner.appendChild(title);
    const where = el("div", { class: "feed-where" });
    where.appendChild(el("span", { class: "src", text: r.source_type }));
    where.appendChild(document.createTextNode(" · "));
    where.appendChild(el("span", { class: "dom", text: r.domain || "—" }));
    inner.appendChild(where);
    item.appendChild(inner);
    root.appendChild(item);
  }
}

// ── Command palette ───────────────────────────────────────────
const cmdkOverlay = $("#cmdk");
const cmdkInput = $("#cmdk-input");
const cmdkList = $("#cmdk-list");
let cmdkActive = 0;
let cmdkResults = [];

function buildCommands(query = "") {
  const q = query.trim().toLowerCase();
  const sections = [
    { kind: "nav", icon: "◐", label: "Go to Dashboard", do: () => navigate("dashboard") },
    { kind: "nav", icon: "≡", label: "Go to Captures",  do: () => navigate("captures") },
    { kind: "nav", icon: "▱", label: "Go to Jobs",      do: () => navigate("jobs") },
    { kind: "nav", icon: "⟳", label: "Go to Tail",      do: () => navigate("tail") },
    { kind: "nav", icon: "⚙", label: "Go to Settings",  do: () => navigate("settings") },
    { kind: "action", icon: "▶", label: "Start tail",   do: async () => { await api("/tail/start", { method: "POST", body: "{}" }); toast("tail started", "ok"); void refreshDashboard(); } },
    { kind: "action", icon: "■", label: "Pause tail",   do: async () => { await api("/tail/stop", { method: "POST", body: "{}" }); toast("tail paused", "ok"); void refreshDashboard(); } },
    { kind: "action", icon: "⌕", label: "Search captures…", do: () => { navigate("captures"); setTimeout(() => $("#caps-search").focus(), 200); } },
  ];
  if (!q) return sections;
  if (q.length >= 2) {
    // Local search shortcut → jump to captures with this query.
    sections.unshift({ kind: "search", icon: "⌕", label: `Search corpus for "${query}"`, do: () => { navigate("captures"); $("#caps-search").value = query; void loadCaptures(true); } });
  }
  return sections.filter((c) => c.label.toLowerCase().includes(q) || c.kind.includes(q));
}

function openCmdk() {
  cmdkOverlay.hidden = false;
  cmdkInput.value = "";
  cmdkActive = 0;
  renderCmdk("");
  setTimeout(() => cmdkInput.focus(), 30);
}
function closeCmdk() { cmdkOverlay.hidden = true; }
function renderCmdk(q) {
  cmdkResults = buildCommands(q);
  clear(cmdkList);
  if (cmdkResults.length === 0) {
    cmdkList.appendChild(el("li", { class: "cmdk-empty" }, "no matches"));
    return;
  }
  cmdkResults.forEach((c, i) => {
    const li = el("li", { class: "cmdk-item" + (i === cmdkActive ? " is-active" : ""), role: "option", "aria-selected": i === cmdkActive ? "true" : "false" });
    li.appendChild(el("span", { class: "cmdk-item-icon", text: c.icon }));
    li.appendChild(el("span", { class: "cmdk-item-label", text: c.label }));
    li.appendChild(el("span", { class: "cmdk-item-kind", text: c.kind }));
    li.addEventListener("click", () => { closeCmdk(); c.do(); });
    li.addEventListener("mouseenter", () => { cmdkActive = i; renderCmdk(q); });
    cmdkList.appendChild(li);
  });
}
cmdkInput?.addEventListener("input", (e) => { cmdkActive = 0; renderCmdk(e.target.value); });
cmdkInput?.addEventListener("keydown", (e) => {
  if (e.key === "ArrowDown") { e.preventDefault(); cmdkActive = Math.min(cmdkResults.length - 1, cmdkActive + 1); renderCmdk(cmdkInput.value); }
  else if (e.key === "ArrowUp") { e.preventDefault(); cmdkActive = Math.max(0, cmdkActive - 1); renderCmdk(cmdkInput.value); }
  else if (e.key === "Enter") { e.preventDefault(); const c = cmdkResults[cmdkActive]; if (c) { closeCmdk(); c.do(); } }
  else if (e.key === "Escape") { closeCmdk(); }
});
cmdkOverlay?.addEventListener("click", (e) => { if (e.target === cmdkOverlay) closeCmdk(); });

// ── Global keyboard shortcuts ─────────────────────────────────
document.addEventListener("keydown", (e) => {
  // Cmd/Ctrl+K → open palette
  if ((e.metaKey || e.ctrlKey) && (e.key === "k" || e.key === "K")) { e.preventDefault(); openCmdk(); return; }
  // "/" focus search (when on captures view)
  if (e.key === "/" && !e.metaKey && !e.ctrlKey && !e.altKey) {
    const tag = (document.activeElement?.tagName || "").toLowerCase();
    if (tag !== "input" && tag !== "textarea" && tag !== "select") {
      e.preventDefault();
      if (currentRoute !== "captures") navigate("captures");
      setTimeout(() => $("#caps-search").focus(), 80);
    }
  }
  // Number shortcuts 1..5 for routes (when not typing)
  if (/^[1-5]$/.test(e.key) && !e.metaKey && !e.ctrlKey && !e.altKey) {
    const tag = (document.activeElement?.tagName || "").toLowerCase();
    if (tag !== "input" && tag !== "textarea" && tag !== "select") {
      navigate(ROUTES[parseInt(e.key, 10) - 1]);
    }
  }
  // Esc: close any overlay
  if (e.key === "Escape") {
    if (!cmdkOverlay.hidden) closeCmdk();
    else if ($("#reader").getAttribute("aria-hidden") === "false") closeReader();
    else if ($(".sidebar").classList.contains("is-open")) $(".sidebar").classList.remove("is-open");
  }
});

// ── Bind nav buttons ─────────────────────────────────────────
$$(".nav-item").forEach((b) => {
  b.addEventListener("click", () => navigate(b.dataset.route));
});
$$('[data-route]').forEach((b) => {
  if (b.classList.contains("nav-item")) return;
  b.addEventListener("click", (e) => { e.preventDefault(); navigate(b.dataset.route); });
});
$('[data-action="open-cmdk"]')?.addEventListener("click", openCmdk);
$("#reader-close")?.addEventListener("click", closeReader);
$("#reader-scrim")?.addEventListener("click", closeReader);

// Captures bindings
$("#caps-apply")?.addEventListener("click", () => loadCaptures(true));
$("#caps-reset")?.addEventListener("click", () => {
  $("#caps-search").value = "";
  $("#caps-source").value = "";
  $("#caps-domain").value = "";
  $("#caps-start").value = "";
  $("#caps-end").value = "";
  loadCaptures(true);
});
$("#caps-search")?.addEventListener("input", () => {
  clearTimeout(capsSearchTimer);
  capsSearchTimer = setTimeout(() => loadCaptures(true), 300);
});
$("#caps-source")?.addEventListener("change", () => loadCaptures(true));
$("#caps-prev")?.addEventListener("click", () => { caps.offset = Math.max(0, caps.offset - caps.limit); loadCaptures(false); });
$("#caps-next")?.addEventListener("click", () => { caps.offset += caps.limit; loadCaptures(false); });
$("#jobs-refresh")?.addEventListener("click", () => loadJobs());

// Mobile nav
$("#mobile-nav-btn")?.addEventListener("click", () => {
  const sb = $(".sidebar");
  const open = sb.classList.toggle("is-open");
  $("#mobile-nav-btn").setAttribute("aria-expanded", String(open));
});

// API offline retry button — re-probes /healthz; the next successful
// api() call clears the banner automatically.
$("#api-offline-retry")?.addEventListener("click", async (e) => {
  const btn = e.target;
  btn.disabled = true;
  btn.textContent = "checking…";
  try {
    await api("/healthz");
    // success — banner already cleared by api() itself.
    void refreshDashboard();
    void refreshFeed();
    if (currentRoute === "tail") void loadTailView();
  } catch (_) {
    // still offline — banner remains.
  } finally {
    btn.disabled = false;
    btn.textContent = "retry";
  }
});

// ── Boot ──────────────────────────────────────────────────────
const initialRoute = (location.hash || "#dashboard").slice(1);
$("#bf-start") && ($("#bf-start").value = isoDay(Date.now() - 30 * 86400 * 1000));
navigate(initialRoute, { push: false });
void refreshDashboard();
void refreshFeed();
setInterval(refreshDashboard, 5000);
setInterval(refreshFeed, 5000);
