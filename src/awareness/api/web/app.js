// Awareness dashboard controller.
//
// Strict XSS hygiene: all data values touch the DOM exclusively via
// textContent / setAttribute / createElement — never innerHTML. The only
// places we set innerHTML are for static structure cleared between renders.

(() => {
  // ── tiny DOM + format helpers ──────────────────────────────────────
  const $ = (q) => document.querySelector(q);
  const $$ = (q) => Array.from(document.querySelectorAll(q));

  function el(tag, props, ...children) {
    const node = document.createElement(tag);
    if (props) {
      for (const [k, v] of Object.entries(props)) {
        if (v == null) continue;
        if (k === "class") node.className = v;
        else if (k === "text") node.textContent = v;
        else if (k === "dataset") for (const [dk, dv] of Object.entries(v)) node.dataset[dk] = dv;
        else if (k === "style") Object.assign(node.style, v);
        else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
        else node.setAttribute(k, v);
      }
    }
    for (const c of children) {
      if (c == null || c === false) continue;
      node.appendChild(typeof c === "string" ? document.createTextNode(c) : c);
    }
    return node;
  }
  function clear(node) { while (node.firstChild) node.removeChild(node.firstChild); }

  const fmt = (n) => (n == null ? "—" : new Intl.NumberFormat().format(n));
  const ago = (iso) => {
    if (!iso) return "—";
    const t = new Date(iso).getTime();
    if (Number.isNaN(t)) return String(iso);
    const d = (Date.now() - t) / 1000;
    if (d < 60) return Math.max(1, Math.round(d)) + "s ago";
    if (d < 3600) return Math.round(d / 60) + "m ago";
    if (d < 86400) return Math.round(d / 3600) + "h ago";
    return Math.round(d / 86400) + "d ago";
  };
  const isoDay = (d) => new Date(d).toISOString().slice(0, 10);

  let toastTimer;
  function toast(msg, kind = "ok") {
    const t = $("#toast");
    t.textContent = msg;
    t.className = "toast show " + kind;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => (t.className = "toast"), 3200);
  }

  async function api(path, opts = {}) {
    const res = await fetch(path, {
      headers: { "Content-Type": "application/json", ...(opts.headers || {}) },
      ...opts,
    });
    if (!res.ok) {
      let detail = res.statusText;
      try { detail = (await res.json()).detail || detail; } catch (_) {}
      throw new Error(res.status + " " + detail);
    }
    if (res.status === 204) return null;
    return await res.json();
  }

  function pill(opts) {
    const cls = "pill" + (opts.modifier ? " " + opts.modifier : "");
    const span = el("span", { class: cls });
    if (opts.live) span.appendChild(el("span", { class: "led" }));
    if (opts.value != null) {
      span.appendChild(el("b", { text: opts.value }));
      span.appendChild(document.createTextNode(" " + opts.label));
    } else {
      span.appendChild(document.createTextNode(opts.label));
    }
    return span;
  }

  // ── header / counters ──────────────────────────────────────────────
  async function refreshHeader() {
    let status, dedup;
    try {
      [status, dedup] = await Promise.all([api("/status"), api("/dedup-stats")]);
    } catch (e) {
      console.error("header refresh failed", e);
      return;
    }
    const t = status.tail || {};
    const docsTotal = (status.jobs || []).reduce((a, j) => a + (j.docs_emitted || 0), 0);
    const jobsTotal = (status.jobs || []).length;

    const pills = $("#pills");
    clear(pills);
    if (t.running) {
      const p = el("span", { class: "pill live" });
      p.appendChild(el("span", { class: "led" }));
      p.appendChild(document.createTextNode("tail · job "));
      p.appendChild(document.createTextNode(t.job_id || "—"));
      pills.appendChild(p);
    } else {
      pills.appendChild(el("span", { class: "pill off", text: "tail · stopped" }));
    }
    pills.appendChild(pill({ value: fmt(docsTotal), label: "docs across recent jobs" }));
    pills.appendChild(pill({ value: fmt(dedup.distinct_content_hashes), label: "distinct hashes" }));
    pills.appendChild(pill({ value: fmt(dedup.total_captures_seen), label: "total captures seen" }));
    pills.appendChild(pill({ value: fmt(jobsTotal), label: "jobs" }));
    $("#last-update").textContent = "updated " + new Date().toLocaleTimeString();

    // Counters
    const counters = $("#counters");
    clear(counters);
    function counter(label, value, sub) {
      const c = el("div", { class: "counter" });
      c.appendChild(el("div", { class: "label", text: label }));
      c.appendChild(el("div", { class: "value", text: value }));
      c.appendChild(el("div", { class: "sub", text: sub || "" }));
      return c;
    }
    counters.appendChild(counter(
      "tail", t.running ? "running" : "stopped",
      t.started_at ? "since " + ago(t.started_at) : "—"
    ));
    counters.appendChild(counter(
      "total captures", fmt(dedup.total_captures_seen),
      fmt(dedup.distinct_content_hashes) + " unique"
    ));
    counters.appendChild(counter(
      "docs emitted", fmt(docsTotal),
      jobsTotal + " recent jobs"
    ));
    counters.appendChild(counter(
      "dedup folds",
      fmt(Math.max(0, dedup.total_captures_seen - dedup.distinct_content_hashes)),
      fmt(dedup.near_dup_index_rows) + " simhash rows"
    ));

    // Tail status badge
    const ts = $("#tail-status");
    ts.classList.toggle("running", !!t.running);
    ts.querySelector(".label").textContent = t.running ? "running" : "stopped";

    const info = $("#tail-info");
    clear(info);
    if (t.running) {
      info.appendChild(el("span", { class: "spin" }));
      info.appendChild(document.createTextNode("job " + (t.job_id || "—") + " · started " + ago(t.started_at)));
    } else if (t.stopped_at) {
      info.appendChild(document.createTextNode("last stopped " + ago(t.stopped_at)));
    }

    renderJobs(status.jobs || []);
  }

  function renderJobs(jobs) {
    const root = $("#jobs-list");
    clear(root);
    if (!jobs.length) {
      const j = el("div", { class: "job" });
      j.appendChild(el("span", { class: "empty", text: "no jobs yet" }));
      root.appendChild(j);
      return;
    }
    for (const j of jobs) {
      const pct = j.tasks_total ? Math.round(100 * j.tasks_completed / j.tasks_total) : 0;
      const jobEl = el("div", { class: "job" });
      const idCell = el("div", { class: "id" });
      idCell.appendChild(el("b", { text: j.job_id }));
      idCell.appendChild(document.createTextNode(" "));
      idCell.appendChild(el("span", { class: "small", text: j.kind }));
      const statusCell = el("div");
      statusCell.appendChild(el("span", { class: "status " + j.status, text: j.status }));
      const stats = el("div", { class: "stats" });
      stats.textContent =
        j.tasks_completed + "/" + j.tasks_total + " tasks (" + pct + "%) · " +
        fmt(j.docs_emitted) + " docs · " +
        fmt(j.docs_dedup_dropped) + " folded · " +
        (j.started_at ? "started " + ago(j.started_at) : "queued");
      jobEl.appendChild(idCell);
      jobEl.appendChild(statusCell);
      jobEl.appendChild(stats);
      root.appendChild(jobEl);
    }
  }

  // ── tail controls ──────────────────────────────────────────────────
  $("#tail-start").addEventListener("click", async (e) => {
    e.target.disabled = true;
    try {
      await api("/tail/start", { method: "POST", body: "{}" });
      toast("tail started", "ok");
    } catch (err) { toast("tail start failed: " + err.message, "err"); }
    finally { e.target.disabled = false; refreshHeader(); }
  });

  $("#tail-stop").addEventListener("click", async (e) => {
    e.target.disabled = true;
    try {
      await api("/tail/stop", { method: "POST", body: "{}" });
      toast("tail stop requested", "ok");
    } catch (err) { toast("tail stop failed: " + err.message, "err"); }
    finally { e.target.disabled = false; refreshHeader(); }
  });

  // ── backfill submit ────────────────────────────────────────────────
  $("#bf-submit").addEventListener("click", async (e) => {
    e.target.disabled = true;
    try {
      const start = $("#bf-start").value;
      if (!start) { toast("pick a start date", "err"); return; }
      const sources = Array.from($("#bf-sources").querySelectorAll("input[type=checkbox]"))
        .filter((x) => x.checked).map((x) => x.value);
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
      toast("job " + resp.job_id + " submitted (" + resp.tasks_total + " tasks)", "ok");
      await api("/backfill/" + encodeURIComponent(resp.job_id) + "/run", { method: "POST", body: "{}" });
      toast("job " + resp.job_id + " running…", "ok");
    } catch (err) { toast("backfill failed: " + err.message, "err"); }
    finally { e.target.disabled = false; refreshHeader(); }
  });

  $("#jobs-refresh").addEventListener("click", refreshHeader);

  // ── captures table ────────────────────────────────────────────────
  const cap = { limit: 50, offset: 0, total: 0 };

  async function refreshCaptures() {
    const params = new URLSearchParams();
    params.set("limit", cap.limit);
    params.set("offset", cap.offset);
    const search = $("#cap-search").value.trim();
    const source = $("#cap-source").value;
    const domain = $("#cap-domain").value.trim();
    const start = $("#cap-start").value;
    const end = $("#cap-end").value;
    if (search) params.set("search", search);
    if (source) params.set("source", source);
    if (domain) params.set("domain", domain);
    if (start) params.set("start", start);
    if (end) params.set("end", end);

    try {
      const data = await api("/captures?" + params.toString());
      cap.total = data.total;
      renderCaptures(data.rows);
      const from = data.total ? cap.offset + 1 : 0;
      const to = Math.min(cap.offset + data.rows.length, cap.total);
      $("#cap-range").textContent = from + "–" + to + " of " + fmt(cap.total);
      $("#cap-total").textContent = fmt(cap.total) + " rows";
      $("#cap-prev").disabled = cap.offset <= 0;
      $("#cap-next").disabled = cap.offset + cap.limit >= cap.total;
    } catch (err) {
      console.error(err);
      const tb = $("#cap-body");
      clear(tb);
      const tr = el("tr", { class: "empty-row" });
      tr.appendChild(el("td", { colspan: "6", text: "query failed: " + err.message }));
      tb.appendChild(tr);
    }
  }

  function renderCaptures(rows) {
    const tb = $("#cap-body");
    clear(tb);
    if (!rows.length) {
      const tr = el("tr", { class: "empty-row" });
      tr.appendChild(el("td", { colspan: "6", text: "no captures match — try clearing filters or run a backfill" }));
      tb.appendChild(tr);
      return;
    }
    for (const r of rows) {
      const tr = el("tr", { class: "row", dataset: { cid: r.capture_id } });
      tr.appendChild(el("td", { class: "ts", title: String(r.fetch_ts || ""), text: ago(r.fetch_ts) }));
      tr.appendChild(el("td", { class: "src", text: r.source_type }));
      tr.appendChild(el("td", { class: "domain", text: r.domain || "—" }));
      const titleCell = el("td", { class: "title" });
      titleCell.appendChild(el("span", { class: "t", text: r.title || "(untitled)" }));
      if (r.url) titleCell.appendChild(el("span", { class: "u", text: r.url }));
      tr.appendChild(titleCell);
      tr.appendChild(el("td", { style: { textAlign: "right" }, text: fmt(r.text_len) }));
      tr.appendChild(el("td", { text: r.language || "—" }));
      tr.addEventListener("click", () => openDetail(r.capture_id, tr));
      tb.appendChild(tr);
    }
  }

  $("#cap-apply").addEventListener("click", () => { cap.offset = 0; refreshCaptures(); });
  $("#cap-search").addEventListener("keydown", (e) => {
    if (e.key === "Enter") { cap.offset = 0; refreshCaptures(); }
  });
  $("#cap-prev").addEventListener("click", () => { cap.offset = Math.max(0, cap.offset - cap.limit); refreshCaptures(); });
  $("#cap-next").addEventListener("click", () => { cap.offset += cap.limit; refreshCaptures(); });
  $("#cap-source").addEventListener("change", () => { cap.offset = 0; refreshCaptures(); });

  // ── detail drawer ──────────────────────────────────────────────────
  async function openDetail(cid, tr) {
    $$(".cap-table tr.row").forEach((x) => x.classList.remove("selected"));
    if (tr) tr.classList.add("selected");
    const body = $("#drawer-body");
    clear(body);
    body.appendChild(el("div", { class: "empty", text: "loading…" }));
    $("#drawer").classList.add("open");
    $("#scrim").classList.add("open");

    let d;
    try { d = await api("/captures/" + encodeURIComponent(cid)); }
    catch (err) {
      clear(body);
      body.appendChild(el("div", { class: "empty", text: "failed: " + err.message }));
      return;
    }

    clear(body);
    body.appendChild(el("h3", { class: "title", text: d.title || "(untitled)" }));
    const meta = el("dl", { class: "meta" });
    function entry(label, value, opts) {
      meta.appendChild(el("dt", { text: label }));
      const dd = el("dd");
      if (opts && opts.link && value) {
        const a = el("a", { href: value, target: "_blank", rel: "noopener" });
        a.textContent = value;
        dd.appendChild(a);
      } else {
        dd.textContent = value == null ? "—" : String(value);
      }
      meta.appendChild(dd);
    }
    entry("doc_id", d.doc_id);
    entry("capture_id", d.capture_id);
    if (d.parent_doc_or_dup_group && d.parent_doc_or_dup_group !== d.doc_id) {
      entry("dup_group", d.parent_doc_or_dup_group);
    }
    entry("source", d.source_type + " · " + d.source_name);
    entry("discovery", d.discovery_channel);
    entry("domain", d.domain);
    entry("url", d.url, { link: true });
    entry("fetch_ts", d.fetch_ts);
    entry("observed_ts", d.observed_ts);
    if (d.published_ts) entry("published", d.published_ts);
    entry("language", d.language);
    entry("content_hash", d.content_hash);
    if (d.near_dup_hash != null) entry("near_dup_hash", String(d.near_dup_hash));
    entry("robots", d.robots_decision);
    body.appendChild(meta);

    const text = el("div", { class: "text", text: d.text || "(empty)" });
    body.appendChild(text);
  }

  function closeDrawer() {
    $("#drawer").classList.remove("open");
    $("#scrim").classList.remove("open");
    $$(".cap-table tr.row").forEach((x) => x.classList.remove("selected"));
  }

  $("#drawer-close").addEventListener("click", closeDrawer);
  $("#scrim").addEventListener("click", closeDrawer);
  document.addEventListener("keydown", (e) => { if (e.key === "Escape") closeDrawer(); });

  // ── boot ──────────────────────────────────────────────────────────
  $("#bf-start").value = isoDay(Date.now() - 30 * 86400 * 1000);
  refreshHeader();
  refreshCaptures();
  setInterval(refreshHeader, 4000);
  setInterval(() => { if (!$("#drawer").classList.contains("open")) refreshCaptures(); }, 6000);
})();
