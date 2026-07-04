/* ==========================================================================
   Distributed Job Scheduler — Dashboard logic
   Vanilla JS, no build step. Talks to the FastAPI backend over REST.
   ========================================================================== */

const API_BASE = window.API_BASE || "http://localhost:8000";
const WS_BASE = API_BASE.replace(/^http/, "ws");

const state = {
  token: localStorage.getItem("djs_token") || null,
  user: null,
  role: null,
  organizations: [],
  projects: [],
  queues: [],
  currentQueueId: null,
  jobsFilter: { status: "", queue_id: "" },
  jobsOffset: 0,
  jobsLimit: 25,
  ws: null,
};

// ---------------- API client ----------------

async function api(path, { method = "GET", body, form, auth = true } = {}) {
  const headers = {};
  if (auth && state.token) headers["Authorization"] = `Bearer ${state.token}`;

  let payload;
  if (form) {
    headers["Content-Type"] = "application/x-www-form-urlencoded";
    payload = new URLSearchParams(form);
  } else if (body !== undefined) {
    headers["Content-Type"] = "application/json";
    payload = JSON.stringify(body);
  }

  const resp = await fetch(`${API_BASE}${path}`, { method, headers, body: payload });
  if (resp.status === 401) {
    logout();
    throw new Error("Session expired, please log in again");
  }
  if (!resp.ok) {
    let detail = resp.statusText;
    try { detail = (await resp.json()).detail || detail; } catch (_) {}
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  if (resp.status === 204) return null;
  const text = await resp.text();
  return text ? JSON.parse(text) : null;
}

// ---------------- Toast ----------------

function toast(message, type = "") {
  const el = document.getElementById("toast");
  el.textContent = message;
  el.className = `toast show ${type}`;
  clearTimeout(toast._t);
  toast._t = setTimeout(() => (el.className = "toast"), 3200);
}

// ---------------- Auth ----------------

function logout() {
  state.token = null;
  localStorage.removeItem("djs_token");
  document.getElementById("app").style.display = "none";
  document.getElementById("login-screen").style.display = "flex";
}

async function boot() {
  if (!state.token) {
    logout();
    return;
  }
  document.getElementById("login-screen").style.display = "none";
  document.getElementById("app").style.display = "flex";
  try {
    const me = await api("/api/auth/me");
    state.role = me.role;
    state.userEmail = me.email;
  } catch (_) { /* fall back to email captured at login */ }
  await loadOrganizations();
  document.getElementById("session-email").textContent = `${state.userEmail || "signed in"} · ${state.role || ""}`;
  applyRoleVisibility();
  connectWebSocket();
  navigate("overview");
}

function applyRoleVisibility() {
  // Admin-only actions (pause/resume queue, replay DLQ, drain worker) are
  // hidden client-side for non-admins; the API enforces the real
  // restriction (see require_role in app/auth.py) - this is just UX polish
  // so a viewer/operator doesn't see buttons that would 403.
  document.body.classList.toggle("role-admin", state.role === "admin");
}

function connectWebSocket() {
  if (state.ws) { try { state.ws.close(); } catch (_) {} }
  try {
    const ws = new WebSocket(`${WS_BASE}/ws/dashboard`);
    ws.onmessage = (event) => {
      const msg = JSON.parse(event.data);
      if (msg.type === "health" && document.getElementById("page-overview").classList.contains("active")) {
        renderHealth(msg.data);
      }
    };
    ws.onclose = () => { setTimeout(connectWebSocket, 4000); }; // auto-reconnect
    state.ws = ws;
  } catch (_) { /* dashboard still works fine via polling if websockets are unavailable */ }
}

document.getElementById("login-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const email = document.getElementById("login-email").value;
  const password = document.getElementById("login-password").value;
  const errEl = document.getElementById("login-error");
  errEl.textContent = "";
  try {
    const data = await api("/api/auth/login", { method: "POST", form: { username: email, password }, auth: false });
    state.token = data.access_token;
    state.userEmail = email;
    localStorage.setItem("djs_token", state.token);
    await boot();
  } catch (err) {
    errEl.textContent = err.message;
  }
});

document.getElementById("register-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const email = document.getElementById("register-email").value;
  const password = document.getElementById("register-password").value;
  const full_name = document.getElementById("register-name").value;
  const role = document.getElementById("register-role").value;
  const errEl = document.getElementById("register-error");
  errEl.textContent = "";
  try {
    await api("/api/auth/register", { method: "POST", body: { email, password, full_name, role }, auth: false });
    const data = await api("/api/auth/login", { method: "POST", form: { username: email, password }, auth: false });
    state.token = data.access_token;
    state.userEmail = email;
    localStorage.setItem("djs_token", state.token);
    await boot();
  } catch (err) {
    errEl.textContent = err.message;
  }
});

document.getElementById("logout-btn").addEventListener("click", logout);

document.querySelectorAll(".login-tabs button").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".login-tabs button").forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");
    const isLogin = btn.dataset.tab === "login";
    document.getElementById("login-form").style.display = isLogin ? "block" : "none";
    document.getElementById("register-form").style.display = isLogin ? "none" : "block";
  });
});

// ---------------- Navigation ----------------

function navigate(page) {
  document.querySelectorAll(".nav-item").forEach((n) => n.classList.toggle("active", n.dataset.page === page));
  document.querySelectorAll(".page").forEach((p) => p.classList.toggle("active", p.id === `page-${page}`));
  const loaders = { overview: loadOverview, queues: loadQueuesPage, jobs: loadJobsPage, workers: loadWorkersPage, dlq: loadDlqPage };
  if (loaders[page]) loaders[page]();
}
document.querySelectorAll(".nav-item").forEach((n) => n.addEventListener("click", () => navigate(n.dataset.page)));

// ---------------- Bootstrap data (orgs/projects) ----------------

async function loadOrganizations() {
  state.organizations = await api("/api/organizations");
  if (state.organizations.length === 0) {
    // First-run convenience: auto-create a default org + project so the
    // dashboard is immediately usable.
    const org = await api("/api/organizations", { method: "POST", body: { name: "Default Org" } });
    state.organizations = [org];
  }
  state.projects = [];
  for (const org of state.organizations) {
    const projs = await api(`/api/projects?organization_id=${org.id}`);
    state.projects.push(...projs);
  }
  if (state.projects.length === 0) {
    const project = await api("/api/projects", {
      method: "POST", body: { name: "Default Project", organization_id: state.organizations[0].id },
    });
    state.projects = [project];
  }
  state.queues = await api(`/api/queues?project_id=${state.projects[0].id}`);
  populateProjectSelects();
}

function populateProjectSelects() {
  const opts = state.queues.map((q) => `<option value="${q.id}">${q.name}</option>`).join("");
  document.querySelectorAll(".queue-select").forEach((sel) => (sel.innerHTML = opts || `<option value="">No queues yet</option>`));
}

// ---------------- Overview ----------------

async function loadOverview() {
  try {
    const health = await api("/api/dashboard/health");
    renderHealth(health);

    const throughput = await api("/api/dashboard/throughput?hours=8");
    const maxVal = Math.max(1, ...throughput.map((b) => b.completed + b.failed));
    document.getElementById("throughput-chart").innerHTML = throughput.map((b) => `
      <div class="chart-col">
        <div class="chart-bars" style="height:${Math.max(4, ((b.completed + b.failed) / maxVal) * 110)}px">
          ${b.failed ? `<div class="chart-bar-failed" style="height:${(b.failed / (b.completed + b.failed || 1)) * 100}%"></div>` : ""}
          ${b.completed ? `<div class="chart-bar-completed" style="height:${(b.completed / (b.completed + b.failed || 1)) * 100}%"></div>` : ""}
        </div>
        <div class="hr-label">${b.hour}</div>
      </div>
    `).join("");
  } catch (err) {
    toast(err.message, "error");
  }
}

function renderHealth(health) {
  const grid = document.getElementById("overview-stats");
  grid.innerHTML = `
    ${statCard("Active Workers", health.active_workers, "accent")}
    ${statCard("Offline Workers", health.offline_workers, health.offline_workers ? "red" : "")}
    ${statCard("Queues", `${health.total_queues}`, "")}
    ${statCard("Paused Queues", health.paused_queues, health.paused_queues ? "amber" : "")}
    ${statCard("Completed / hr", health.jobs_completed_last_hour, "accent")}
    ${statCard("Failed / hr", health.jobs_failed_last_hour, health.jobs_failed_last_hour ? "red" : "")}
    ${statCard("Dead Letter Queue", health.dlq_size, health.dlq_size ? "violet" : "")}
  `;

  document.getElementById("pipeline").innerHTML = `
    ${pipelineStage("queued", "Queued", health.jobs_queued)}
    <div class="pipeline-arrow">→</div>
    ${pipelineStage("running", "Running", health.jobs_running)}
    <div class="pipeline-arrow">→</div>
    ${pipelineStage("completed", "Completed (1h)", health.jobs_completed_last_hour)}
    <div class="pipeline-arrow">→</div>
    ${pipelineStage("dead", "DLQ", health.dlq_size)}
  `;
}

function statCard(label, value, colorClass) {
  return `<div class="stat-card"><div class="label">${label}</div><div class="value ${colorClass}">${value}</div></div>`;
}
function pipelineStage(cls, label, value) {
  return `<div class="pipeline-stage ${cls}"><div class="ring"></div><div class="n">${value}</div><div class="l">${label}</div></div>`;
}

// ---------------- Queues ----------------

async function loadQueuesPage() {
  try {
    state.queues = await api(`/api/queues?project_id=${state.projects[0].id}`);
    populateProjectSelects();
    const rows = await Promise.all(state.queues.map(async (q) => {
      const stats = await api(`/api/queues/${q.id}/stats`);
      return { q, stats };
    }));
    const tbody = document.getElementById("queues-tbody");
    if (rows.length === 0) {
      tbody.innerHTML = `<tr><td colspan="8"><div class="empty">No queues yet — create one to start scheduling jobs.</div></td></tr>`;
      return;
    }
    tbody.innerHTML = rows.map(({ q, stats }) => `
      <tr>
        <td class="mono">${q.name}</td>
        <td>${q.is_paused ? '<span class="badge failed">paused</span>' : '<span class="badge completed">active</span>'}</td>
        <td>${q.priority}</td>
        <td>${q.concurrency_limit}</td>
        <td>${stats.queued}</td>
        <td>${stats.running}</td>
        <td>${stats.completed}</td>
        <td>${stats.dead}</td>
        <td>
          <button class="btn small admin-only" onclick="toggleQueue('${q.id}', ${q.is_paused})">${q.is_paused ? "Resume" : "Pause"}</button>
        </td>
      </tr>
    `).join("");
  } catch (err) {
    toast(err.message, "error");
  }
}

async function toggleQueue(id, isPaused) {
  try {
    await api(`/api/queues/${id}/${isPaused ? "resume" : "pause"}`, { method: "POST" });
    toast(isPaused ? "Queue resumed" : "Queue paused", "success");
    loadQueuesPage();
  } catch (err) {
    toast(err.message, "error");
  }
}

document.getElementById("new-queue-btn").addEventListener("click", () => openModal("modal-new-queue"));

document.getElementById("new-queue-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  try {
    await api("/api/queues", {
      method: "POST",
      body: {
        name: document.getElementById("nq-name").value,
        project_id: state.projects[0].id,
        priority: parseInt(document.getElementById("nq-priority").value || "0"),
        concurrency_limit: parseInt(document.getElementById("nq-concurrency").value || "5"),
        shard_count: parseInt(document.getElementById("nq-shards").value || "1"),
      },
    });
    closeModal("modal-new-queue");
    e.target.reset();
    toast("Queue created", "success");
    loadQueuesPage();
  } catch (err) {
    toast(err.message, "error");
  }
});

// ---------------- Jobs ----------------

async function loadJobsPage() {
  await loadOrganizations(); // ensure queue list fresh for the create-job select
  try {
    let path = `/api/jobs?limit=${state.jobsLimit}&offset=${state.jobsOffset}`;
    if (state.jobsFilter.status) path += `&status=${state.jobsFilter.status}`;
    if (state.jobsFilter.queue_id) path += `&queue_id=${state.jobsFilter.queue_id}`;
    const jobs = await api(path);
    const tbody = document.getElementById("jobs-tbody");
    if (jobs.length === 0) {
      tbody.innerHTML = `<tr><td colspan="7"><div class="empty">No jobs match this filter.</div></td></tr>`;
      return;
    }
    tbody.innerHTML = jobs.map((j) => `
      <tr onclick="openJobDrawer('${j.id}')">
        <td class="mono truncate">${j.id}</td>
        <td>${j.task_name}</td>
        <td><span class="badge ${j.status}">${j.status}</span></td>
        <td>${j.job_type}</td>
        <td>${j.attempt_count}/${j.max_retries}</td>
        <td class="mono">${toLocalDate(j.created_at).toLocaleTimeString()}</td>
        <td>${j.priority}</td>
      </tr>
    `).join("");
  } catch (err) {
    toast(err.message, "error");
  }
}

document.getElementById("job-status-filter").addEventListener("change", (e) => {
  state.jobsFilter.status = e.target.value;
  state.jobsOffset = 0;
  loadJobsPage();
});
document.getElementById("jobs-refresh").addEventListener("click", loadJobsPage);
document.getElementById("jobs-prev").addEventListener("click", () => {
  state.jobsOffset = Math.max(0, state.jobsOffset - state.jobsLimit);
  loadJobsPage();
});
document.getElementById("jobs-next").addEventListener("click", () => {
  state.jobsOffset += state.jobsLimit;
  loadJobsPage();
});

document.getElementById("new-job-btn").addEventListener("click", () => openModal("modal-new-job"));

document.getElementById("nj-type").addEventListener("change", (e) => {
  document.getElementById("nj-cron-field").style.display = e.target.value === "recurring" ? "block" : "none";
  document.getElementById("nj-runat-field").style.display = (e.target.value === "delayed" || e.target.value === "scheduled") ? "block" : "none";
});

document.getElementById("new-job-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  try {
    let payloadJson = {};
    const raw = document.getElementById("nj-payload").value.trim();
    if (raw) payloadJson = JSON.parse(raw);

    const body = {
      queue_id: document.getElementById("nj-queue").value,
      task_name: document.getElementById("nj-task").value,
      payload: payloadJson,
      job_type: document.getElementById("nj-type").value,
      priority: parseInt(document.getElementById("nj-priority").value || "0"),
    };
    const cron = document.getElementById("nj-cron").value.trim();
    if (cron) body.cron_expression = cron;
    const runAt = document.getElementById("nj-runat").value;
    if (runAt) body.run_at = new Date(runAt).toISOString();
    const dependsOnRaw = document.getElementById("nj-depends-on").value.trim();
    if (dependsOnRaw) body.depends_on = dependsOnRaw.split(",").map((s) => s.trim()).filter(Boolean);

    await api("/api/jobs", { method: "POST", body });
    closeModal("modal-new-job");
    e.target.reset();
    toast("Job submitted", "success");
    loadJobsPage();
  } catch (err) {
    toast(err.message, "error");
  }
});

async function openJobDrawer(jobId) {
  try {
    const [job, logs, executions] = await Promise.all([
      api(`/api/jobs/${jobId}`),
      api(`/api/jobs/${jobId}/logs`),
      api(`/api/jobs/${jobId}/executions`),
    ]);
    document.getElementById("drawer-title").textContent = job.id;
    document.getElementById("drawer-body").innerHTML = `
      <div class="pill-row" style="margin-bottom:14px;">
        <span class="badge ${job.status}">${job.status}</span>
        <span class="pill">${job.task_name}</span>
        <span class="pill">${job.job_type}</span>
        <span class="pill">attempt ${job.attempt_count}/${job.max_retries}</span>
      </div>

      <div class="section-label">Payload</div>
      <pre class="mono" style="white-space:pre-wrap; background:var(--bg-2); padding:10px; border-radius:6px; font-size:11.5px;">${escapeHtml(JSON.stringify(job.payload, null, 2))}</pre>

      ${job.result ? `<div class="section-label">Result</div><pre class="mono" style="white-space:pre-wrap; background:var(--bg-2); padding:10px; border-radius:6px; font-size:11.5px;">${escapeHtml(JSON.stringify(job.result, null, 2))}</pre>` : ""}
      ${job.error_message ? `<div class="section-label">Last error</div><div class="mono" style="color:var(--red); font-size:12px;">${escapeHtml(job.error_message)}</div>` : ""}

      <div class="section-label">Executions (${executions.length})</div>
      ${executions.map((ex) => `
        <div class="log-line">
          <span class="ts">#${ex.attempt_number}</span>
          <span class="msg"><span class="badge ${ex.status}">${ex.status}</span> ${ex.duration_ms ? ex.duration_ms + "ms" : ""} ${ex.error_message ? "— " + escapeHtml(ex.error_message) : ""}</span>
        </div>
      `).join("") || '<div class="empty">No executions yet</div>'}

      <div class="section-label">Logs</div>
      ${logs.map((l) => `
        <div class="log-line ${l.level}">
          <span class="ts">${toLocalDate(l.created_at).toLocaleTimeString()}</span>
          <span class="msg">${escapeHtml(l.message)}</span>
        </div>
      `).join("") || '<div class="empty">No logs yet</div>'}

      <div class="modal-actions" style="justify-content:flex-start; margin-top:20px;">
        ${["failed", "dead"].includes(job.status) ? `<button class="btn primary small" onclick="retryJob('${job.id}')">Retry job</button>` : ""}
        ${!["completed", "running", "dead"].includes(job.status) ? `<button class="btn danger small" onclick="cancelJob('${job.id}')">Cancel job</button>` : ""}
      </div>
    `;
    document.getElementById("drawer-overlay").classList.add("open");
  } catch (err) {
    toast(err.message, "error");
  }
}

async function retryJob(id) {
  try {
    await api(`/api/jobs/${id}/retry`, { method: "POST" });
    toast("Job requeued", "success");
    closeDrawer();
    loadJobsPage();
  } catch (err) { toast(err.message, "error"); }
}
async function cancelJob(id) {
  try {
    await api(`/api/jobs/${id}/cancel`, { method: "POST" });
    toast("Job cancelled", "success");
    closeDrawer();
    loadJobsPage();
  } catch (err) { toast(err.message, "error"); }
}

function closeDrawer() { document.getElementById("drawer-overlay").classList.remove("open"); }
document.getElementById("drawer-overlay").addEventListener("click", (e) => {
  if (e.target.id === "drawer-overlay") closeDrawer();
});
document.getElementById("drawer-close").addEventListener("click", closeDrawer);

// ---------------- Workers ----------------

async function loadWorkersPage() {
  try {
    const workers = await api("/api/workers");
    const tbody = document.getElementById("workers-tbody");
    if (workers.length === 0) {
      tbody.innerHTML = `<tr><td colspan="7"><div class="empty">No workers registered. Start one with:<br><code class="mono">python -m app.worker --name worker-1</code></div></td></tr>`;
      return;
    }
    tbody.innerHTML = workers.map((w) => `
      <tr>
        <td class="mono">${w.name}</td>
        <td><span class="badge ${w.status}">${w.status}</span></td>
        <td>${w.concurrency}</td>
        <td>${w.shard_id !== null && w.shard_id !== undefined ? w.shard_id : "all"}</td>
        <td>${w.queues.length ? w.queues.length + " queue(s)" : "all queues"}</td>
        <td class="mono">${toLocalDate(w.started_at).toLocaleString()}</td>
        <td class="mono">${toLocalDate(w.last_seen_at).toLocaleTimeString()}</td>
      </tr>
    `).join("");
  } catch (err) {
    toast(err.message, "error");
  }
}

// ---------------- Dead Letter Queue ----------------

async function loadDlqPage() {
  try {
    const entries = await api("/api/dead-letter-queue");
    const tbody = document.getElementById("dlq-tbody");
    if (entries.length === 0) {
      tbody.innerHTML = `<tr><td colspan="5"><div class="empty">Dead Letter Queue is empty. 🎉</div></td></tr>`;
      return;
    }
    tbody.innerHTML = entries.map((entry) => `
      <tr>
        <td class="mono truncate">${entry.job_id}</td>
        <td class="truncate" title="${escapeHtml(entry.ai_summary || entry.reason)}">${escapeHtml(entry.ai_summary || entry.reason)}</td>
        <td class="mono">${toLocalDate(entry.moved_at).toLocaleString()}</td>
        <td>${entry.replayed ? '<span class="badge completed">replayed</span>' : '<span class="badge dead">pending</span>'}</td>
        <td>${!entry.replayed ? `<button class="btn small primary admin-only" onclick="replayDlq('${entry.id}')">Replay</button>` : ""}</td>
      </tr>
    `).join("");
  } catch (err) {
    toast(err.message, "error");
  }
}

async function replayDlq(id) {
  try {
    await api(`/api/dead-letter-queue/${id}/replay`, { method: "POST" });
    toast("Job replayed from DLQ", "success");
    loadDlqPage();
  } catch (err) { toast(err.message, "error"); }
}

// ---------------- Modal helpers ----------------

function openModal(id) { document.getElementById(id).classList.add("open"); }
function closeModal(id) { document.getElementById(id).classList.remove("open"); }
document.querySelectorAll(".overlay").forEach((ov) => {
  ov.addEventListener("click", (e) => { if (e.target === ov) ov.classList.remove("open"); });
});
document.querySelectorAll("[data-close-modal]").forEach((btn) => {
  btn.addEventListener("click", () => closeModal(btn.dataset.closeModal));
});

function escapeHtml(str) {
  return String(str).replace(/[&<>"']/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m]));
}

function toLocalDate(isoString) {
  // Backend sends naive UTC timestamps without a timezone suffix (e.g.
  // "2026-07-04T09:58:00.123"). JavaScript's Date treats that as *local*
  // time if there's no "Z", causing displayed times to be wrong by your
  // UTC offset. Appending "Z" (if missing) fixes it.
  if (isoString && !isoString.endsWith("Z") && !isoString.includes("+")) {
    isoString += "Z";
  }
  return new Date(isoString);
}

// ---------------- Auto-refresh ----------------

setInterval(() => {
  const activePage = document.querySelector(".page.active");
  if (!activePage || !state.token) return;
  if (activePage.id === "page-overview") loadOverview();
  if (activePage.id === "page-workers") loadWorkersPage();
}, 8000);

// ---------------- Init ----------------

boot();