/* DataFlow OS — frontend application */
'use strict';

/* ═══════════════════════════════════════════════════
   STATE
═══════════════════════════════════════════════════ */
const API = '';
let ws = null, wsRetries = 0;
let metricsHistory = { rows: [], latency: [] };
let metricsTimer   = null;
let analyticsState = { tables: [], currentRows: [], currentCols: [], charts: [] };
let lastStatsReport = null;

/* Auth */
let jwtToken = sessionStorage.getItem('dfo_jwt') || null;
let isLoggedIn = !!jwtToken;

/* Pipeline builder */
const pb = { steps: [], editId: null, max_retries: 3, retry_delay_sec: 30, webhook_url: '', webhook_on: 'failure', alert_cooldown: 300 };

/* User preferences */
let prefs = {};

/* ═══════════════════════════════════════════════════
   AIRBYTE — pre-defined source catalog
   Used by the pipeline builder when connector_type === 'airbyte'.
   Versions pinned to known-good as of 2026-05.
═══════════════════════════════════════════════════ */
const AIRBYTE_CATALOG = {
  /* databases */
  'PostgreSQL':     { image: 'airbyte/source-postgres:3.6.16',         category: 'database' },
  'MySQL':          { image: 'airbyte/source-mysql:3.7.4',             category: 'database' },
  'MongoDB v2':     { image: 'airbyte/source-mongodb-v2:1.5.0',        category: 'database' },
  'MS SQL Server':  { image: 'airbyte/source-mssql:4.0.31',            category: 'database' },
  'Oracle':         { image: 'airbyte/source-oracle:0.5.4',            category: 'database' },
  'Snowflake':      { image: 'airbyte/source-snowflake:0.3.1',         category: 'database' },
  /* payments / finance */
  'Stripe':         { image: 'airbyte/source-stripe:5.5.0',            category: 'payments' },
  'PayPal':         { image: 'airbyte/source-paypal-transaction:2.5.5', category: 'payments' },
  /* CRM / sales */
  'HubSpot':        { image: 'airbyte/source-hubspot:4.4.5',           category: 'crm' },
  'Salesforce':     { image: 'airbyte/source-salesforce:2.5.20',       category: 'crm' },
  'Pipedrive':      { image: 'airbyte/source-pipedrive:2.2.0',         category: 'crm' },
  'Intercom':       { image: 'airbyte/source-intercom:0.8.5',          category: 'crm' },
  /* marketing */
  'Mailchimp':      { image: 'airbyte/source-mailchimp:2.0.16',        category: 'marketing' },
  'SendGrid':       { image: 'airbyte/source-sendgrid:1.1.4',          category: 'marketing' },
  'Facebook Ads':   { image: 'airbyte/source-facebook-marketing:3.3.10', category: 'marketing' },
  'Google Ads':     { image: 'airbyte/source-google-ads:3.7.4',        category: 'marketing' },
  /* analytics */
  'Google Analytics': { image: 'airbyte/source-google-analytics-data-api:2.6.0', category: 'analytics' },
  'Mixpanel':       { image: 'airbyte/source-mixpanel:2.5.0',          category: 'analytics' },
  'Amplitude':      { image: 'airbyte/source-amplitude:0.4.6',         category: 'analytics' },
  'Segment':        { image: 'airbyte/source-segment:0.1.0',           category: 'analytics' },
  /* commerce */
  'Shopify':        { image: 'airbyte/source-shopify:2.4.10',          category: 'ecommerce' },
  'WooCommerce':    { image: 'airbyte/source-woocommerce:0.4.0',       category: 'ecommerce' },
  'Square':         { image: 'airbyte/source-square:1.6.6',            category: 'ecommerce' },
  /* dev / ops */
  'GitHub':         { image: 'airbyte/source-github:1.7.3',            category: 'dev' },
  'GitLab':         { image: 'airbyte/source-gitlab:3.0.4',            category: 'dev' },
  'Jira':           { image: 'airbyte/source-jira:1.7.5',              category: 'dev' },
  'Linear':         { image: 'airbyte/source-linear:0.4.0',            category: 'dev' },
  /* comms */
  'Slack':          { image: 'airbyte/source-slack:1.4.0',             category: 'comms' },
  'Notion':         { image: 'airbyte/source-notion:2.2.6',            category: 'comms' },
  'Zendesk':        { image: 'airbyte/source-zendesk-support:4.7.4',   category: 'comms' },
  /* dev sample */
  'Faker (test)':   { image: 'airbyte/source-faker:6.2.6',             category: 'sample' },
};
function airbyteCategoryColor(cat) {
  return ({database:'#5b6ef5',payments:'#34d399',crm:'#fbbf24',marketing:'#f87171',
          analytics:'#22d3ee',ecommerce:'#a78bfa',dev:'#94a3b8',comms:'#fb7185',
          sample:'#94a3b8'})[cat] || '#94a3b8';
}


/* ═══════════════════════════════════════════════════
   PREFERENCES
═══════════════════════════════════════════════════ */
function loadPrefs() {
  try { prefs = JSON.parse(localStorage.getItem('dfo_prefs') || '{}'); } catch (_) { prefs = {}; }
  prefs.autoRefresh      = prefs.autoRefresh      ?? true;
  prefs.refreshInterval  = prefs.refreshInterval  ?? 30;
  prefs.rowLimit         = prefs.rowLimit          ?? 100;
  prefs.compact          = prefs.compact           ?? false;
}

function savePref(key, val) {
  prefs[key] = val;
  localStorage.setItem('dfo_prefs', JSON.stringify(prefs));
}

function applyPrefs() {
  document.body.classList.toggle('compact', !!prefs.compact);
}

function applyPrefsToSettingsForm() {
  const el = (id) => document.getElementById(id);
  el('pref-auto-refresh').checked   = prefs.autoRefresh;
  el('pref-refresh-interval').value = prefs.refreshInterval;
  el('pref-compact').checked        = prefs.compact;
  const rl = el('pref-row-limit');
  if (rl) for (const o of rl.options) o.selected = (+o.value === prefs.rowLimit);
}


/* ═══════════════════════════════════════════════════
   NAVIGATION
═══════════════════════════════════════════════════ */
document.querySelectorAll('.nav-item').forEach(a => {
  a.addEventListener('click', e => { e.preventDefault(); switchView(a.dataset.view); });
});

const VALID_VIEWS = new Set(['pipelines','builder','analytics','matviews','security','metrics','settings']);

function switchView(name, { pushState = true } = {}) {
  if (!VALID_VIEWS.has(name)) name = 'pipelines';

  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  const navEl = document.querySelector(`[data-view="${name}"]`);
  if (navEl) navEl.classList.add('active');
  const viewEl = document.getElementById('view-' + name);
  if (viewEl) viewEl.classList.add('active');

  /* hide builder nav item when not editing */
  const nb = document.getElementById('nav-builder');
  if (name !== 'builder') nb.classList.remove('visible');

  if (name === 'pipelines') loadPipelinesView();
  if (name === 'analytics') loadAnalyticsModule();
  if (name === 'matviews')  loadMatviews();
  if (name === 'security')  { loadRbacPolicies(); }
  if (name === 'metrics')   loadMetrics();
  if (name === 'settings')  loadSettings();

  if (pushState && location.hash !== '#' + name)
    history.pushState({ view: name }, '', '#' + name);
}

/* Pipelines view now also hosts the tables panel (formerly a separate view). */
function loadPipelinesView() {
  loadPipelines();
  loadQuerySidebar();
}

window.addEventListener('popstate', e => {
  const { view, id } = parseHash();
  if (view === 'builder' && id) { restoreBuilderFromHash(id); return; }
  switchView(view || 'pipelines', { pushState: false });
});


/* ═══════════════════════════════════════════════════
   WEBSOCKET
═══════════════════════════════════════════════════ */
function connectWS() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const host  = location.hostname || 'localhost';
  const port  = location.port     || '8080';
  ws = new WebSocket(`${proto}://${host}:${port}/ws`);

  ws.onopen = () => {
    wsRetries = 0;
    setBadge('ok', 'подключено');
    logActivity('WebSocket подключён');
  };
  ws.onclose = () => {
    setBadge('warn', 'переподключение…');
    const delay = Math.min(1000 * Math.pow(1.5, wsRetries++), 15000);
    setTimeout(connectWS, delay);
  };
  ws.onerror = () => setBadge('err', 'ошибка ws');
  ws.onmessage = e => {
    try { handleWsEvent(JSON.parse(e.data)); } catch (_) {}
  };
}

function handleWsEvent(msg) {
  const ev = msg.event || msg.type || '';
  logActivity(ev + (msg.table ? ` — ${msg.table}` : '') + (msg.id ? ` — ${msg.id}` : ''));
  if (ev === 'table_updated')        { loadQuerySidebar(); }
  if (ev === 'pipeline_created')     { loadPipelines(); }
  if (ev === 'pipeline_run_started') { loadPipelines(); }
  if (ev === 'pipeline_triggered')   { loadPipelines(); }
}

function setBadge(cls, text) {
  const el = document.getElementById('ws-status');
  el.className   = 'badge badge-' + cls;
  el.textContent = text;
}


/* ═══════════════════════════════════════════════════
   TOAST NOTIFICATIONS
═══════════════════════════════════════════════════ */
function showToast(msg, type = 'info') {
  const container = document.getElementById('toast-container');
  const toast = document.createElement('div');
  const icons = { ok: '✓', error: '✕', warn: '⚠', info: 'ℹ' };
  toast.className = `toast toast-${type}`;
  toast.innerHTML = `<span class="toast-icon">${icons[type] || ''}</span>${escHtml(msg)}`;
  container.appendChild(toast);
  requestAnimationFrame(() => requestAnimationFrame(() => toast.classList.add('show')));
  setTimeout(() => {
    toast.classList.remove('show');
    setTimeout(() => toast.remove(), 300);
  }, 3500);
}


/* ═══════════════════════════════════════════════════
   ACTIVITY LOG
═══════════════════════════════════════════════════ */
function logActivity(msg) {
  const box = document.getElementById('activity-log');
  if (!box) return;
  if (box.textContent === 'Событий пока нет.') box.innerHTML = '';
  const row = document.createElement('div');
  row.className = 'log-entry';
  row.innerHTML = `<span class="log-time">${new Date().toLocaleTimeString('ru')}</span><span class="log-msg">${escHtml(msg)}</span>`;
  box.prepend(row);
  while (box.children.length > 100) box.removeChild(box.lastChild);
}


/* ═══════════════════════════════════════════════════
   TABLES + QUERY
═══════════════════════════════════════════════════ */
async function loadQuerySidebar() {
  const list = document.getElementById('query-table-list');
  if (!list) return;
  list.innerHTML = '<div class="empty-state" style="padding:1rem;color:var(--muted)">Загрузка…</div>';
  try {
    const tables = await apiFetch('/api/tables');
    if (!tables) return;
    list.innerHTML = '';
    if (!tables.length) {
      list.innerHTML = '<div class="empty-state">Таблиц пока нет.<br>Перейдите в раздел <strong>Загрузка</strong>, чтобы импортировать CSV.</div>';
      return;
    }
    const ingested = tables.filter(t => t.source !== 'pipeline');
    const pipeline = tables.filter(t => t.source === 'pipeline');

    function renderGroup(label, items) {
      if (!items.length) return;
      const hdr = document.createElement('div');
      hdr.className = 'tables-group-header';
      hdr.textContent = label;
      list.appendChild(hdr);
      const grid = document.createElement('div');
      grid.className = 'tables-group-grid';
      items.forEach(t => grid.appendChild(makeTableCard(t)));
      list.appendChild(grid);
    }

    renderGroup('Загруженные данные', ingested);
    renderGroup('Созданные конвейером', pipeline);
  } catch (err) {
    list.innerHTML = `<div style="color:var(--red)">Error: ${escHtml(String(err))}</div>`;
  }
}

function makeTableCard(t) {
  const card = document.createElement('div');
  card.className = 'table-card';
  const cols = (t.columns || []).map(c => {
    const cls = c.type === 'int64' ? 'int' : c.type === 'double' ? 'double' : c.type === 'bool' ? 'bool' : '';
    return `<span class="col-pill ${cls}">${escHtml(c.name)}<em style="opacity:.5">:${c.type}</em></span>`;
  }).join('');
  const sourceBadge = t.source === 'pipeline'
    ? '<span class="source-badge pipeline">конвейер</span>'
    : t.source === 'parquet'
      ? '<span class="source-badge parquet">Parquet</span>'
      : '<span class="source-badge ingest">CSV</span>';
  card.innerHTML = `
    <div class="table-card-head">
      <h3>${escHtml(t.name)}${sourceBadge}</h3>
      <button class="btn btn-sm btn-danger" title="Удалить таблицу">Удалить</button>
    </div>
    <div class="meta">${fmtNum(t.rows || 0)} строк · ${(t.columns || []).length} столбцов <button class="btn btn-sm idx-btn" onclick="openIndexManager('${escAttr(t.name)}', event)" title="Индексы">⚡ Индексы</button></div>
    <div class="col-list">${cols || '<span class="col-pill">—</span>'}</div>
  `;
  const delBtn = card.querySelector('button');
  delBtn.addEventListener('click', (ev) => {
    ev.stopPropagation();
    deleteTable(t.name);
  });
  card.onclick = () => openTablePreview(t.name);
  return card;
}

/* Run a single step in isolation, without saving the parent pipeline.
 *
 * Strategy: build a one-shot transient pipeline with this step + a synthetic
 * target_table (`__preview_xxx`), trigger it via /api/pipelines/:id/run, query
 * the result, and clean up. Works uniformly for SQL, Python, and connector
 * steps because we reuse the actual pipeline runner.
 *
 * opts.save = true   → write into the step's real target_table and keep it
 * opts.save = false  → write into a temp table, return rows, delete the temp
 */
/* Run ONLY this step's transform_sql against current tables — no upstream
 * chain, no connector, no python. Result renders in the inline preview panel
 * below the card. Uses /api/tables/query directly (fastest path). */
async function runStepSQL(idx) {
  const step = pb.steps[idx];
  const sql = (step?.transform_sql || '').trim();
  const panel = document.getElementById(`step-preview-${idx}`);
  if (!panel) return;
  panel.style.display = '';
  if (!sql) {
    panel.innerHTML = '<div style="padding:.6rem;color:var(--amber)">SQL пустой — нечего запускать</div>';
    return;
  }
  panel.innerHTML = '<div style="padding:.6rem;color:var(--muted)">Выполняется SQL…</div>';
  const t0 = performance.now();
  try {
    const data = await apiPost('/api/tables/query', { sql });
    if (!data) { panel.innerHTML = '<div style="padding:.6rem;color:var(--red)">Сервер вернул undefined</div>'; return; }
    const ms = (performance.now() - t0).toFixed(1);
    const rows = data.rows || [];
    panel.innerHTML = '';
    const status = document.createElement('div');
    status.style.cssText = 'font-size:.78rem;color:var(--muted);margin-bottom:.4rem';
    status.innerHTML = `▶ SQL — ${rows.length} ${pluralRows(rows.length)} · ${ms} мс`;
    panel.appendChild(status);
    panel.appendChild(makeResultTable(data));
  } catch (err) {
    let msg = String(err);
    try { const j = JSON.parse(msg.replace(/^Error:\s*/,'')); if (j.error) msg = j.error; } catch(_) {}
    panel.innerHTML = `<div style="padding:.75rem;color:var(--red);background:var(--surface);border:1px dashed var(--border);border-radius:var(--radius);font-family:var(--mono);font-size:.78rem">${escHtml(msg)}</div>`;
  }
}

/* Run ONLY this step's python_code (with its transform_sql as input data) —
 * no upstream chain. Single-step POST to /api/pipelines/preview-step. */
async function runStepPython(idx) {
  const step = pb.steps[idx];
  const panel = document.getElementById(`step-preview-${idx}`);
  if (!panel) return;
  panel.style.display = '';
  if (!step?.python_code && !step?.python_file) {
    panel.innerHTML = '<div style="padding:.6rem;color:var(--amber)">Python-код / python_file пустые</div>';
    return;
  }
  panel.innerHTML = '<div style="padding:.6rem;color:var(--muted)">Выполняется Python…</div>';

  const payload = {
    name: 'preview',
    enabled: false,
    steps: [{
      id:                 'preview',
      name:               step.name || 'preview',
      connector_type:     '',
      connector_config:   '',
      transform_sql:      step.transform_sql || '',
      python_code:        step.python_code || '',
      python_file:        step.python_file || '',
      python_context_dir: step.python_context_dir || '',
      python_timeout_sec: step.python_timeout_sec || 300,
      target_table:       '',
      max_retries:        0,
      retry_delay_sec:    1,
      deps:               [],
    }],
  };
  const limit = prefs.rowLimit || 100;
  const t0 = performance.now();
  try {
    const data = await apiPost(`/api/pipelines/preview-step?save=0&limit=${limit}`, payload);
    if (!data) { panel.innerHTML = '<div style="padding:.6rem;color:var(--red)">Сервер вернул undefined</div>'; return; }
    const ms = (performance.now() - t0).toFixed(1);
    const rows = data.rows || [];
    panel.innerHTML = '';
    const status = document.createElement('div');
    status.style.cssText = 'font-size:.78rem;color:var(--muted);margin-bottom:.4rem';
    status.innerHTML = `▶ Python — ${rows.length} ${pluralRows(rows.length)} · ${ms} мс`;
    panel.appendChild(status);
    panel.appendChild(makeResultTable(data));
  } catch (err) {
    let msg = String(err);
    try { const j = JSON.parse(msg.replace(/^Error:\s*/,'')); if (j.error) msg = j.error; } catch(_) {}
    panel.innerHTML = `<div style="padding:.75rem;color:var(--red);background:var(--surface);border:1px dashed var(--border);border-radius:var(--radius);font-family:var(--mono);font-size:.78rem">${escHtml(msg)}</div>`;
  }
}

/* Run ONLY this step's scala_code (with its transform_sql as input data) —
 * no upstream chain. Single-step POST to /api/pipelines/preview-step. */
async function runStepScala(idx) {
  const step = pb.steps[idx];
  const panel = document.getElementById(`step-preview-${idx}`);
  if (!panel) return;
  panel.style.display = '';
  if (!step?.scala_code) {
    panel.innerHTML = '<div style="padding:.6rem;color:var(--amber)">Scala-код пустой</div>';
    return;
  }
  panel.innerHTML = '<div style="padding:.6rem;color:var(--muted)">Выполняется Scala (Spark прогревается)…</div>';

  const payload = {
    name: 'preview',
    enabled: false,
    steps: [{
      id:                'preview',
      name:              step.name || 'preview',
      connector_type:    '',
      connector_config:  '',
      transform_sql:     step.transform_sql || '',
      scala_code:        step.scala_code,
      scala_timeout_sec: step.scala_timeout_sec || 600,
      target_table:      '',
      max_retries:       0,
      retry_delay_sec:   1,
      deps:              [],
    }],
  };
  const limit = prefs.rowLimit || 100;
  const t0 = performance.now();
  try {
    const data = await apiPost(`/api/pipelines/preview-step?save=0&limit=${limit}`, payload);
    if (!data) { panel.innerHTML = '<div style="padding:.6rem;color:var(--red)">Сервер вернул undefined</div>'; return; }
    const ms = (performance.now() - t0).toFixed(1);
    const rows = data.rows || [];
    panel.innerHTML = '';
    const status = document.createElement('div');
    status.style.cssText = 'font-size:.78rem;color:var(--muted);margin-bottom:.4rem';
    status.innerHTML = `▶ Scala — ${rows.length} ${pluralRows(rows.length)} · ${ms} мс`;
    panel.appendChild(status);
    panel.appendChild(makeResultTable(data));
  } catch (err) {
    let msg = String(err);
    try { const j = JSON.parse(msg.replace(/^Error:\s*/,'')); if (j.error) msg = j.error; } catch(_) {}
    panel.innerHTML = `<div style="padding:.75rem;color:var(--red);background:var(--surface);border:1px dashed var(--border);border-radius:var(--radius);font-family:var(--mono);font-size:.78rem">${escHtml(msg)}</div>`;
  }
}

async function runStepPreview(idx, opts = {}) {
  const save = !!opts.save;
  const step = pb.steps[idx];

  /* In save mode, persist the entire builder state first — otherwise the
   * user's SQL/Python edits in the textareas would be lost. The data write
   * to target_table comes second so the pipeline JSON matches what produced
   * the rows. */
  if (save) {
    try {
      const created = await persistBuilderPipeline();
      if (!created) return;
    } catch (err) {
      const panel = document.getElementById(`step-preview-${idx}`);
      if (panel) {
        panel.style.display = '';
        panel.innerHTML = `<div style="padding:.75rem;color:var(--red);font-family:var(--mono);font-size:.78rem">Ошибка сохранения конвейера: ${escHtml(String(err))}</div>`;
      }
      return;
    }
  }

  /* Show the panel up-front so any failure path is visible to the user. */
  const panel = document.getElementById(`step-preview-${idx}`);
  if (!panel) { console.error(`runStepPreview: no panel for idx=${idx}`); return; }
  panel.style.display = '';
  const setStatus = (msg, color) => {
    panel.innerHTML = `<div style="padding:.6rem;color:${color || 'var(--muted)'};font-family:var(--mono);font-size:.78rem">${msg}</div>`;
  };

  if (!step) { setStatus(`step #${idx} не найден в pb.steps (length=${pb.steps.length})`, 'var(--red)'); return; }
  if (!step.transform_sql && !step.python_code && !step.python_file && !step.scala_code &&
      !step.connector_type && !step.scd2_business_key && !step.match_rules) {
    setStatus('Шаг пустой — заполни SQL, Python, Scala, коннектор, SCD2 или Match-rules', 'var(--amber)');
    return;
  }
  if (save && !step.target_table) {
    setStatus('Укажи target_table — иначе некуда сохранять', 'var(--amber)');
    return;
  }
  setStatus(save
    ? `Сохранение шага ${idx + 1}${idx > 0 ? ` (+ ${idx} upstream)` : ''}…`
    : `Запуск шага ${idx + 1}${idx > 0 ? ` (+ ${idx} upstream)` : ''}…`);

  const limit = prefs.rowLimit || 100;

  /* Run every step up to AND INCLUDING the clicked one. Without this, a step
   * that reads from an upstream target_table (e.g. SELECT ... FROM clean_employees)
   * would find that table empty because step 0/1/… never ran. The backend
   * overrides only the LAST step's target_table for save=0, so intermediate
   * tables are written for real — same as running the pipeline up to here. */
  const chain = pb.steps.slice(0, idx + 1).map((s, i) => ({
    ...stepForApi(s),               /* carries scd2_ and any new fields through */
    id:                 s.id || `s${i}`,
    name:               s.name || `step ${i + 1}`,
    connector_type:     s.connector_type || '',
    connector_config:   s.connector_config || '',
    transform_sql:      s.transform_sql || '',
    python_code:        s.python_code || '',
    python_timeout_sec: s.python_timeout_sec || 300,
    scala_code:         s.scala_code || '',
    scala_timeout_sec:  s.scala_timeout_sec || 600,
    target_table:       s.target_table || '',
    max_retries:        0,
    retry_delay_sec:    1,
    deps:               [],
  }));

  const payload = { name: 'preview', enabled: false, steps: chain };

  try {
    /* Single-call flow: server runs the chain in-memory, returns rows
     * directly. For save=0 the server transparently uses a temp table that
     * is dropped before the response is sent — no client-side cleanup needed. */
    const url = `/api/pipelines/preview-step?save=${save ? 1 : 0}&limit=${limit}`;
    console.log('[step preview] POST', url, payload);
    const data = await apiPost(url, payload);
    if (!data) {
      setStatus('Сервер вернул пустой ответ', 'var(--red)');
      return;
    }
    console.log('[step preview] result:', data);
    const rows = data.rows || [];
    const cols = data.columns || [];
    const outTable = data.target_table || step.target_table || '';
    panel.innerHTML = '';

    const status = document.createElement('div');
    status.style.cssText = 'font-size:.78rem;color:var(--muted);margin-bottom:.4rem';
    status.innerHTML = save
      ? `✓ Сохранено в <strong>${escHtml(outTable)}</strong> — ${rows.length} ${pluralRows(rows.length)} (показаны первые ${limit})`
      : `Превью — ${rows.length} ${pluralRows(rows.length)} (LIMIT ${limit})`;
    panel.appendChild(status);

    if (rows.length === 0) {
      /* Diagnostic: parse FROM/JOIN out of transform_sql, fetch /api/tables,
       * show which source tables are empty — that's the usual reason. */
      const sql = (step.transform_sql || '').toLowerCase();
      const sources = [...new Set([
        ...[...sql.matchAll(/\bfrom\s+([a-z_][a-z0-9_]*)/g)].map(m => m[1]),
        ...[...sql.matchAll(/\bjoin\s+([a-z_][a-z0-9_]*)/g)].map(m => m[1]),
      ])];

      let sourcesReport = '';
      if (sources.length) {
        try {
          const tbls = await apiFetch('/api/tables');
          const counts = sources.map(s => {
            const t = (tbls || []).find(x => x.name === s);
            return { name: s, rows: t ? (t.rows || 0) : null };
          });
          sourcesReport = `<div style="margin-top:.6rem;text-align:left">Источники в SQL:<br>${
            counts.map(c => c.rows === null
              ? `&nbsp;&nbsp;<code>${escHtml(c.name)}</code> <span style="color:var(--red)">— нет такой таблицы</span>`
              : `&nbsp;&nbsp;<code>${escHtml(c.name)}</code> — <strong>${fmtNum(c.rows)}</strong> ${pluralRows(c.rows)}${c.rows===0?' <span style="color:var(--amber)">⚠ пусто</span>':''}`
            ).join('<br>')
          }</div>`;
        } catch (_) { /* skip diagnostic on auth issues */ }
      }

      const empty = document.createElement('div');
      empty.style.cssText = 'padding:1rem;color:var(--muted);background:var(--surface);border:1px dashed var(--border);border-radius:var(--radius)';
      const colsHint = cols.length
        ? `<div style="text-align:center">Схема результата: <code>${cols.map(escHtml).join(', ')}</code></div>`
        : '';
      empty.innerHTML = `${colsHint}
        <div style="text-align:center;margin-top:.5rem">Шаг отработал, но вернул <strong>0 строк</strong>.</div>
        ${sourcesReport}`;
      panel.appendChild(empty);
    } else {
      panel.appendChild(makeResultTable(data));
    }

    if (save) {
      showToast(`Таблица «${outTable}» обновлена`, 'ok');
      loadQuerySidebar();
    }
    /* No client-side cleanup: server already dropped the temp table for save=0. */
  } catch (err) {
    let msg = String(err);
    try { const j = JSON.parse(msg.replace(/^Error:\s*/,'')); if (j.error) msg = j.error; } catch(_) {}
    panel.innerHTML = `<div style="padding:.75rem;color:var(--red);background:var(--surface);border:1px dashed var(--border);border-radius:var(--radius);font-family:var(--mono);font-size:.78rem">${escHtml(msg)}</div>`;
  }
}

/* Preview the first N rows of a table in a modal (replaces the old Query view's
 * inline SQL editor flow). For ad-hoc SQL, use the Analytics page. */
async function openTablePreview(name) {
  const modal = document.getElementById('table-preview-modal');
  if (!modal) return;
  document.getElementById('table-preview-title').textContent = `Таблица: ${name}`;
  const status = document.getElementById('table-preview-status');
  const body   = document.getElementById('table-preview-body');
  const limit  = prefs.rowLimit || 100;
  status.textContent = 'Загрузка…';
  body.innerHTML = '';
  modal.classList.remove('hidden');
  const t0 = performance.now();
  try {
    const data = await apiPost('/api/tables/query', { sql: `SELECT * FROM ${name} LIMIT ${limit}` });
    const ms = (performance.now() - t0).toFixed(1);
    const rows = data.rows || [];
    status.textContent = `${rows.length} ${pluralRows(rows.length)} · ${ms} мс · LIMIT ${limit}`;
    body.appendChild(makeResultTable(data));
  } catch (err) {
    status.textContent = 'Ошибка';
    body.innerHTML = `<div style="color:var(--red);padding:.5rem;font-family:monospace">${escHtml(String(err))}</div>`;
  }
}

async function openIndexManager(tableName, ev) {
  ev.stopPropagation();
  const modal = document.getElementById('index-modal');
  document.getElementById('index-modal-title').textContent = `Индексы: ${tableName}`;
  document.getElementById('index-modal-table').value = tableName;
  document.getElementById('index-modal-col').value = '';
  document.getElementById('index-modal-status').textContent = '';
  modal.classList.remove('hidden');
  await loadTableIndexes(tableName);
}

async function loadTableIndexes(tableName) {
  const list = document.getElementById('index-modal-list');
  list.innerHTML = '<span style="color:var(--muted)">Загрузка…</span>';
  try {
    const data = await apiFetch(`/api/tables/${encodeURIComponent(tableName)}/indexes`);
    if (!data.length) { list.innerHTML = '<span style="color:var(--muted)">Индексов нет</span>'; return; }
    list.innerHTML = data.map(ix => `<span class="col-pill int">⚡ ${escHtml(ix.column)}</span>`).join(' ');
  } catch (_) { list.innerHTML = '<span style="color:var(--muted)">—</span>'; }
}

async function createIndex() {
  const table = document.getElementById('index-modal-table').value;
  const col   = document.getElementById('index-modal-col').value.trim();
  if (!col) { showToast('Укажите колонку', 'warn'); return; }
  const status = document.getElementById('index-modal-status');
  status.textContent = '…';
  try {
    await apiPost(`/api/tables/${encodeURIComponent(table)}/indexes`, { column: col });
    status.innerHTML = '<span style="color:var(--green)">Индекс создан</span>';
    await loadTableIndexes(table);
    showToast(`Индекс по ${col} создан`, 'ok');
  } catch (err) {
    status.innerHTML = `<span style="color:var(--red)">${escHtml(String(err))}</span>`;
  }
}

async function deleteTable(name) {
  if (!confirm(`Удалить таблицу "${name}"? Это действие нельзя отменить.`)) return;
  try {
    await apiFetch(`/api/tables/${encodeURIComponent(name)}`, 'DELETE');
    showToast(`Таблица "${name}" удалена`, 'ok');
    logActivity(`удалена таблица ${name}`);
    await loadQuerySidebar();
  } catch (err) {
    showToast(`Ошибка удаления: ${err.message || err}`, 'err');
  }
}

/* SQL editor moved to the Analytics page; standalone Query view was removed. */

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('an-view-sql')?.addEventListener('keydown', e => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); runViewQuery(); }
  });
  document.getElementById('an-sql')?.addEventListener('keydown', e => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); runAnalyticsQuery(); }
  });
}, false);


function makeResultTable(data) {
  const cols = data.columns || [];
  const rows = data.rows    || [];
  if (!cols.length && !rows.length) {
    const d = document.createElement('div');
    d.className = 'result-empty'; d.textContent = 'Нет результатов.'; return d;
  }
  const wrap  = document.createElement('div'); wrap.className = 'result-table-wrap';
  const tbl   = document.createElement('table'); tbl.className = 'result-table';
  const thead = tbl.createTHead(); const hrow = thead.insertRow();
  cols.forEach(c => { const th = document.createElement('th'); th.textContent = c; hrow.appendChild(th); });
  const tbody = tbl.createTBody();
  rows.forEach(row => {
    const tr   = tbody.insertRow();
    const vals = Array.isArray(row) ? row : cols.map(c => row[c]);
    vals.forEach(v => {
      const td = tr.insertCell();
      if (v === null || v === undefined) td.innerHTML = '<span class="null-val">NULL</span>';
      else td.textContent = String(v);
    });
  });
  wrap.appendChild(tbl);
  return wrap;
}


/* ═══════════════════════════════════════════════════
   YAML pipeline import (Step 5 — GitOps preview)
═══════════════════════════════════════════════════ */
let _yamlPreview = null;

function openYamlImport() {
  document.getElementById('yaml-modal').classList.remove('hidden');
  document.getElementById('yaml-preview-out').style.display = 'none';
  document.getElementById('yaml-status').textContent = '';
  document.getElementById('yaml-apply-btn').disabled = true;
  _yamlPreview = null;
}

async function previewYaml() {
  const text = document.getElementById('yaml-input').value;
  const status = document.getElementById('yaml-status');
  const out    = document.getElementById('yaml-preview-out');
  if (!text.trim()) { status.textContent = 'пусто'; return; }
  status.textContent = 'проверяю…';
  try {
    const headers = { 'Content-Type': 'text/yaml' };
    if (jwtToken) headers['Authorization'] = 'Bearer ' + jwtToken;
    const r = await fetch(API + '/api/pipelines/preview-yaml',
                          { method: 'POST', headers, body: text });
    const body = await r.text();
    if (!r.ok) {
      out.style.display = 'block';
      out.textContent = body;
      status.textContent = `ошибка HTTP ${r.status}`;
      _yamlPreview = null;
      document.getElementById('yaml-apply-btn').disabled = true;
      return;
    }
    let parsed;
    try { parsed = JSON.parse(body); }
    catch (_) { status.textContent = 'некорректный JSON-ответ'; return; }
    out.style.display = 'block';
    out.textContent = JSON.stringify(parsed, null, 2);
    status.textContent = `✓ ok — pipeline "${parsed.name || '?'}", ${(parsed.steps || []).length} шаг(ов)`;
    _yamlPreview = parsed;
    document.getElementById('yaml-apply-btn').disabled = false;
  } catch (err) {
    status.textContent = String(err);
    _yamlPreview = null;
  }
}

async function applyYaml() {
  if (!_yamlPreview) { showToast('Сначала нажми «Проверить»', 'error'); return; }
  try {
    await apiPost('/api/pipelines', _yamlPreview);
    document.getElementById('yaml-modal').classList.add('hidden');
    showToast(`Конвейер "${_yamlPreview.name}" импортирован`, 'ok');
    loadPipelines();
  } catch (err) {
    showToast('Не удалось применить: ' + err, 'error');
  }
}

/* ═══════════════════════════════════════════════════
   PIPELINE LIST
═══════════════════════════════════════════════════ */
async function loadPipelines() {
  const list = document.getElementById('pipelines-list');
  list.innerHTML = '<div style="color:var(--muted);padding:.5rem">Загрузка…</div>';
  try {
    const pipelines = await apiFetch('/api/pipelines');
    if (!pipelines) return;
    if (!pipelines.length) {
      list.innerHTML = `
        <div class="empty-state">
          Конвейеров пока нет.<br>
          <button class="btn btn-primary" style="margin-top:.75rem" onclick="openPipelineBuilder(null)">
            + Создать первый конвейер
          </button>
        </div>`;
      return;
    }
    list.innerHTML = '';
    pipelines.forEach(p => list.appendChild(makePipelineRow(p)));
  } catch (err) {
    list.innerHTML = `<div style="color:var(--red)">Error: ${escHtml(String(err))}</div>`;
  }
}

const STATUS_LABELS = ['ожидание','выполняется','успех','ошибка','отменён'];
const STATUS_BADGE  = ['badge-warn','badge-run','badge-ok','badge-err','badge-warn'];

const _pipelineCache = {};

function makePipelineRow(p) {
  _pipelineCache[p.id] = p;
  const div    = document.createElement('div');
  div.className = 'pipeline-item pipeline-item-clickable';
  div.onclick = (e) => {
    if (e.target.closest('button')) return;
    openPipelineBuilder(_pipelineCache[p.id]);
  };
  const status = p.status || 0;
  const steps  = (p.steps || []).map(s =>
    `<span class="step-badge">${escHtml(s.name || s.id || 'step')}</span>`).join('');
  const nextRunStr = p.next_run && p.next_run > 0 && p.next_run < 9e15
    ? new Date(p.next_run * 1000).toLocaleString() : '—';
  div.innerHTML = `
    <div style="flex:1;min-width:0">
      <div style="display:flex;align-items:center;gap:.5rem;flex-wrap:wrap">
        <span class="pipeline-name">${escHtml(p.name || p.id)}</span>
        <span class="badge ${STATUS_BADGE[status]}">${STATUS_LABELS[status]}</span>
        ${!p.enabled ? '<span class="badge badge-warn">отключён</span>' : ''}
      </div>
      <div class="pipeline-meta">
        расписание: <code>${escHtml(p.cron || 'вручную')}</code>
        &nbsp;·&nbsp; последний запуск: ${p.last_run ? new Date(p.last_run * 1000).toLocaleString() : '—'}
        &nbsp;·&nbsp; следующий: ${nextRunStr}
      </div>
      <div class="step-list">${steps}</div>
    </div>
    <div class="pipeline-actions">
      <button class="btn btn-sm btn-primary" onclick="triggerPipeline('${escAttr(p.id)}')">▶ Запустить</button>
      <button class="btn btn-sm" onclick="showPipelineRuns('${escAttr(p.id)}','${escAttr(p.name||p.id)}')">История</button>
      <button class="btn btn-sm btn-danger" onclick="deletePipeline('${escAttr(p.id)}','${escAttr(p.name||p.id)}')">✕</button>
    </div>
  `;
  return div;
}

async function triggerPipeline(id) {
  try {
    await apiPost(`/api/pipelines/${id}/run`, {});
    showToast('Конвейер запущен', 'ok');
    logActivity(`запущен конвейер ${id}`);
    setTimeout(loadPipelines, 600);
  } catch (err) {
    showToast(String(err), 'error');
  }
}

async function deletePipeline(id, name) {
  if (!confirm(`Удалить конвейер "${name || id}"?`)) return;
  try {
    await apiFetch(`/api/pipelines/${id}`, 'DELETE');
    showToast(`Конвейер "${name || id}" удалён`, 'ok');
    loadPipelines();
  } catch (err) { showToast(String(err), 'error'); }
}

async function showPipelineRuns(id, name) {
  const modal = document.getElementById('runs-modal');
  const title = document.getElementById('runs-modal-title');
  const body  = document.getElementById('runs-modal-body');
  title.textContent = `История запусков — ${name || id}`;
  body.innerHTML = '<div style="color:var(--muted)">Загрузка…</div>';
  modal.classList.remove('hidden');
  try {
    const runs = await apiFetch(`/api/pipelines/${id}/runs`);
    const list = Array.isArray(runs) ? runs : [];
    if (!list.length) { body.innerHTML = '<div style="color:var(--muted);padding:.5rem">Запусков пока нет.</div>'; return; }
    let html = '<table class="runs-table"><thead><tr><th>#</th><th>Начало</th><th>Окончание</th><th>Статус</th><th>Повторы</th><th>Ошибка</th></tr></thead><tbody>';
    list.forEach(r => {
      const statusLabel = r.status === 0 ? '<span class="badge badge-ok">успех</span>'
        : '<span class="badge badge-err">ошибка</span>';
      html += `<tr>
        <td>${r.id}</td>
        <td>${r.started ? new Date(r.started*1000).toLocaleString() : '—'}</td>
        <td>${r.finished ? new Date(r.finished*1000).toLocaleString() : '—'}</td>
        <td>${statusLabel}</td>
        <td>${typeof r.retries === 'number' ? r.retries : 0}</td>
        <td>${escHtml(r.error || '')}</td>
      </tr>`;
    });
    html += '</tbody></table>';
    body.innerHTML = html;
  } catch (err) { body.innerHTML = `<div style="color:var(--red)">${escHtml(String(err))}</div>`; }
}


/* ═══════════════════════════════════════════════════
   PIPELINE BUILDER
═══════════════════════════════════════════════════ */
const CRON_DESCRIPTIONS = {
  '@reboot':     'При запуске (однократно)',
  '@hourly':     'Каждый час',
  '@daily':      'Каждый день в полночь',
  '@weekly':     'Каждое воскресенье в полночь',
  '@monthly':    '1-е число каждого месяца в полночь',
  '@yearly':     '1 января в полночь',
  '* * * * *':   'Каждую минуту',
  '*/5 * * * *': 'Каждые 5 минут',
  '*/15 * * * *':'Каждые 15 минут',
  '*/30 * * * *':'Каждые 30 минут',
  '0 * * * *':   'Каждый час (ровно)',
  '0 0 * * *':   'Каждый день в полночь',
  '0 6 * * *':   'Каждый день в 6:00',
  '0 9 * * *':   'Каждый день в 9:00',
  '0 9 * * 1-5': 'По будням в 9:00',
  '0 0 * * 0':   'Каждое воскресенье',
  '0 0 1 * *':   '1-е число каждого месяца',
};

function cronDescription(expr) {
  return CRON_DESCRIPTIONS[expr] || (expr ? 'Произвольное расписание' : 'Без расписания — только вручную');
}

function openPipelineBuilder(pipeline) {
  pb.editId = pipeline ? (pipeline.id || null) : null;
  pb.steps  = pipeline ? (pipeline.steps || []).map(s => ({...s, deps: s.deps || []})) : [];
  pb.max_retries     = pipeline ? (pipeline.max_retries || 3) : 3;
  pb.retry_delay_sec = pipeline ? (pipeline.retry_delay_sec || 30) : 30;
  pb.webhook_url     = pipeline ? (pipeline.webhook_url || '') : '';
  pb.webhook_on      = pipeline ? (pipeline.webhook_on || 'failure') : 'failure';
  pb.alert_cooldown  = pipeline ? (pipeline.alert_cooldown || 300) : 300;
  /* Step 4: extra triggers beyond the legacy `cron` field. We keep cron as
   * its own primary input; webhook/file_arrival/manual ride here. */
  pb.triggers = pipeline && Array.isArray(pipeline.triggers)
    ? pipeline.triggers.filter(t => t.type !== 'cron').map(t => ({...t}))
    : [];

  document.getElementById('pb-name').value      = pipeline ? (pipeline.name || '') : '';
  document.getElementById('pb-enabled').checked = pipeline ? !!pipeline.enabled : true;
  document.getElementById('pb-cron').value       = pipeline ? (pipeline.cron || '') : '';
  document.getElementById('pb-max-retries').value = pb.max_retries;
  document.getElementById('pb-retry-delay').value = pb.retry_delay_sec;
  document.getElementById('pb-webhook-url').value = pb.webhook_url;
  document.getElementById('pb-webhook-on').value = pb.webhook_on;
  document.getElementById('pb-alert-cooldown').value = pb.alert_cooldown;
  document.getElementById('pb-cron-preset').value = '';
  syncCronPreset();

  const title = document.getElementById('builder-title');
  title.textContent = pb.editId ? `Редактирование — ${pipeline.name || pb.editId}` : 'Новый конвейер';

  /* show builder nav */
  const nb = document.getElementById('nav-builder');
  nb.classList.add('visible');

  renderBuilderSteps();
  renderTriggers();

  /* Push a pipeline-specific URL so F5 can restore the same builder context.
   * Use switchView with pushState:false so it doesn't push generic '#builder'
   * that would lose the id. */
  switchView('builder', { pushState: false });
  const wantHash = '#builder/' + (pb.editId || 'new');
  if (location.hash !== wantHash) {
    history.pushState({ view: 'builder', id: pb.editId || null }, '', wantHash);
  }
}

/* Parse a hash like '#builder/p_xxx' or '#analytics' into { view, id }.
 * Returns id=null when the hash is a plain view name. */
function parseHash() {
  const raw = location.hash.replace('#', '');
  const slash = raw.indexOf('/');
  if (slash > 0) return { view: raw.slice(0, slash), id: raw.slice(slash + 1) };
  return { view: raw, id: null };
}

/* Look up a pipeline by id and open the builder on it, or fall back to the
 * list. Used by bootView/initApp/popstate when the URL is '#builder/<id>'. */
async function restoreBuilderFromHash(id) {
  if (!id || id === 'new') { switchView('pipelines', { pushState: false }); return; }
  try {
    const pipelines = await apiFetch('/api/pipelines');
    const p = (pipelines || []).find(x => x.id === id);
    if (p) { openPipelineBuilder(p); return; }
  } catch (_) { /* fall through to pipelines */ }
  switchView('pipelines', { pushState: false });
  history.replaceState({ view: 'pipelines' }, '', '#pipelines');
}

/* ── Step 4: triggers UI ─────────────────────────────────────── */
function pbAddTrigger() {
  if (!pb.triggers) pb.triggers = [];
  pb.triggers.push({
    type: 'webhook',
    webhook_token: 'wh_' + Math.random().toString(36).slice(2, 12),
    webhook_method: 'POST',
  });
  renderTriggers();
}

function pbRemoveTrigger(idx) {
  pb.triggers.splice(idx, 1);
  renderTriggers();
}

function pbUpdateTrigger(idx, field, val) {
  pb.triggers[idx][field] = val;
}

function pbChangeTriggerType(idx, type) {
  const t = pb.triggers[idx] = { type };
  if (type === 'webhook') {
    t.webhook_token = 'wh_' + Math.random().toString(36).slice(2, 12);
    t.webhook_method = 'POST';
  } else if (type === 'file_arrival') {
    t.watch_dir    = '/var/dfo/incoming';
    t.file_pattern = '*.csv';
  }
  renderTriggers();
}

function renderTriggers() {
  const el = document.getElementById('pb-triggers');
  if (!el) return;
  if (!pb.triggers || pb.triggers.length === 0) {
    el.innerHTML = '<div style="font-size:.75rem;color:var(--muted);padding:.4rem 0">Триггеров нет — конвейер фирится только по cron (выше) или вручную.</div>';
    return;
  }
  el.innerHTML = pb.triggers.map((t, i) => {
    let body = '';
    if (t.type === 'webhook') {
      const port = location.port || '8080';
      const url  = `${location.protocol}//${location.hostname}:${port}/api/triggers/${escAttr(t.webhook_token || '')}`;
      body = `
        <input type="text" placeholder="webhook_token (секрет)"
               value="${escAttr(t.webhook_token || '')}"
               oninput="pbUpdateTrigger(${i},'webhook_token',this.value);renderTriggers()"
               style="flex:1;font-family:var(--mono);font-size:.78rem">
        <select onchange="pbUpdateTrigger(${i},'webhook_method',this.value)" style="width:90px">
          <option value="POST" ${t.webhook_method!=='GET'?'selected':''}>POST</option>
          <option value="GET"  ${t.webhook_method==='GET'?'selected':''}>GET</option>
        </select>
        <code style="font-size:.7rem;color:var(--muted);flex:2;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
              title="${escAttr(url)}">${escHtml(url)}</code>`;
    } else if (t.type === 'file_arrival') {
      body = `
        <input type="text" placeholder="watch_dir"
               value="${escAttr(t.watch_dir || '')}"
               oninput="pbUpdateTrigger(${i},'watch_dir',this.value)"
               style="flex:2;font-family:var(--mono);font-size:.78rem">
        <input type="text" placeholder="*.csv"
               value="${escAttr(t.file_pattern || '*')}"
               oninput="pbUpdateTrigger(${i},'file_pattern',this.value)"
               style="flex:1;font-family:var(--mono);font-size:.78rem">
        <span style="font-size:.7rem;color:var(--muted)">Linux only</span>`;
    } else if (t.type === 'manual') {
      body = '<span style="font-size:.75rem;color:var(--muted);flex:1">Только через UI / POST /api/pipelines/:id/run</span>';
    } else if (t.type === 'cron') {
      body = `
        <input type="text" placeholder="cron expression"
               value="${escAttr(t.cron_expr || '')}"
               oninput="pbUpdateTrigger(${i},'cron_expr',this.value)"
               style="flex:1;font-family:var(--mono);font-size:.78rem">`;
    }
    return `<div style="display:flex;gap:.4rem;align-items:center;margin-bottom:.4rem">
      <select onchange="pbChangeTriggerType(${i},this.value)" style="width:140px">
        <option value="webhook"      ${t.type==='webhook'      ?'selected':''}>🪝 Webhook</option>
        <option value="file_arrival" ${t.type==='file_arrival' ?'selected':''}>📁 File arrival</option>
        <option value="cron"         ${t.type==='cron'         ?'selected':''}>⏰ Cron (доп.)</option>
        <option value="manual"       ${t.type==='manual'       ?'selected':''}>👆 Только вручную</option>
      </select>
      ${body}
      <button class="btn btn-sm btn-danger" onclick="pbRemoveTrigger(${i})" title="Удалить триггер">✕</button>
    </div>`;
  }).join('');
}

/* listen to enabled toggle */
document.getElementById('pb-enabled').addEventListener('change', function() {
  document.getElementById('pb-enabled-label').textContent = this.checked ? 'Активен' : 'Неактивен';
});

function applyCronPreset(val) {
  if (!val) return;
  document.getElementById('pb-cron').value = val;
  updateCronHuman(val);
}

function syncCronPreset() {
  const val = document.getElementById('pb-cron').value.trim();
  updateCronHuman(val);
  const sel = document.getElementById('pb-cron-preset');
  let found = false;
  for (const o of sel.options) { if (o.value === val) { o.selected = true; found = true; break; } }
  if (!found) sel.value = '';
}

function updateCronHuman(expr) {
  const el = document.getElementById('pb-cron-human');
  if (el) el.textContent = cronDescription(expr.trim());
}

/* ── Step management ── */
function pbAddStep() {
  pb.steps.push({
    id: `step_${pb.steps.length + 1}`,
    name: '',
    connector_type: '',
    connector_config: '',
    transform_sql: '',
    target_table: '',
    deps: [],
    max_retries: 3,
    retry_delay_sec: 30,
  });
  renderBuilderSteps();
  /* scroll to bottom */
  const last = document.getElementById('pb-steps').lastElementChild;
  if (last) last.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function pbRemoveStep(idx) {
  pb.steps.splice(idx, 1);
  /* fix dep references: remove refs to deleted step, shift down refs > idx */
  pb.steps.forEach(s => {
    s.deps = (s.deps || [])
      .filter(d => d !== idx)
      .map(d => d > idx ? d - 1 : d);
  });
  renderBuilderSteps();
}

function pbMoveStep(idx, dir) {
  const newIdx = idx + dir;
  if (newIdx < 0 || newIdx >= pb.steps.length) return;
  [pb.steps[idx], pb.steps[newIdx]] = [pb.steps[newIdx], pb.steps[idx]];
  /* fix dep references: swap idx and newIdx */
  pb.steps.forEach(s => {
    s.deps = (s.deps || []).map(d => d === idx ? newIdx : d === newIdx ? idx : d);
  });
  renderBuilderSteps();
}

function pbUpdateStep(idx, field, val) {
  pb.steps[idx][field] = val;
}

function pbChangeConnType(idx, type) {
  /* Special pseudo-type "__python": switches the step into Python mode.
   * Internally that means clearing connector_type and seeding python_code
   * with a starter snippet so the textarea has something useful in it. */
  if (type === '__python') {
    pb.steps[idx].connector_type   = '';
    pb.steps[idx].connector_config = '';
    if (!pb.steps[idx].python_code) {
      pb.steps[idx].python_code =
        '# input  : df  (pandas DataFrame from transform_sql)\n' +
        '# output : df  (will be written to target_table as CSV)\n' +
        'df["score"] = df["score"] * 2\n' +
        'df = df[df["score"] > 50]';
    }
  } else {
    pb.steps[idx].connector_type    = type;
    pb.steps[idx].connector_config  = '';
    pb.steps[idx].python_code       = '';
  }
  /* re-render only the connector config section */
  const cfgDiv = document.getElementById(`step-conn-cfg-${idx}`);
  if (cfgDiv) cfgDiv.innerHTML = makeConnectorConfigHTML(pb.steps[idx], idx);
}

function pbUpdateConnConfig(idx, field, val) {
  const cfg = safeParse(pb.steps[idx].connector_config, {});
  cfg[field] = val;
  pb.steps[idx].connector_config = JSON.stringify(cfg);
}

/* ── Step "type" ────────────────────────────────────────────────────────────
 * A step has no explicit "type" field — the backend dispatches on WHICH field
 * is filled (python_code → connector_type → scd2_business_key → transform_sql).
 * The UI mirrors that: `_ui_type` is a transient authoring hint (stripped before
 * any API call by stepForApi) so a half-filled SCD2/Match step keeps its form
 * shown; when absent (e.g. a pipeline loaded from the server) we derive it. */
const SCD2_FIELDS = ['scd2_business_key','scd2_compare_columns','scd2_transaction_time',
                     'scd2_effective_from_col','scd2_effective_to_col','scd2_deleted_flag',
                     'scd2_ignored_columns','scd2_hash_col'];
const PY_STARTER =
  '# input  : df  (pandas DataFrame from transform_sql)\n' +
  '# output : df  (will be written to target_table as CSV)\n' +
  'df["score"] = df["score"] * 2\n' +
  'df = df[df["score"] > 50]';
const SCALA_STARTER =
  '// input  : df  (Spark DataFrame from transform_sql; all cols are String)\n' +
  '// output : df  (will be written to target_table as CSV)\n' +
  'import org.apache.spark.sql.functions._\n' +
  'df = df.withColumn("score", col("score").cast("int") * 2)\n' +
  'df = df.filter(col("score") > 50)';
const MATCH_RULES_STARTER =
  JSON.stringify([
    { rule_name: '1.1 DUL→FIO', id_column: 'party_id',
      scan_columns: ['dul_number'], test_columns: ['last_name', 'first_name'],
      metric_function: 'word_similarity', operator: '>=', threshold: 0.80, normalize: 'name' }
  ], null, 2);

const CONNECTOR_TYPES = ['csv','parquet','json_http','postgresql','greenplum','oracle','s3','kafka','airbyte'];

/* Connectors that can act as sinks (write_batch in ABI v2). */
const SINK_TYPES = ['csv','json_http','postgresql','s3','kafka'];

function stepType(step) {
  if (step._ui_type) return step._ui_type;
  if (step.is_sink && step.connector_type) return 'sink:' + step.connector_type;
  if (step.match_rules)       return 'matchrules';
  if (step.scd2_business_key) return 'scd2';
  if (step.python_code || step.python_file) return 'python';
  if (step.scala_code)        return 'scala';
  if (step.connector_type)    return step.connector_type;  /* the specific connector */
  return 'sql';
}

/* Shallow copy of a step WITHOUT UI-only (underscore-prefixed) keys — used
 * everywhere a step is sent to the API so _ui_type / _match_* never leak. */
function stepForApi(s) {
  const out = {};
  for (const k in s) if (!k.startsWith('_')) out[k] = s[k];
  return out;
}

function pbChangeStepType(idx, type) {
  const s = pb.steps[idx];
  s._ui_type = type;
  /* Reset every discriminating field so only the chosen one stays set. */
  s.connector_type = '';
  s.connector_config = '';
  s.python_code = '';
  s.python_file = '';
  s.python_context_dir = '';
  s.scala_code = '';
  s.match_rules = '';
  s.match_input_table = '';
  s.is_sink = false;
  SCD2_FIELDS.forEach(k => delete s[k]);
  if (type.startsWith('sink:')) {
    /* Sink (приёмник): write transform_sql rows OUT via the connector. */
    s.is_sink = true;
    s.connector_type = type.slice(5);
    s.connector_config = '{}';
    if (!s.sink_mode) s.sink_mode = 'append';
    if (s.sink_entity == null) s.sink_entity = '';
  } else if (type === 'python') {
    s.python_code = PY_STARTER;
  } else if (type === 'scala') {
    s.scala_code = SCALA_STARTER;
    if (s.scala_timeout_sec == null) s.scala_timeout_sec = 600;
  } else if (CONNECTOR_TYPES.includes(type)) {
    s.connector_type = type;
    s.connector_config = '{}';
  } else if (type === 'scd2') {
    s.scd2_business_key       = s.scd2_business_key       || '';
    s.scd2_effective_from_col = s.scd2_effective_from_col || 'valid_from';
    s.scd2_effective_to_col   = s.scd2_effective_to_col   || 'valid_to';
  } else if (type === 'matchrules') {
    s.match_rules = s.match_rules || MATCH_RULES_STARTER;
  }
  renderBuilderSteps();
}

/* Match step → builds a jaro_winkler() cross-match query into transform_sql.
 * Match is just a SQL step on the backend, so this only seeds the textarea;
 * the user can edit the generated SQL freely afterwards. */
function pbGenerateMatchSQL(idx) {
  const s = pb.steps[idx];
  const A  = (s._match_left  || 'table_a').trim();
  const B  = (s._match_right || 'table_b').trim();
  const c  = (s._match_col   || 'name').trim();
  const th = (s._match_threshold != null ? s._match_threshold : 0.85);
  s.transform_sql =
`SELECT a.${c} AS left_${c}, b.${c} AS right_${c},
       jaro_winkler(a.${c}, b.${c}) AS similarity
FROM ${A} a, ${B} b
WHERE jaro_winkler(a.${c}, b.${c}) >= ${th}`;
  renderBuilderSteps();
}

/* Sink (приёмник) fields: destination entity + write mode. The connection
 * config (path/host/bucket/brokers/...) is rendered by makeConnectorConfigHTML
 * above, since a sink reuses the connector's own config form. */
function makeSinkFieldsHTML(step, idx) {
  const ENT = {
    csv:        ['Путь файла',        './data/export.csv'],
    json_http:  ['URL (пусто = из конфига)', 'https://hooks.example.com/ingest'],
    postgresql: ['Таблица назначения', 'public.export_table'],
    s3:         ['Ключ объекта',       'exports/revenue.csv'],
    kafka:      ['Топик',              'dfo_export'],
  }[step.connector_type] || ['Назначение', ''];
  const mode = step.sink_mode || 'append';
  return `
    <div style="margin-top:.75rem;padding:.6rem;border:1px dashed var(--border);border-radius:var(--radius)">
      <div style="font-size:.72rem;color:var(--muted);margin-bottom:.5rem">
        📤 Приёмник. Поле <strong>SQL-трансформация</strong> ниже = запрос-источник
        (его строки выгружаются наружу). Поле «Результат → таблица» для приёмника не используется.</div>
      <div class="step-row-2">
        <div class="form-group" style="margin:0">
          <label>${ENT[0]}</label>
          <input type="text" value="${escAttr(step.sink_entity || '')}" placeholder="${escAttr(ENT[1])}"
                 oninput="pbUpdateStep(${idx},'sink_entity',this.value)">
        </div>
        <div class="form-group" style="margin:0">
          <label>Режим записи</label>
          <select onchange="pbUpdateStep(${idx},'sink_mode',this.value)">
            <option value="append"    ${mode==='append'   ?'selected':''}>Дозапись (append)</option>
            <option value="overwrite" ${mode==='overwrite'?'selected':''}>Перезапись (overwrite)</option>
          </select>
        </div>
      </div>
    </div>`;
}

function makeScd2FieldsHTML(step, idx) {
  const f = (label, key, ph, hint) => `
    <div class="form-group" style="margin:0"><label>${label}${hint ? ` <span class="label-hint">${hint}</span>` : ''}</label>
      <input type="text" value="${escAttr(step[key] || '')}" placeholder="${ph}"
             oninput="pbUpdateStep(${idx},'${key}',this.value)"></div>`;
  return `
    <div style="margin-top:.75rem;padding:.6rem;border:1px dashed var(--border);border-radius:var(--radius)">
      <div style="font-size:.72rem;color:var(--muted);margin-bottom:.5rem">
        🕒 SCD2 (тип 2). Поле <strong>SQL-трансформация</strong> ниже = входящий срез источника.</div>
      <div class="step-row-2">${f('Business key','scd2_business_key','customer_id','через запятую')}${f('Compare columns','scd2_compare_columns','name,email,city','пусто = все')}</div>
      <div class="step-row-2" style="margin-top:.4rem">${f('valid_from колонка','scd2_effective_from_col','valid_from')}${f('valid_to колонка','scd2_effective_to_col','valid_to')}</div>
      <div class="step-row-2" style="margin-top:.4rem">${f('transaction_time','scd2_transaction_time','updated_at','для ORDER BY окна')}${f('deleted_flag','scd2_deleted_flag','is_deleted','soft-delete в источнике')}</div>
      <div class="step-row-2" style="margin-top:.4rem">${f('hash колонка','scd2_hash_col','_dfo_row_hash','MD5 по compare-колонкам; ускоряет детект изменений и ловит ""↔NULL')}</div>
    </div>`;
}

function makeMatchFieldsHTML(step, idx) {
  const thr = step._match_threshold != null ? step._match_threshold : 0.85;
  return `
    <div style="margin-top:.75rem;padding:.6rem;border:1px dashed var(--border);border-radius:var(--radius)">
      <div style="font-size:.72rem;color:var(--muted);margin-bottom:.5rem">
        🔗 Match (fuzzy). Кнопка соберёт SQL c <code>jaro_winkler()</code> в поле <strong>SQL-трансформация</strong> ниже — его можно править.</div>
      <div class="step-row-2">
        <div class="form-group" style="margin:0"><label>Таблица A</label>
          <input type="text" value="${escAttr(step._match_left || '')}" placeholder="customers_crm"
                 oninput="pbUpdateStep(${idx},'_match_left',this.value)"></div>
        <div class="form-group" style="margin:0"><label>Таблица B</label>
          <input type="text" value="${escAttr(step._match_right || '')}" placeholder="customers_erp"
                 oninput="pbUpdateStep(${idx},'_match_right',this.value)"></div>
      </div>
      <div class="step-row-2" style="margin-top:.4rem">
        <div class="form-group" style="margin:0"><label>Ключевая колонка</label>
          <input type="text" value="${escAttr(step._match_col || '')}" placeholder="name"
                 oninput="pbUpdateStep(${idx},'_match_col',this.value)"></div>
        <div class="form-group" style="margin:0"><label>Порог similarity (0..1)</label>
          <input type="number" min="0" max="1" step="0.05" value="${escAttr(thr)}"
                 oninput="pbUpdateStep(${idx},'_match_threshold',parseFloat(this.value))"></div>
      </div>
      <button type="button" class="btn btn-sm btn-primary" style="margin-top:.5rem"
              onclick="event.stopPropagation();pbGenerateMatchSQL(${idx})">⚙ Сгенерировать SQL</button>
    </div>`;
}

/* Match-rules: declarative entity-resolution engine (run_match_step). The
 * candidate set comes from the SQL-трансформация field below; match_rules is a
 * JSON array of rules. See docs/MATCH_RULES.md. */
function makeMatchRulesFieldsHTML(step, idx) {
  return `
    <div style="margin-top:.75rem;padding:.6rem;border:1px dashed var(--border);border-radius:var(--radius)">
      <div style="font-size:.72rem;color:var(--muted);margin-bottom:.5rem">
        🧬 Match-rules (движок). Кандидаты — из поля <strong>SQL-трансформация</strong> ниже;
        результат (<code>id_a,id_b,rule_name,metric_value</code>) — в <strong>target_table</strong>.
        Метрики: <code>word_similarity</code> · <code>jaro_winkler</code> · <code>levenshtein</code>.</div>
      <div class="form-group" style="margin:0">
        <label>Правила (JSON-массив) <span class="label-hint">scan_columns = точная группа; test_columns = similarity</span></label>
        <textarea class="mono-textarea" rows="12"
          oninput="pb.steps[${idx}].match_rules=this.value"
          style="font-family:var(--mono);font-size:.74rem">${escHtml(step.match_rules || '')}</textarea>
      </div>
    </div>`;
}

function pbToggleDep(stepIdx, depIdx, checked) {
  const deps = pb.steps[stepIdx].deps || [];
  if (checked && !deps.includes(depIdx)) deps.push(depIdx);
  pb.steps[stepIdx].deps = checked ? deps : deps.filter(d => d !== depIdx);
}

/* ── Rendering ── */
function renderBuilderSteps() {
  renderFlowGraph();
  const container = document.getElementById('pb-steps');
  if (!pb.steps.length) {
    container.innerHTML = `
      <div class="empty-state">
        Шагов пока нет.<br>
        <button class="btn btn-primary" style="margin-top:.75rem" onclick="pbAddStep()">+ Добавить первый шаг</button>
      </div>`;
    return;
  }
  container.innerHTML = '';
  pb.steps.forEach((step, i) => {
    container.appendChild(makeStepCard(step, i));
    /* Sibling div that lives directly below each card — this is where the
     * inline target-table preview renders when the card is clicked. */
    const preview = document.createElement('div');
    preview.id = `step-preview-${i}`;
    preview.className = 'step-preview-inline';
    preview.style.cssText = 'display:none;margin:-0.5rem 0 1rem 0';
    container.appendChild(preview);
  });
}

function renderFlowGraph() {
  const wrap = document.getElementById('pb-flow-graph');
  if (!wrap) return;
  if (!pb.steps.length) { wrap.innerHTML = ''; return; }

  const NW = 200, NH = 80, GAP = 52, PADX = 20, PADY = 28;
  const W = pb.steps.length * (NW + GAP) - GAP + PADX * 2;
  const H = NH + PADY * 2;

  const ACCENT  = '#5b6ef5';
  const NODE_BG = '#252d4a';
  const TEXT    = '#f1f5f9';
  const SUB     = '#94a3b8';
  const BADGE_BG= '#5b6ef5';

  let s = `<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <marker id="arh" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto">
      <polygon points="0 0,10 4,0 8" fill="${ACCENT}"/>
    </marker>
  </defs>`;

  pb.steps.forEach((step, i) => {
    const x = PADX + i * (NW + GAP), y = PADY;
    const cx = x + NW / 2;
    const name  = (step.name || `Шаг ${i + 1}`).slice(0, 24);
    const table = step.target_table ? step.target_table.slice(0, 26) : '';

    if (i > 0) {
      const x0 = x - GAP, my = y + NH / 2;
      s += `<line x1="${x0}" y1="${my}" x2="${x - 3}" y2="${my}"
              stroke="${ACCENT}" stroke-width="2" marker-end="url(#arh)"/>`;
    }

    /* badge pill sits above the node with 10px gap */
    const bw = 52, bh = 20, bx = cx - bw / 2, by = y - bh - 6;

    s += `<g onclick="document.getElementById('step-card-${i}')?.scrollIntoView({behavior:'smooth',block:'nearest'})" style="cursor:pointer">
      <rect x="${x}" y="${y}" width="${NW}" height="${NH}" rx="10"
            fill="${NODE_BG}" stroke="${ACCENT}" stroke-width="2"/>
      <rect x="${bx}" y="${by}" width="${bw}" height="${bh}" rx="10"
            fill="${BADGE_BG}"/>
      <text x="${cx}" y="${by + 14}" text-anchor="middle" font-size="10" font-weight="700"
            fill="#fff" font-family="system-ui,sans-serif" letter-spacing=".05em">ШАГ ${i + 1}</text>
      <text x="${cx}" y="${y + 30}" text-anchor="middle" font-size="13" font-weight="600"
            fill="${TEXT}" font-family="system-ui,sans-serif">${escHtml(name)}</text>
      ${table ? `<text x="${cx}" y="${y + 52}" text-anchor="middle" font-size="11"
            fill="${SUB}" font-family="system-ui,sans-serif"
            onclick="event.stopPropagation();document.getElementById('step-card-${i}')?.scrollIntoView({behavior:'smooth',block:'nearest'});runStepPreview(${i})"
            style="cursor:pointer;text-decoration:underline">→ ${escHtml(table)}</text>` : ''}
    </g>`;
  });

  s += '</svg>';
  wrap.innerHTML = s;
}

function makeStepCard(step, idx) {
  const div = document.createElement('div');
  div.className = 'step-card';
  div.id = `step-card-${idx}`;
  div.style.cursor = 'pointer';
  /* Click on the card body (but not on form controls) toggles the target_table
   * preview panel inline below the card. */
  div.addEventListener('click', (e) => {
    /* Ignore clicks on actual form controls; everything else (labels, hints,
     * card background) toggles the inline preview. We RUN the step in preview
     * mode rather than just querying target_table — most steps haven't been
     * executed yet, so the existing table is empty. */
    if (e.target.closest('input, textarea, select, button, option')) return;
    const panel = document.getElementById(`step-preview-${idx}`);
    if (panel && panel.style.display !== 'none') {
      panel.style.display = 'none';
      panel.innerHTML = '';
    } else {
      runStepPreview(idx);
    }
  });
  const t = stepType(step);
  div.innerHTML = `
    <div class="step-header">
      <span class="step-num">${idx + 1}</span>
      <input class="step-name-input" type="text" placeholder="Название шага"
             value="${escAttr(step.name)}"
             oninput="pbUpdateStep(${idx},'name',this.value);renderFlowGraph()">
      <div class="step-move-btns">
        ${idx > 0                      ? `<button class="btn btn-sm" onclick="pbMoveStep(${idx},-1)" title="Вверх">↑</button>` : ''}
        ${idx < pb.steps.length - 1   ? `<button class="btn btn-sm" onclick="pbMoveStep(${idx}, 1)" title="Вниз">↓</button>` : ''}
      </div>
      <button class="btn btn-sm btn-danger" onclick="pbRemoveStep(${idx})">Удалить</button>
    </div>
    <div class="step-body">
      <div class="step-row-2">
        <div class="form-group" style="margin:0">
          <label>Тип шага <span class="label-hint">= какое поле заполнено</span></label>
          <select onchange="pbChangeStepType(${idx}, this.value)">
            <optgroup label="Трансформации">
              <option value="sql"    ${t==='sql'   ?'selected':''}>SQL-трансформация</option>
              <option value="python" ${t==='python'?'selected':''}>🐍 Python (pandas)</option>
              <option value="scala"  ${t==='scala' ?'selected':''}>⚡ Scala (Spark)</option>
              <option value="scd2"   ${t==='scd2'  ?'selected':''}>🕒 SCD2 (историзация)</option>
              <option value="match"  ${t==='match' ?'selected':''}>🔗 Match (fuzzy SQL)</option>
              <option value="matchrules" ${t==='matchrules'?'selected':''}>🧬 Match-rules (движок)</option>
            </optgroup>
            <optgroup label="Источники (коннекторы)">
              <option value="csv"        ${t==='csv'       ?'selected':''}>CSV файл</option>
              <option value="parquet"    ${t==='parquet'   ?'selected':''}>Parquet файл</option>
              <option value="json_http"  ${t==='json_http' ?'selected':''}>HTTP / REST API (JSON)</option>
              <option value="postgresql" ${t==='postgresql'?'selected':''}>🗄 PostgreSQL (БД)</option>
              <option value="greenplum"  ${t==='greenplum' ?'selected':''}>🐘 Greenplum (MPP)</option>
              <option value="oracle"     ${t==='oracle'    ?'selected':''}>🔶 Oracle (ODPI-C)</option>
              <option value="s3"         ${t==='s3'        ?'selected':''}>S3 / MinIO</option>
              <option value="kafka"      ${t==='kafka'     ?'selected':''}>📨 Kafka</option>
              <option value="airbyte"    ${t==='airbyte'   ?'selected':''}>🛬 Airbyte (любой источник)</option>
            </optgroup>
            <optgroup label="Приёмники (запись наружу)">
              <option value="sink:csv"        ${t==='sink:csv'       ?'selected':''}>📤 CSV файл</option>
              <option value="sink:json_http"  ${t==='sink:json_http' ?'selected':''}>📤 HTTP / Webhook (JSON)</option>
              <option value="sink:postgresql" ${t==='sink:postgresql'?'selected':''}>📤 PostgreSQL (БД)</option>
              <option value="sink:s3"         ${t==='sink:s3'        ?'selected':''}>📤 S3 / MinIO</option>
              <option value="sink:kafka"      ${t==='sink:kafka'     ?'selected':''}>📤 Kafka</option>
            </optgroup>
          </select>
        </div>
        <div class="form-group" style="margin:0">
          <label>Результат → таблица <span class="label-hint">— клик по карточке откроет содержимое</span></label>
          <input id="step-target-${idx}" type="text" placeholder="имя_таблицы"
                 value="${escAttr(step.target_table)}"
                 oninput="pbUpdateStep(${idx},'target_table',this.value);renderFlowGraph()">
        </div>
      </div>
      <div id="step-conn-cfg-${idx}" style="margin-top:0.75rem">${makeConnectorConfigHTML(step, idx)}</div>
      ${t.startsWith('sink:') ? makeSinkFieldsHTML(step, idx) : ''}
      ${t === 'scd2'  ? makeScd2FieldsHTML(step, idx)  : ''}
      ${t === 'match' ? makeMatchFieldsHTML(step, idx) : ''}
      ${t === 'matchrules' ? makeMatchRulesFieldsHTML(step, idx) : ''}
      <div class="step-row-2" style="margin-top:0.75rem">
        <div class="form-group" style="margin:0">
          <label>Max retries</label>
          <input type="number" min="0" value="${escAttr(step.max_retries != null ? step.max_retries : 3)}"
                 oninput="pbUpdateStep(${idx},'max_retries',parseInt(this.value,10) || 0)">
        </div>
        <div class="form-group" style="margin:0">
          <label>Backoff (сек)</label>
          <input type="number" min="1" value="${escAttr(step.retry_delay_sec != null ? step.retry_delay_sec : 30)}"
                 oninput="pbUpdateStep(${idx},'retry_delay_sec',parseInt(this.value,10) || 30)">
        </div>
      </div>
      <div class="form-group" style="margin:0">
        <label style="display:flex;align-items:center;gap:.5rem">
          SQL-трансформация
          <span class="label-hint">выполняется после загрузки данных</span>
          <button type="button" class="btn btn-sm btn-primary" style="margin-left:auto;padding:.15rem .55rem;font-size:.72rem"
                  onclick="event.stopPropagation();runStepSQL(${idx})"
                  title="Выполнить только этот SQL (без upstream-шагов)">▶ SQL</button>
        </label>
        <textarea class="mono-textarea" rows="4"
                  placeholder="SELECT * FROM source_table WHERE ..."
                  oninput="pbUpdateStep(${idx},'transform_sql',this.value)">${escHtml(step.transform_sql || '')}</textarea>
      </div>
      <div style="display:flex;gap:.5rem;margin-top:.75rem;flex-wrap:wrap;align-items:center">
        <button class="btn btn-sm" onclick="event.stopPropagation();runStepPreview(${idx},{save:true})"
                title="Запустить шаг и записать результат в target_table">💾 Сохранить в target_table</button>
        <span style="font-size:.7rem;color:var(--muted);margin-left:auto">клик по карточке → весь chain до этого шага</span>
      </div>
    </div>
  `;
  return div;
}

function makeConnectorConfigHTML(step, idx) {
  const type = step.connector_type;
  const cfg  = safeParse(step.connector_config, {});

  /* Python step (no connector_type, but has python_code or python_file) */
  if (!type && (step.python_code || step.python_file)) {
    return `
      <div class="form-group" style="margin:0">
        <label style="display:flex;align-items:center;gap:.5rem">
          Python-код
          <span class="label-hint">df = pandas DataFrame из transform_sql; результат — в target_table${step.python_file ? '; игнорируется если задан python_file' : ''}</span>
          <button type="button" class="btn btn-sm btn-primary" style="margin-left:auto;padding:.15rem .55rem;font-size:.72rem"
                  onclick="event.stopPropagation();runStepPython(${idx})"
                  title="Выполнить только этот Python (без upstream-шагов)">▶ Python</button>
        </label>
        <textarea class="mono-textarea" rows="8"
          oninput="pb.steps[${idx}].python_code=this.value"
          style="font-family:var(--mono);font-size:.78rem">${escHtml(step.python_code || '')}</textarea>
      </div>
      <div class="step-row-2" style="margin-top:.4rem">
        <div class="form-group" style="margin:0;flex:2">
          <label>python_file <span class="label-hint">путь .py на хосте gateway (≤512КБ; перекрывает код выше)</span></label>
          <input type="text" value="${escAttr(step.python_file || '')}" placeholder="/opt/tsmdm/scripts/connector.py"
                 oninput="pbUpdateStep(${idx},'python_file',this.value)">
        </div>
        <div class="form-group" style="margin:0">
          <label>Timeout (сек)</label>
          <input type="number" min="1" max="3600"
                 value="${step.python_timeout_sec || 300}"
                 oninput="pb.steps[${idx}].python_timeout_sec=parseInt(this.value,10)||300">
        </div>
      </div>
      <div class="form-group" style="margin:.4rem 0 0">
        <label>python_context_dir <span class="label-hint">опц.: общая папка для state между шагами → DFO_CONTEXT_DIR в subprocess</span></label>
        <input type="text" value="${escAttr(step.python_context_dir || '')}" placeholder="/tmp/dfo_run_{{ pipeline_id }}"
               oninput="pbUpdateStep(${idx},'python_context_dir',this.value)">
      </div>
      <div style="font-size:.7rem;color:var(--muted);margin-top:.3rem">
        Требуется <code>python3</code> + <code>pandas</code> на сервере. Один subprocess на запуск шага.
      </div>`;
  }

  /* Scala step (no connector_type, but has scala_code) */
  if (!type && step.scala_code) {
    return `
      <div class="form-group" style="margin:0">
        <label style="display:flex;align-items:center;gap:.5rem">
          Scala-код
          <span class="label-hint">df = Spark DataFrame из transform_sql; результат — в target_table</span>
          <button type="button" class="btn btn-sm btn-primary" style="margin-left:auto;padding:.15rem .55rem;font-size:.72rem"
                  onclick="event.stopPropagation();runStepScala(${idx})"
                  title="Выполнить только этот Scala (без upstream-шагов)">▶ Scala</button>
        </label>
        <textarea class="mono-textarea" rows="8"
          oninput="pb.steps[${idx}].scala_code=this.value"
          style="font-family:var(--mono);font-size:.78rem">${escHtml(step.scala_code)}</textarea>
      </div>
      <div class="step-row-2" style="margin-top:.4rem">
        <div class="form-group" style="margin:0">
          <label>Timeout (сек)</label>
          <input type="number" min="1" max="3600"
                 value="${step.scala_timeout_sec || 600}"
                 oninput="pb.steps[${idx}].scala_timeout_sec=parseInt(this.value,10)||600">
        </div>
        <div style="font-size:.7rem;color:var(--muted);align-self:end">
          Требуется <code>scala-cli</code> на сервере (Spark тянется при первом запуске).
          Один subprocess на запуск шага.
        </div>
      </div>`;
  }

  if (!type) return '';

  if (type === 'csv') return `
    <div class="step-row-2">
      <div class="form-group" style="margin:0">
        <label>Путь к файлу</label>
        <input type="text" value="${escAttr(cfg.path || '')}"
               oninput="pbUpdateConnConfig(${idx},'path',this.value)"
               placeholder="/data/input.csv">
      </div>
      <div class="form-group" style="margin:0">
        <label>Разделитель</label>
        <select onchange="pbUpdateConnConfig(${idx},'delimiter',this.value)">
          <option value=","  ${(cfg.delimiter||',')===',' ?'selected':''}>Запятая (,)</option>
          <option value=";"  ${cfg.delimiter===';'       ?'selected':''}>Точка с запятой (;)</option>
          <option value="\t" ${cfg.delimiter==='\t'      ?'selected':''}>Табуляция (TSV)</option>
        </select>
      </div>
    </div>`;

  if (type === 'parquet') return `
    <div class="step-row-2">
      <div class="form-group" style="margin:0;flex:3">
        <label>Путь или glob-паттерн</label>
        <input type="text" value="${escAttr(cfg.path || '')}"
               oninput="pbUpdateConnConfig(${idx},'path',this.value)"
               placeholder="/data/exports/*.parquet">
      </div>
    </div>`;

  if (type === 'json_http') return `
    <div class="step-row-2">
      <div class="form-group" style="margin:0;flex:3">
        <label>Адрес (URL)</label>
        <input type="text" value="${escAttr(cfg.url || '')}"
               oninput="pbUpdateConnConfig(${idx},'url',this.value)"
               placeholder="https://api.example.com/data">
      </div>
      <div class="form-group" style="margin:0;flex:1">
        <label>Метод</label>
        <select onchange="pbUpdateConnConfig(${idx},'method',this.value)">
          <option ${(cfg.method||'GET')==='GET'?'selected':''}>GET</option>
          <option ${cfg.method==='POST'?'selected':''}>POST</option>
        </select>
      </div>
    </div>
    <div class="step-row-2">
      <div class="form-group" style="margin:0">
        <label>Авторизация</label>
        <select onchange="pbUpdateConnConfig(${idx},'auth_type',this.value)">
          <option value="none"    ${(cfg.auth_type||'none')==='none'   ?'selected':''}>Нет</option>
          <option value="bearer"  ${cfg.auth_type==='bearer'           ?'selected':''}>Bearer Token</option>
          <option value="api_key" ${cfg.auth_type==='api_key'          ?'selected':''}>API Key</option>
        </select>
      </div>
      <div class="form-group" style="margin:0;flex:2" ${(cfg.auth_type||'none')==='none'?'style="display:none"':''}>
        <label>Токен / ключ</label>
        <input type="text" value="${escAttr(cfg.auth_token || '')}"
               oninput="pbUpdateConnConfig(${idx},'auth_token',this.value)"
               placeholder="sk-...">
      </div>
      <div class="form-group" style="margin:0;flex:2">
        <label>Путь к массиву (data_path)</label>
        <input type="text" value="${escAttr(cfg.data_path || '')}"
               oninput="pbUpdateConnConfig(${idx},'data_path',this.value)"
               placeholder="data.items">
      </div>
    </div>`;

  if (type === 'postgresql') return `
    <div class="step-row-2">
      <div class="form-group" style="margin:0">
        <label>Хост</label>
        <input type="text" value="${escAttr(cfg.host || '')}"
               oninput="pbUpdateConnConfig(${idx},'host',this.value)"
               placeholder="localhost">
      </div>
      <div class="form-group" style="margin:0" style="max-width:100px">
        <label>Порт</label>
        <input type="text" value="${escAttr(cfg.port || '5432')}"
               oninput="pbUpdateConnConfig(${idx},'port',this.value)"
               placeholder="5432">
      </div>
      <div class="form-group" style="margin:0">
        <label>База данных</label>
        <input type="text" value="${escAttr(cfg.dbname || '')}"
               oninput="pbUpdateConnConfig(${idx},'dbname',this.value)"
               placeholder="postgres">
      </div>
    </div>
    <div class="step-row-2">
      <div class="form-group" style="margin:0">
        <label>Пользователь</label>
        <input type="text" value="${escAttr(cfg.user || '')}"
               oninput="pbUpdateConnConfig(${idx},'user',this.value)"
               placeholder="postgres">
      </div>
      <div class="form-group" style="margin:0">
        <label>Пароль</label>
        <input type="password" value="${escAttr(cfg.password || '')}"
               oninput="pbUpdateConnConfig(${idx},'password',this.value)"
               placeholder="">
      </div>
      <div class="form-group" style="margin:0;flex:2">
        <label>Таблица / SQL-запрос источника</label>
        <input type="text" value="${escAttr(cfg.query || cfg.table || '')}"
               oninput="pbUpdateConnConfig(${idx},'table',this.value)"
               placeholder="orders или SELECT * FROM orders WHERE active=true">
      </div>
    </div>
    <div class="step-row-2">
      <div class="form-group" style="margin:0">
        <label>Режим забора <span class="label-hint">incremental → cursor_column</span></label>
        <select onchange="pbUpdateConnConfig(${idx},'read_mode',this.value);document.getElementById('step-conn-cfg-${idx}').innerHTML=makeConnectorConfigHTML(pb.steps[${idx}],${idx})">
          <option value="full"   ${(cfg.read_mode||'full')==='full' ?'selected':''}>full — полная выгрузка</option>
          <option value="cursor" ${cfg.read_mode==='cursor'         ?'selected':''}>cursor — инкремент по высокой отметке</option>
          <option value="cdc"    ${cfg.read_mode==='cdc'            ?'selected':''}>cdc — потоковый (фаза 3)</option>
        </select>
      </div>
      ${(cfg.read_mode==='cursor' || cfg.read_mode==='cdc') ? `
      <div class="form-group" style="margin:0;flex:2">
        <label>cursor_column <span class="label-hint">монотонное поле: id / updated_at / lsn</span></label>
        <input type="text" value="${escAttr(cfg.cursor_column || '')}"
               oninput="pbUpdateConnConfig(${idx},'cursor_column',this.value)"
               placeholder="updated_at">
      </div>` : ''}
    </div>`;

  if (type === 'greenplum') return `
    <div style="font-size:.72rem;color:var(--muted);margin-bottom:.5rem">
      🐘 Greenplum (MPP PostgreSQL) через libpq. Таблицу-источник укажи в поле
      <strong>«SQL-трансформация»</strong> ниже (имя таблицы или <code>SELECT …</code>).</div>
    <div class="step-row-2">
      <div class="form-group" style="margin:0">
        <label>Хост (мастер)</label>
        <input type="text" value="${escAttr(cfg.host || '')}"
               oninput="pbUpdateConnConfig(${idx},'host',this.value)" placeholder="gp-master.prod.internal">
      </div>
      <div class="form-group" style="margin:0">
        <label>Порт</label>
        <input type="text" value="${escAttr(cfg.port || '5432')}"
               oninput="pbUpdateConnConfig(${idx},'port',this.value)" placeholder="5432">
      </div>
      <div class="form-group" style="margin:0">
        <label>База данных</label>
        <input type="text" value="${escAttr(cfg.dbname || '')}"
               oninput="pbUpdateConnConfig(${idx},'dbname',this.value)" placeholder="tsmdm">
      </div>
    </div>
    <div class="step-row-2">
      <div class="form-group" style="margin:0">
        <label>Схема <span class="label-hint">search_path + discovery</span></label>
        <input type="text" value="${escAttr(cfg.schema || '')}"
               oninput="pbUpdateConnConfig(${idx},'schema',this.value)" placeholder="tsmdm_cdh">
      </div>
      <div class="form-group" style="margin:0;max-width:120px">
        <label>GP версия <span class="label-hint">авто</span></label>
        <input type="text" value="${escAttr(cfg.gp_version || '')}"
               oninput="pbUpdateConnConfig(${idx},'gp_version',this.value)" placeholder="6">
      </div>
      <div class="form-group" style="margin:0;max-width:140px">
        <label>sslmode</label>
        <input type="text" value="${escAttr(cfg.sslmode || 'disable')}"
               oninput="pbUpdateConnConfig(${idx},'sslmode',this.value)" placeholder="disable">
      </div>
    </div>
    <div class="step-row-2">
      <div class="form-group" style="margin:0">
        <label>Пользователь</label>
        <input type="text" value="${escAttr(cfg.user || '')}"
               oninput="pbUpdateConnConfig(${idx},'user',this.value)" placeholder="tsmdm_rw">
      </div>
      <div class="form-group" style="margin:0">
        <label>Пароль</label>
        <input type="password" value="${escAttr(cfg.password || '')}"
               oninput="pbUpdateConnConfig(${idx},'password',this.value)" placeholder="">
      </div>
      <div class="form-group" style="margin:0;flex:2">
        <label>cursor_column <span class="label-hint">пусто = OFFSET (медленно на master); поле = инкремент</span></label>
        <input type="text" value="${escAttr(cfg.cursor_column || '')}"
               oninput="pbUpdateConnConfig(${idx},'cursor_column',this.value)" placeholder="last_upd_ts">
      </div>
    </div>`;

  if (type === 'oracle') return `
    <div style="font-size:.72rem;color:var(--muted);margin-bottom:.5rem">
      🔶 Oracle через ODPI-C (нужен Instant Client на сервере). Таблицу-источник
      укажи в поле <strong>«SQL-трансформация»</strong> ниже (имя таблицы или <code>SELECT …</code>).</div>
    <div class="step-row-2">
      <div class="form-group" style="margin:0">
        <label>Хост</label>
        <input type="text" value="${escAttr(cfg.host || '')}"
               oninput="pbUpdateConnConfig(${idx},'host',this.value)" placeholder="oracle.prod.internal">
      </div>
      <div class="form-group" style="margin:0;max-width:100px">
        <label>Порт</label>
        <input type="text" value="${escAttr(cfg.port || '1521')}"
               oninput="pbUpdateConnConfig(${idx},'port',this.value)" placeholder="1521">
      </div>
      <div class="form-group" style="margin:0">
        <label>Service name <span class="label-hint">Easy Connect</span></label>
        <input type="text" value="${escAttr(cfg.service_name || '')}"
               oninput="pbUpdateConnConfig(${idx},'service_name',this.value)" placeholder="ORCL">
      </div>
    </div>
    <div class="step-row-2">
      <div class="form-group" style="margin:0">
        <label>Схема (OWNER)</label>
        <input type="text" value="${escAttr(cfg.schema || '')}"
               oninput="pbUpdateConnConfig(${idx},'schema',this.value)" placeholder="TSMDM_IN">
      </div>
      <div class="form-group" style="margin:0;max-width:140px">
        <label>Oracle версия <span class="label-hint">авто, дефолт 12</span></label>
        <input type="text" value="${escAttr(cfg.oracle_version || '')}"
               oninput="pbUpdateConnConfig(${idx},'oracle_version',this.value)" placeholder="19">
      </div>
    </div>
    <div class="step-row-2">
      <div class="form-group" style="margin:0">
        <label>Пользователь</label>
        <input type="text" value="${escAttr(cfg.user || '')}"
               oninput="pbUpdateConnConfig(${idx},'user',this.value)" placeholder="tsmdm_user">
      </div>
      <div class="form-group" style="margin:0">
        <label>Пароль</label>
        <input type="password" value="${escAttr(cfg.password || '')}"
               oninput="pbUpdateConnConfig(${idx},'password',this.value)" placeholder="">
      </div>
      <div class="form-group" style="margin:0;flex:2">
        <label>cursor_column <span class="label-hint">пусто = OFFSET; поле = инкремент (напр. LAST_UPD)</span></label>
        <input type="text" value="${escAttr(cfg.cursor_column || '')}"
               oninput="pbUpdateConnConfig(${idx},'cursor_column',this.value)" placeholder="LAST_UPD">
      </div>
    </div>`;

  if (type === 'airbyte') {
    /* Step 2 (connector): pick from AIRBYTE_CATALOG, then a JSON pane for
     * the source-specific config (each Airbyte source has its own schema). */
    const sources = Object.entries(AIRBYTE_CATALOG)
      .sort((a,b) => a[1].category.localeCompare(b[1].category) ||
                     a[0].localeCompare(b[0]));
    const optsByCat = {};
    sources.forEach(([name, m]) => {
      (optsByCat[m.category] ||= []).push({name, image: m.image});
    });
    let groups = '';
    for (const cat of Object.keys(optsByCat).sort()) {
      groups += `<optgroup label="${escAttr(cat)}">`;
      for (const s of optsByCat[cat]) {
        groups += `<option value="${escAttr(s.image)}" ${cfg.image===s.image?'selected':''}>${escHtml(s.name)} — ${escHtml(s.image)}</option>`;
      }
      groups += '</optgroup>';
    }
    const innerCfg = cfg.config ? JSON.stringify(cfg.config, null, 2) : '{}';
    return `
      <div class="form-group" style="margin:0">
        <label>Airbyte source image <span class="label-hint">whitelisted; см. AIRBYTE_CATALOG</span></label>
        <select onchange="pbUpdateAirbyteImage(${idx}, this.value)">
          <option value="">— выбери источник —</option>
          ${groups}
        </select>
      </div>
      <div class="form-group" style="margin:.4rem 0 0">
        <label>Source config (JSON) <span class="label-hint">соответствует spec выбранного образа</span></label>
        <textarea class="mono-textarea" rows="4" placeholder='{"start_date":"...", "api_token":"..."}'
                  oninput="pbUpdateAirbyteConfig(${idx}, this.value)">${escHtml(innerCfg)}</textarea>
      </div>`;
  }

  /* fallback: raw JSON */
  return `
    <div class="form-group" style="margin:0">
      <label>Конфигурация коннектора (JSON)</label>
      <textarea class="mono-textarea" rows="2" placeholder="{}"
                oninput="pb.steps[${idx}].connector_config=this.value">${escHtml(step.connector_config || '')}</textarea>
    </div>`;
}

/* Airbyte connector_config = {"image": "...", "config": {…}} */
function pbUpdateAirbyteImage(idx, image) {
  const cfg = safeParse(pb.steps[idx].connector_config, {});
  cfg.image = image;
  if (!cfg.config) cfg.config = {};
  pb.steps[idx].connector_config = JSON.stringify(cfg);
}
function pbUpdateAirbyteConfig(idx, jsonText) {
  const cfg = safeParse(pb.steps[idx].connector_config, {});
  let inner = {};
  try { inner = JSON.parse(jsonText); } catch (_) { return; /* invalid JSON — wait for valid */ }
  cfg.config = inner;
  pb.steps[idx].connector_config = JSON.stringify(cfg);
}

/* ── Save pipeline ── */
/* Persist the current builder state to the server. Returns true on success. */
async function persistBuilderPipeline() {
  const name    = document.getElementById('pb-name').value.trim();
  const cron    = document.getElementById('pb-cron').value.trim();
  const enabled = document.getElementById('pb-enabled').checked;
  const maxRetries = parseInt(document.getElementById('pb-max-retries').value, 10) || 3;
  const retryDelay = parseInt(document.getElementById('pb-retry-delay').value, 10) || 30;
  const webhookUrl = document.getElementById('pb-webhook-url').value.trim();
  const webhookOn  = document.getElementById('pb-webhook-on').value || 'failure';
  const alertCooldown = parseInt(document.getElementById('pb-alert-cooldown').value, 10);

  if (!name) { showToast('Необходимо указать название конвейера', 'error'); return null; }

  const steps = pb.steps.map((s, i) => ({
    ...stepForApi(s),               /* carries scd2_ and any new fields through */
    id:               s.id || `step_${i + 1}`,
    name:             s.name || `Шаг ${i + 1}`,
    connector_type:   s.connector_type   || '',
    connector_config: s.connector_config || '',
    transform_sql:    s.transform_sql    || '',
    target_table:     s.target_table     || '',
    python_code:      s.python_code      || '',
    python_timeout_sec: s.python_timeout_sec || 300,
    scala_code:       s.scala_code       || '',
    scala_timeout_sec: s.scala_timeout_sec || 600,
    max_retries:      s.max_retries != null ? s.max_retries : 3,
    retry_delay_sec:  s.retry_delay_sec != null ? s.retry_delay_sec : 30,
    deps:             s.deps             || [],
    ndeps:            (s.deps || []).length,
  }));
  const triggers = (pb.triggers || []).filter(t => t && t.type).map(t => ({...t}));
  const body = { name, cron, enabled, steps, triggers,
                 max_retries: maxRetries,
                 retry_delay_sec: retryDelay,
                 webhook_url: webhookUrl,
                 webhook_on: webhookOn,
                 alert_cooldown: isNaN(alertCooldown) ? 300 : alertCooldown };
  if (pb.editId) body.id = pb.editId;

  if (pb.editId) {
    await apiFetch(`/api/pipelines/${pb.editId}`, 'DELETE');
  }
  const created = await apiPost('/api/pipelines', body);
  if (created?.id) pb.editId = created.id;
  /* Keep the builder URL in sync with the (possibly new) id. */
  const hash = '#builder/' + (pb.editId || 'new');
  if (location.hash !== hash) history.replaceState({ view: 'builder', id: pb.editId }, '', hash);
  return created;
}

async function savePipeline() {
  try {
    const created = await persistBuilderPipeline();
    if (!created) return;
    const nm = created.name || document.getElementById('pb-name').value.trim();
    showToast(`Конвейер "${nm}" сохранён`, 'ok');
    logActivity(`сохранён конвейер: ${nm}`);
    document.getElementById('nav-builder').classList.remove('visible');
    switchView('pipelines');
    loadPipelines();
  } catch (err) {
    showToast(`Ошибка: ${err}`, 'error');
  }
}


/* ═══════════════════════════════════════════════════
   METRICS
═══════════════════════════════════════════════════ */
async function loadMetrics() {
  try {
    const [raw, tables, pipelines] = await Promise.all([
      apiFetch('/api/metrics').catch(() => null),
      apiFetch('/api/tables').catch(() => []),
      apiFetch('/api/pipelines').catch(() => []),
    ]);
    if (!raw) return; /* logged out or network error */
    const m = raw.metrics ?? raw; /* /api/metrics wraps in {metrics:{...}} */

    /* ── Stat cards ── */
    const grid = document.getElementById('metrics-detail');
    grid.innerHTML = '';
    const fields = [
      ['uptime',                  'Аптайм',                    v => fmtUptime(v)],
      ['total_rows',              'Строк загружено',           v => fmtNum(v)],
      ['total_queries',           'Запросов выполнено',        v => fmtNum(v)],
      ['total_pipelines_run',     'Запусков конвейеров',       v => fmtNum(v)],
      ['avg_query_latency_ms',    'Задержка запроса, мс',      v => v.toFixed(1)],
      ['avg_pipeline_latency_ms', 'Задержка конвейера, мс',   v => v.toFixed(1)],
    ];
    fields.forEach(([key, label, fmt]) => {
      const v    = m[key] ?? 0;
      const card = document.createElement('div');
      card.className = 'stat-card';
      card.innerHTML = `<div class="stat-val">${fmt(v)}</div><div class="stat-label">${label}</div>`;
      grid.appendChild(card);
    });

    /* ── Tables bar chart ── */
    const barList = document.getElementById('metrics-bar-list');
    const sorted  = [...tables].sort((a, b) => (b.rows || 0) - (a.rows || 0)).slice(0, 10);
    const maxRows = Math.max(...sorted.map(t => t.rows || 0), 1);
    if (!sorted.length) {
      barList.innerHTML = '<div class="metrics-empty">Нет таблиц</div>';
    } else {
      barList.innerHTML = sorted.map(t => {
        const pct  = Math.max(2, Math.round(((t.rows || 0) / maxRows) * 100));
        const rows = fmtNum(t.rows || 0);
        return `<div class="mbar-row">
          <div class="mbar-label" title="${escHtml(t.name)}">${escHtml(t.name)}</div>
          <div class="mbar-track"><div class="mbar-fill" style="width:${pct}%"></div></div>
          <div class="mbar-val">${rows}</div>
        </div>`;
      }).join('');
    }

    /* ── Pipelines summary ── */
    const plList = document.getElementById('metrics-pipeline-list');
    if (!pipelines.length) {
      plList.innerHTML = '<div class="metrics-empty">Нет конвейеров</div>';
    } else {
      plList.innerHTML = pipelines.map(p => {
        const st   = p.status || 0;
        const badge= STATUS_BADGE[st]  || '';
        const lbl  = STATUS_LABELS[st] || '';
        const last = p.last_run ? new Date(p.last_run * 1000).toLocaleString() : 'не запускался';
        return `<div class="mpl-row">
          <span class="mpl-name">${escHtml(p.name || p.id)}</span>
          <span class="badge ${badge}" style="flex-shrink:0">${lbl}</span>
          <span class="mpl-meta">${last}</span>
        </div>`;
      }).join('');
    }

  } catch (err) {
    document.getElementById('metrics-detail').innerHTML =
      `<div style="color:var(--red)">${escHtml(String(err))}</div>`;
  }
}

function restartMetricsTimer() {
  if (metricsTimer) clearInterval(metricsTimer);
  if (prefs.autoRefresh) {
    const ms = Math.max(5000, (prefs.refreshInterval || 30) * 1000);
    metricsTimer = setInterval(() => {
      if (document.getElementById('view-metrics').classList.contains('active'))
        loadMetrics();
    }, ms);
  }
}


/* ═══════════════════════════════════════════════════
   ANALYTICS — Visual Chart Builder
═══════════════════════════════════════════════════ */

const CHART_PALETTE = ['#5b6ef5','#34c759','#ff9f0a','#ff375f','#bf5af2',
                       '#5ac8fa','#ff6b35','#30d158','#f5a623','#00b4d8',
                       '#a8e063','#c77dff'];
const chartStateMap = new Map();
let chartIdCounter = 0;

function newChartCfg(overrides = {}) {
  return { id:'ch'+(++chartIdCounter), title:'График '+chartIdCounter, xCol:'', yCol:'', type:'bar', agg:'sum', topN:20, ...overrides };
}

function switchAnTab(tab) {
  document.getElementById('an-pane-charts').style.display = tab === 'charts' ? '' : 'none';
  document.getElementById('an-pane-stats').style.display  = tab === 'stats'  ? '' : 'none';
  document.querySelectorAll('.an-tab').forEach(t =>
    t.classList.toggle('active', t.dataset.tab === tab));
}

async function loadAnalyticsModule() {
  showAnScreen('list');
  loadSavedResults();
  /* prefetch tables for editor */
  try {
    const tables = await apiFetch('/api/tables');
    analyticsState.tables = tables || [];
  } catch(_) {}
}

function showAnScreen(screen) {
  document.getElementById('an-list-screen').style.display   = screen === 'list'   ? '' : 'none';
  document.getElementById('an-view-screen').style.display   = screen === 'view'   ? '' : 'none';
  document.getElementById('an-editor-screen').style.display = screen === 'editor' ? '' : 'none';
}

function openNewAnalytics() {
  document.getElementById('an-editor-title').textContent = 'Новая аналитика';
  document.getElementById('an-sql').value = '';
  document.getElementById('an-status').textContent = '';
  document.getElementById('an-chart-area').style.display = 'none';
  document.getElementById('an-save-btn').style.display = 'none';
  analyticsState.currentRows = []; analyticsState.currentCols = []; analyticsState.charts = [];
  /* fill table pills */
  const pills = document.getElementById('an-table-pills');
  if (pills) pills.innerHTML = analyticsState.tables.length
    ? analyticsState.tables.map(t =>
        `<button class="btn btn-sm" onclick="anQuickTable('${escAttr(t.name)}')">${escHtml(t.name)}</button>`
      ).join('')
    : '<span style="color:var(--muted);font-size:.85rem">Нет таблиц</span>';
  showAnScreen('editor');
}

function closeAnalyticsEditor() {
  showAnScreen('list');
}

function anQuickTable(name) {
  const el = document.getElementById('an-sql');
  if (el) el.value = `SELECT * FROM ${name} LIMIT 500`;
  runAnalyticsQuery();
}

async function runAnalyticsQuery() {
  const sqlEl = document.getElementById('an-sql');
  const sql = (sqlEl?.value || '').trim();
  if (!sql) { setText('an-status', 'Введите SQL запрос'); return; }
  setText('an-status', 'Выполнение…');
  const t0 = Date.now();
  try {
    const data = await apiPost('/api/tables/query', { sql });
    const cols = data.columns || [];
    const srcRows = data.rows || [];
    const rows = srcRows.map(r => {
      const obj = {};
      const vals = Array.isArray(r) ? r : cols.map(c => r[c]);
      cols.forEach((c, i) => { obj[c] = vals[i]; });
      return obj;
    });
    analyticsState.currentRows = rows;
    analyticsState.currentCols = cols;
    setText('an-status', `${rows.length} строк · ${((Date.now()-t0)/1000).toFixed(2)}с`);
    document.getElementById('an-chart-area').style.display = '';
    document.getElementById('an-save-btn').style.display = '';
    switchAnTab('charts');
    renderAnalyticsDataTable(rows, cols);
    fillStatsSelectors(cols);
    onStatMethodChange();
    if (!analyticsState.charts.length) makeDefaultCharts(cols, rows);
    else buildAllCharts();
  } catch(err) {
    let msg = String(err);
    try { const j = JSON.parse(msg.replace(/^Error:\s*/, '')); if (j.error) msg = j.error; } catch(_) {}
    setText('an-status', `Ошибка: ${msg}`);
  }
}

/* ── Chart management ── */
function makeDefaultCharts(cols, rows) {
  const numCols  = cols.filter(c => rows.slice(0,20).some(r => Number.isFinite(Number(r[c])) && r[c] !== ''));
  const lblCols  = cols.filter(c => !numCols.includes(c));
  const xCands   = lblCols.filter(c => !/^id$|_id$/i.test(c));
  const xCol     = xCands[0] || lblCols[0] || cols[0] || '';
  const metrics  = numCols.filter(c => isLikelyMetricColumn(c));
  const nonId    = numCols.filter(c => !/^id$|_id$/i.test(c));
  const yCol     = metrics[0] || nonId[0] || numCols[0] || cols[1] || '';
  const dateCol  = lblCols.find(c => /date|time|month|year|day|week/i.test(c));
  clearAllCharts();
  addAnalyticsChart('bar',  { xCol, yCol });
  if (dateCol && yCol)       addAnalyticsChart('area',    { xCol: dateCol, yCol });
  else if (nonId.length >= 2) addAnalyticsChart('scatter', { xCol: nonId[0], yCol: nonId[1] });
}

function clearAllCharts() {
  analyticsState.charts = [];
  chartStateMap.clear();
  chartIdCounter = 0;
  const g = document.getElementById('an-charts-grid');
  if (g) g.innerHTML = '';
}

function addAnalyticsChart(typeHint = 'bar', overrides = {}) {
  const cols = analyticsState.currentCols;
  const rows = analyticsState.currentRows;
  if (!cols.length) return;
  let { xCol, yCol } = overrides;
  if (!xCol || !yCol) {
    const numCols = cols.filter(c => rows.slice(0,20).some(r => Number.isFinite(Number(r[c])) && r[c] !== ''));
    const lblCols = cols.filter(c => !numCols.includes(c));
    const xCands  = lblCols.filter(c => !/^id$|_id$/i.test(c));
    if (!xCol) xCol = xCands[0] || lblCols[0] || cols[0] || '';
    const metrics = numCols.filter(c => isLikelyMetricColumn(c));
    const nonId   = numCols.filter(c => !/^id$|_id$/i.test(c));
    if (!yCol) yCol = metrics[0] || nonId[0] || numCols[0] || cols[1] || '';
  }
  const cfg = newChartCfg({ type: typeHint, xCol, yCol });
  analyticsState.charts.push(cfg);
  const grid = document.getElementById('an-charts-grid');
  if (!grid) return;
  const wrap = document.createElement('div');
  wrap.innerHTML = renderChartCardHtml(cfg);
  grid.appendChild(wrap.firstElementChild);
  requestAnimationFrame(() => buildChartById(cfg.id));
}

function removeAnalyticsChart(id) {
  analyticsState.charts = analyticsState.charts.filter(c => c.id !== id);
  chartStateMap.delete(id);
  document.getElementById('cc_' + id)?.remove();
}

function anChartSet(id, prop, val) {
  const cfg = analyticsState.charts.find(c => c.id === id);
  if (!cfg) return;
  cfg[prop] = prop === 'topN' ? (Math.max(2, Number(val)) || 20) : val;
  requestAnimationFrame(() => { buildChartById(id); observeChartResize(id); });
}

function buildAllCharts() {
  analyticsState.charts.forEach(cfg => requestAnimationFrame(() => buildChartById(cfg.id)));
}

function buildChartById(id) {
  const cfg  = analyticsState.charts.find(c => c.id === id);
  if (!cfg) return;
  const rows = analyticsState.currentRows || [];
  if (!rows.length) return;
  const pts  = aggregateForChart(cfg, rows);
  const aggL = cfg.agg === 'count' ? 'COUNT(*)' : `${cfg.agg.toUpperCase()}(${cfg.yCol})`;
  drawChartOnCanvas(id, pts, cfg.type, aggL, cfg.xCol);
  renderChartLegend(id, pts, cfg.type);
}

/* backward-compat alias used by stats module */
function buildAnalyticsChart() { buildAllCharts(); }

/* ── Chart card HTML ── */
/* ── Drag-and-drop reorder ──
   draggable="true" lives on the ⠿ handle span only.
   dragover/drop/dragleave live on each card via currentTarget.
── */
let anDragSrcId = null;

function anDragStartHandle(ev, id) {
  anDragSrcId = id;
  ev.dataTransfer.effectAllowed = 'move';
  ev.dataTransfer.setData('text/plain', id);
  /* use the whole card as ghost image */
  const card = document.getElementById('cc_' + id);
  if (card && ev.dataTransfer.setDragImage) ev.dataTransfer.setDragImage(card, 24, 24);
  setTimeout(() => card?.classList.add('an-dragging'), 0);
}
function anDragOver(ev) {
  if (!anDragSrcId) return;
  const card = ev.currentTarget;
  if (card.id === 'cc_' + anDragSrcId) return;
  ev.preventDefault();
  ev.dataTransfer.dropEffect = 'move';
  document.querySelectorAll('.an-chart-card').forEach(c => c.classList.remove('an-drag-over'));
  card.classList.add('an-drag-over');
}
function anDragLeave(ev) {
  if (!ev.currentTarget.contains(ev.relatedTarget))
    ev.currentTarget.classList.remove('an-drag-over');
}
function anDragEnd() {
  document.querySelectorAll('.an-chart-card').forEach(c => c.classList.remove('an-dragging','an-drag-over'));
  anDragSrcId = null;
}
function anDrop(ev) {
  ev.preventDefault();
  const srcId = anDragSrcId;
  const tgtEl = ev.currentTarget;
  anDragEnd();
  if (!srcId) return;
  const tgtId = tgtEl.id.replace('cc_', '');
  if (srcId === tgtId) return;
  const grid = document.getElementById('an-charts-grid');
  const srcEl = document.getElementById('cc_' + srcId);
  if (!srcEl || !tgtEl || !grid) return;
  const kids = [...grid.children];
  if (kids.indexOf(srcEl) < kids.indexOf(tgtEl)) tgtEl.after(srcEl);
  else tgtEl.before(srcEl);
  const newOrder = [...grid.children].map(el => el.id.replace('cc_','')).filter(Boolean);
  analyticsState.charts.sort((a,b) => newOrder.indexOf(a.id) - newOrder.indexOf(b.id));
}

/* ── Mouse resize: right edge (width) and bottom edge (height) ── */
let _anResizeRAF = null;
function _anRAFRedraw(id) {
  if (_anResizeRAF) cancelAnimationFrame(_anResizeRAF);
  _anResizeRAF = requestAnimationFrame(() => { buildChartById(id); _anResizeRAF = null; });
}

function anResizeRight(ev, id) {
  ev.preventDefault();
  ev.stopPropagation();
  const card = document.getElementById('cc_' + id);
  if (!card) return;
  const startX = ev.clientX;
  const widths = ['1', '2', 'full'];
  const startIdx = Math.max(0, widths.indexOf(card.dataset.width || '1'));
  let lastIdx = startIdx;

  const onMove = e => {
    const delta = e.clientX - startX;
    let idx = startIdx;
    if      (delta >  140) idx = Math.min(startIdx + 2, 2);
    else if (delta >   60) idx = Math.min(startIdx + 1, 2);
    else if (delta < -140) idx = Math.max(startIdx - 2, 0);
    else if (delta <  -60) idx = Math.max(startIdx - 1, 0);
    if (idx !== lastIdx) {
      lastIdx = idx;
      card.dataset.width = widths[idx];
      _anRAFRedraw(id);
    }
  };
  const onUp = () => {
    document.removeEventListener('mousemove', onMove);
    document.removeEventListener('mouseup', onUp);
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  };
  document.body.style.cursor = 'ew-resize';
  document.body.style.userSelect = 'none';
  document.addEventListener('mousemove', onMove);
  document.addEventListener('mouseup', onUp);
}

function anResizeBottom(ev, id) {
  ev.preventDefault();
  const wrap = document.getElementById('cw_' + id);
  if (!wrap) return;
  const startY = ev.clientY;
  const startH = wrap.getBoundingClientRect().height;

  const onMove = e => {
    wrap.style.height = Math.max(180, startH + (e.clientY - startY)) + 'px';
    _anRAFRedraw(id);
  };
  const onUp = () => {
    document.removeEventListener('mousemove', onMove);
    document.removeEventListener('mouseup', onUp);
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  };
  document.body.style.cursor = 'ns-resize';
  document.body.style.userSelect = 'none';
  document.addEventListener('mousemove', onMove);
  document.addEventListener('mouseup', onUp);
}

function observeChartResize(id) {
  /* no-op: resizing is driven by anResizeBottom/anResizeRight directly */
}

function renderChartCardHtml(cfg) {
  const cols = analyticsState.currentCols;
  const colOpts = sel => cols.map(c => `<option value="${escAttr(c)}"${sel===c?' selected':''}>${escHtml(c)}</option>`).join('');
  const types = [
    ['bar','📊 Столбчатая'],['line','📈 Линия'],['area','🌊 Область'],['pie','🥧 Круговая'],
    ['scatter','⬤ Точечная'],['histogram','📉 Распределение'],['waterfall','💧 Водопад'],['funnel','🔽 Воронка']
  ];
  const typeOpts = types.map(([v,l]) => `<option value="${v}"${cfg.type===v?' selected':''}>${l}</option>`).join('');
  const aggs = [['sum','Σ Сумма'],['avg','∅ Среднее'],['count','# Кол-во'],['min','↓ Мин'],['max','↑ Макс']];
  const aggOpts = aggs.map(([v,l]) => `<option value="${v}"${cfg.agg===v?' selected':''}>${l}</option>`).join('');
  const id = cfg.id;
  const noAgg = cfg.type === 'scatter' || cfg.type === 'histogram';
  return `<div class="card an-chart-card" id="cc_${id}" data-width="1"
  ondragover="anDragOver(event)" ondrop="anDrop(event)"
  ondragend="anDragEnd()" ondragleave="anDragLeave(event)">
  <div class="an-chart-card-header">
    <span class="an-drag-handle" draggable="true"
          ondragstart="anDragStartHandle(event,'${id}')"
          title="Перетащить для перестановки">⠿</span>
    <input class="an-chart-title-input" value="${escAttr(cfg.title)}"
           oninput="analyticsState.charts.find(c=>c.id==='${id}').title=this.value">
    <div class="an-chart-card-btns">
      <button class="btn btn-sm" onclick="downloadChartById('${id}')" title="Скачать PNG">⬇</button>
      <button class="btn btn-sm" onclick="removeAnalyticsChart('${id}')" title="Удалить">✕</button>
    </div>
  </div>
  <div class="an-chart-controls-row">
    <div class="form-group"><label>Тип</label>
      <select onchange="anChartSet('${id}','type',this.value)">${typeOpts}</select></div>
    <div class="form-group" style="flex:1.5"><label>Ось X</label>
      <select onchange="anChartSet('${id}','xCol',this.value)">${colOpts(cfg.xCol)}</select></div>
    <div class="form-group" style="flex:1.5"><label>Ось Y</label>
      <select onchange="anChartSet('${id}','yCol',this.value)">${colOpts(cfg.yCol)}</select></div>
    <div class="form-group"${noAgg?' style="display:none"':''}><label>Агрег.</label>
      <select onchange="anChartSet('${id}','agg',this.value)">${aggOpts}</select></div>
    <div class="form-group" style="flex:0.65;min-width:55px"><label>Top N</label>
      <input type="number" value="${cfg.topN}" min="2" max="500" style="width:100%"
             oninput="anChartSet('${id}','topN',this.value)"></div>
  </div>
  <div id="cw_${id}" class="an-chart-canvas-wrap">
    <canvas id="c_${id}"></canvas>
    <div id="ct_${id}" class="analytics-tooltip hidden"></div>
    <div class="an-edge-bottom" onmousedown="anResizeBottom(event,'${id}')" title="Растянуть по высоте"></div>
  </div>
  <div id="cl_${id}" class="analytics-legend"></div>
  <div class="an-edge-right" onmousedown="anResizeRight(event,'${id}')" title="Растянуть по ширине"></div>
</div>`;
}

/* ── Data aggregation ── */
function aggregateForChart(cfg, rows) {
  const { xCol, yCol, type, agg } = cfg;
  const N = Math.max(2, cfg.topN || 20);

  if (type === 'scatter') {
    return rows.map(r => ({ x: Number(r[xCol])||0, y: Number(r[yCol])||0,
      label:`(${r[xCol]},${r[yCol]})`, value: Number(r[yCol])||0 })).slice(0, 2000);
  }
  if (type === 'histogram') return computeHistoBuckets(rows, yCol);

  const map = new Map();
  rows.forEach(r => {
    const key = String(r[xCol] ?? '');
    const v = Number(r[yCol]) || 0;
    if (!map.has(key)) map.set(key, { label:key, sum:0, n:0, min:Infinity, max:-Infinity });
    const e = map.get(key);
    e.sum += v; e.n++;
    if (v < e.min) e.min = v;
    if (v > e.max) e.max = v;
  });
  const getV = e =>
    agg==='avg'   ? (e.n ? e.sum/e.n : 0) :
    agg==='count' ? e.n :
    agg==='min'   ? (isFinite(e.min)?e.min:0) :
    agg==='max'   ? (isFinite(e.max)?e.max:0) : e.sum;

  let pts = [...map.values()].map(e => ({ label:e.label, value:getV(e), count:e.n }));
  if (type==='line'||type==='area') {
    pts.sort((a,b) => { const na=Number(a.label),nb=Number(b.label);
      return (Number.isFinite(na)&&Number.isFinite(nb)) ? na-nb : a.label.localeCompare(b.label,'ru'); });
  } else if (type!=='waterfall') {
    pts.sort((a,b) => b.value - a.value);
  }
  if (pts.length > N && type!=='line' && type!=='area' && type!=='waterfall') {
    const rest = pts.slice(N).reduce((s,p) => s+p.value, 0);
    pts = pts.slice(0, N);
    if (Math.abs(rest) > 0) pts.push({ label:'Остальные', value:rest, count:0 });
  }
  return pts;
}

function computeHistoBuckets(rows, col, bins = 12) {
  const vals = rows.map(r => Number(r[col])).filter(Number.isFinite);
  if (!vals.length) return [];
  const mn = Math.min(...vals), mx = Math.max(...vals);
  if (mn === mx) return [{ label:String(mn), value:vals.length }];
  const step = (mx-mn)/bins;
  const counts = new Array(bins).fill(0);
  vals.forEach(v => counts[Math.min(bins-1, Math.floor((v-mn)/step))]++);
  return counts.map((cnt,i) => ({ label:`${shortNum(mn+i*step)}–${shortNum(mn+(i+1)*step)}`, value:cnt }));
}

/* ── Main draw dispatcher ── */
function drawChartOnCanvas(chartId, points, type, metricLabel, xLabel) {
  const canvas  = document.getElementById('c_' + chartId);
  const tooltip = document.getElementById('ct_' + chartId);
  const wrap    = document.getElementById('cw_' + chartId);
  if (!canvas || !wrap) return;
  const dpr = window.devicePixelRatio || 1;
  const W = wrap.clientWidth;
  if (W < 10) return;
  /* use wrap's explicit height if user resized it, else derive from width */
  const wrapH = wrap.clientHeight;
  const H = wrapH > 50 ? wrapH : Math.max(320, Math.round(W * 0.5));
  canvas.style.width = W+'px'; canvas.style.height = H+'px';
  canvas.width = Math.round(W*dpr); canvas.height = Math.round(H*dpr);
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  const cs     = getComputedStyle(document.documentElement);
  const bg     = cs.getPropertyValue('--surface').trim()||'#181c27';
  const gridC  = cs.getPropertyValue('--border').trim()||'#2a2f3d';
  const muteC  = cs.getPropertyValue('--muted').trim()||'#8e8e93';
  const textC  = cs.getPropertyValue('--text').trim()||'#e0e0e0';
  const accent = cs.getPropertyValue('--accent').trim()||'#5b6ef5';
  ctx.fillStyle = bg; ctx.fillRect(0,0,W,H);
  const state = { hitboxes:[], type, points, total:points.reduce((s,p)=>s+Math.max(0,p.value||0),0), dpr };
  chartStateMap.set(chartId, state);
  bindChartHover(chartId, canvas, tooltip);
  if (tooltip) tooltip.classList.add('hidden');
  if (!points.length) { ctx.fillStyle=muteC; ctx.font='14px system-ui,sans-serif'; ctx.fillText('Нет данных',20,44); return; }
  const C = { bg, gridC, muteC, textC, accent };
  switch (type) {
    case 'pie':       return drawChartPie(ctx, points, W, H, metricLabel, C, state);
    case 'scatter':   return drawChartScatter(ctx, points, W, H, metricLabel, xLabel, C, state);
    case 'funnel':    return drawChartFunnel(ctx, points, W, H, metricLabel, C, state);
    case 'waterfall': return drawChartWaterfall(ctx, points, W, H, metricLabel, C, state);
    default:          return drawChartAxes(ctx, points, type, W, H, metricLabel, C, state);
  }
}

/* ── niceYTicks ── */
function niceYTicks(rawMin, rawMax, count = 5) {
  if (rawMin === rawMax) { rawMax = rawMin + 1; }
  const range = rawMax - rawMin, rough = range/count;
  const mag = Math.pow(10, Math.floor(Math.log10(rough)));
  const f = rough/mag;
  const step = f<=1?mag : f<=2?2*mag : f<=5?5*mag : 10*mag;
  const lo = Math.floor(rawMin/step)*step, hi = Math.ceil(rawMax/step)*step;
  const ticks = [];
  for (let v=lo; v<=hi+step*0.001; v+=step) ticks.push(Math.round(v*1e9)/1e9);
  return ticks;
}

/* ── Bar / Line / Area / Histogram ── */
function drawChartAxes(ctx, points, type, W, H, metricLabel, C, state) {
  const { bg, gridC, muteC, textC, accent } = C;
  const pad = { l:76, r:20, t:42, b:88 };
  const plotW = W-pad.l-pad.r, plotH = H-pad.t-pad.b;
  const rawMax = Math.max(...points.map(p=>p.value));
  const rawMin = (type==='line'||type==='area') ? Math.min(...points.map(p=>p.value)) : Math.min(0,...points.map(p=>p.value));
  const ticks  = niceYTicks(rawMin, rawMax);
  const axMin  = ticks[0], axMax = ticks[ticks.length-1];
  const axSpan = Math.max(1e-9, axMax-axMin);
  const yPx    = v => pad.t + plotH*(1-(v-axMin)/axSpan);
  const zeroY  = yPx(0);
  ctx.font = '11px system-ui,sans-serif';
  ticks.forEach(v => {
    const gy = yPx(v);
    if (gy<pad.t-2||gy>pad.t+plotH+2) return;
    ctx.strokeStyle=gridC; ctx.lineWidth=1; ctx.setLineDash([3,3]);
    ctx.beginPath(); ctx.moveTo(pad.l,gy); ctx.lineTo(W-pad.r,gy); ctx.stroke();
    ctx.setLineDash([]); ctx.fillStyle=muteC; ctx.textAlign='right';
    ctx.fillText(shortNum(v), pad.l-8, gy+4);
  });
  ctx.textAlign='left';
  ctx.strokeStyle=gridC; ctx.lineWidth=1.5;
  ctx.beginPath(); ctx.moveTo(pad.l,pad.t); ctx.lineTo(pad.l,pad.t+plotH); ctx.lineTo(pad.l+plotW,pad.t+plotH); ctx.stroke();
  if (axMin<0&&axMax>0) { ctx.strokeStyle=muteC; ctx.lineWidth=1.5; ctx.beginPath(); ctx.moveTo(pad.l,zeroY); ctx.lineTo(W-pad.r,zeroY); ctx.stroke(); }

  if (type==='bar'||type==='histogram') {
    const slotW=plotW/points.length, barW=Math.max(4,Math.min(64,slotW*0.72));
    const showV=barW>=22;
    ctx.font='bold 10px system-ui,sans-serif';
    points.forEach((p,i) => {
      const color=CHART_PALETTE[type==='histogram'?0:i%CHART_PALETTE.length];
      const top=yPx(Math.max(p.value,0)), bot=yPx(Math.min(p.value,0)), bh=Math.max(1,bot-top);
      const bx=pad.l+i*slotW+(slotW-barW)/2;
      ctx.fillStyle=color; ctx.beginPath();
      if (ctx.roundRect) ctx.roundRect(bx,top,barW,bh,p.value>=0?[3,3,0,0]:[0,0,3,3]); else ctx.rect(bx,top,barW,bh);
      ctx.fill();
      if (showV) {
        ctx.textAlign='center';
        if (top-5>pad.t+10) { ctx.fillStyle=textC; ctx.fillText(shortNum(p.value),bx+barW/2,top-5); }
        else { ctx.fillStyle='#fff'; ctx.fillText(shortNum(p.value),bx+barW/2,top+13); }
      }
      state.hitboxes.push({ x:bx,y:top-2,w:barW,h:bh+4,label:p.label,value:p.value,cx:bx+barW/2,cy:top-16,type:'bar' });
    });
    ctx.textAlign='left';
  } else {
    const n=points.length, xOf=i=>pad.l+(n<2?plotW/2:(i/(n-1))*plotW);
    if (type==='area') {
      ctx.beginPath(); points.forEach((p,i)=>{ const px=xOf(i),py=yPx(p.value); i===0?ctx.moveTo(px,py):ctx.lineTo(px,py); });
      ctx.lineTo(xOf(n-1),zeroY); ctx.lineTo(xOf(0),zeroY); ctx.closePath(); ctx.fillStyle=accent+'33'; ctx.fill();
    }
    ctx.beginPath(); points.forEach((p,i)=>{ const px=xOf(i),py=yPx(p.value); i===0?ctx.moveTo(px,py):ctx.lineTo(px,py); });
    ctx.strokeStyle=accent; ctx.lineWidth=2.5; ctx.lineJoin='round'; ctx.stroke();
    const showDots=n<=120;
    points.forEach((p,i) => {
      const px=xOf(i),py=yPx(p.value);
      if (showDots) { ctx.beginPath(); ctx.arc(px,py,4.5,0,Math.PI*2); ctx.fillStyle=bg; ctx.fill(); ctx.beginPath(); ctx.arc(px,py,3,0,Math.PI*2); ctx.fillStyle=accent; ctx.fill(); }
      state.hitboxes.push({ x:px-12,y:py-12,w:24,h:24,label:p.label,value:p.value,cx:px,cy:py-18,type:'point' });
    });
  }
  ctx.fillStyle=muteC; ctx.font='11px system-ui,sans-serif';
  const evN=Math.max(1,Math.ceil(points.length/16));
  points.forEach((p,i) => {
    if (i%evN!==0&&i!==points.length-1) return;
    const lx=(type==='bar'||type==='histogram') ? pad.l+i*(plotW/points.length)+plotW/points.length/2
                                                 : pad.l+(points.length<2?plotW/2:(i/(points.length-1))*plotW);
    ctx.save(); ctx.translate(lx,H-56); ctx.rotate(-0.55); ctx.textAlign='right'; ctx.fillText(shortenLabel(String(p.label),18),0,0); ctx.restore();
  });
  ctx.textAlign='left';
  ctx.save(); ctx.translate(13,pad.t+plotH/2); ctx.rotate(-Math.PI/2); ctx.textAlign='center'; ctx.fillStyle=muteC; ctx.font='11px system-ui,sans-serif'; ctx.fillText(metricLabel,0,0); ctx.restore(); ctx.textAlign='left';
}

/* ── Scatter ── */
function drawChartScatter(ctx, points, W, H, metricLabel, xLabel, C, state) {
  const { bg, gridC, muteC } = C;
  const pad={l:76,r:20,t:30,b:70};
  const plotW=W-pad.l-pad.r, plotH=H-pad.t-pad.b;
  const xs=points.map(p=>p.x), ys=points.map(p=>p.y);
  const xTicks=niceYTicks(Math.min(...xs),Math.max(...xs)), yTicks=niceYTicks(Math.min(...ys),Math.max(...ys));
  const axXmin=xTicks[0],axXmax=xTicks[xTicks.length-1],axYmin=yTicks[0],axYmax=yTicks[yTicks.length-1];
  const xOf=v=>pad.l+(v-axXmin)/Math.max(1e-9,axXmax-axXmin)*plotW;
  const yOf=v=>pad.t+plotH*(1-(v-axYmin)/Math.max(1e-9,axYmax-axYmin));
  ctx.font='11px system-ui,sans-serif';
  yTicks.forEach(v => { const gy=yOf(v); ctx.strokeStyle=gridC; ctx.lineWidth=1; ctx.setLineDash([3,3]); ctx.beginPath(); ctx.moveTo(pad.l,gy); ctx.lineTo(W-pad.r,gy); ctx.stroke(); ctx.setLineDash([]); ctx.fillStyle=muteC; ctx.textAlign='right'; ctx.fillText(shortNum(v),pad.l-8,gy+4); });
  xTicks.forEach(v => { const gx=xOf(v); ctx.strokeStyle=gridC; ctx.lineWidth=1; ctx.setLineDash([3,3]); ctx.beginPath(); ctx.moveTo(gx,pad.t); ctx.lineTo(gx,pad.t+plotH); ctx.stroke(); ctx.setLineDash([]); ctx.fillStyle=muteC; ctx.textAlign='center'; ctx.fillText(shortNum(v),gx,pad.t+plotH+14); });
  ctx.textAlign='left';
  ctx.strokeStyle=gridC; ctx.lineWidth=1.5; ctx.beginPath(); ctx.moveTo(pad.l,pad.t); ctx.lineTo(pad.l,pad.t+plotH); ctx.lineTo(pad.l+plotW,pad.t+plotH); ctx.stroke();
  const r=Math.max(2.5,Math.min(5,280/Math.sqrt(points.length)));
  points.forEach((p,i) => {
    const px=xOf(p.x), py=yOf(p.y);
    ctx.beginPath(); ctx.arc(px,py,r,0,Math.PI*2); ctx.fillStyle=CHART_PALETTE[i%CHART_PALETTE.length]+'bb'; ctx.fill();
    state.hitboxes.push({ x:px-8,y:py-8,w:16,h:16,label:p.label,value:p.y,cx:px,cy:py-14,type:'point' });
  });
  ctx.fillStyle=muteC; ctx.font='11px system-ui,sans-serif';
  ctx.save(); ctx.translate(13,pad.t+plotH/2); ctx.rotate(-Math.PI/2); ctx.textAlign='center'; ctx.fillText(metricLabel,0,0); ctx.restore();
  ctx.textAlign='center'; ctx.fillText(xLabel,pad.l+plotW/2,H-8); ctx.textAlign='left';
}

/* ── Waterfall ── */
function drawChartWaterfall(ctx, points, W, H, metricLabel, C, state) {
  const { bg, gridC, muteC, textC } = C;
  const pad={l:76,r:20,t:42,b:88};
  const plotW=W-pad.l-pad.r, plotH=H-pad.t-pad.b;
  let running=0;
  const bars=points.map(p => { const from=running; running+=p.value; return { label:p.label, from, to:running, value:p.value }; });
  const allV=bars.flatMap(b=>[b.from,b.to]);
  const ticks=niceYTicks(Math.min(...allV),Math.max(...allV));
  const axMin=ticks[0],axMax=ticks[ticks.length-1],axSpan=Math.max(1e-9,axMax-axMin);
  const yPx=v=>pad.t+plotH*(1-(v-axMin)/axSpan);
  ctx.font='11px system-ui,sans-serif';
  ticks.forEach(v => { const gy=yPx(v); ctx.strokeStyle=gridC; ctx.lineWidth=1; ctx.setLineDash([3,3]); ctx.beginPath(); ctx.moveTo(pad.l,gy); ctx.lineTo(W-pad.r,gy); ctx.stroke(); ctx.setLineDash([]); ctx.fillStyle=muteC; ctx.textAlign='right'; ctx.fillText(shortNum(v),pad.l-8,gy+4); });
  ctx.textAlign='left'; ctx.strokeStyle=gridC; ctx.lineWidth=1.5; ctx.beginPath(); ctx.moveTo(pad.l,pad.t); ctx.lineTo(pad.l,pad.t+plotH); ctx.lineTo(pad.l+plotW,pad.t+plotH); ctx.stroke();
  const slotW=plotW/bars.length, barW=Math.max(4,Math.min(60,slotW*0.68));
  ctx.font='bold 10px system-ui,sans-serif';
  bars.forEach((b,i) => {
    const color=b.value>=0?'#34c759':'#ff375f';
    const top=yPx(Math.max(b.from,b.to)), bot=yPx(Math.min(b.from,b.to)), bh=Math.max(1,bot-top);
    const bx=pad.l+i*slotW+(slotW-barW)/2;
    ctx.fillStyle=color; ctx.beginPath();
    if (ctx.roundRect) ctx.roundRect(bx,top,barW,bh,[3,3,0,0]); else ctx.rect(bx,top,barW,bh);
    ctx.fill();
    if (i<bars.length-1) { ctx.strokeStyle=muteC; ctx.lineWidth=1; ctx.setLineDash([4,4]); ctx.beginPath(); ctx.moveTo(bx+barW,yPx(b.to)); ctx.lineTo(pad.l+(i+1)*slotW,yPx(b.to)); ctx.stroke(); ctx.setLineDash([]); }
    const lbl=(b.value>=0?'+':'')+shortNum(b.value);
    ctx.textAlign='center';
    if (top-5>pad.t+10) { ctx.fillStyle=textC; ctx.fillText(lbl,bx+barW/2,top-5); } else { ctx.fillStyle='#fff'; ctx.fillText(lbl,bx+barW/2,top+13); }
    state.hitboxes.push({ x:bx,y:top-2,w:barW,h:bh+4,label:b.label,value:b.value,cx:bx+barW/2,cy:top-16,type:'bar' });
  });
  ctx.textAlign='left';
  const evN=Math.max(1,Math.ceil(bars.length/16));
  bars.forEach((b,i) => { if (i%evN!==0&&i!==bars.length-1) return; const lx=pad.l+i*slotW+slotW/2; ctx.save(); ctx.translate(lx,H-56); ctx.rotate(-0.55); ctx.textAlign='right'; ctx.fillStyle=muteC; ctx.font='11px system-ui,sans-serif'; ctx.fillText(shortenLabel(b.label,18),0,0); ctx.restore(); });
  ctx.textAlign='left';
}

/* ── Funnel ── */
function drawChartFunnel(ctx, points, W, H, metricLabel, C, state) {
  const { muteC, textC } = C;
  const slices=points.slice(0,20).sort((a,b)=>b.value-a.value);
  if (!slices.length) return;
  const pad={l:20,r:20,t:30,b:20};
  const plotW=W-pad.l-pad.r, plotH=H-pad.t-pad.b;
  const rowH=Math.max(24,plotH/slices.length), maxV=slices[0].value||1;
  slices.forEach((p,i) => {
    const bw=Math.max(16,(p.value/maxV)*plotW*0.92), bx=pad.l+(plotW-bw)/2;
    const by=pad.t+i*rowH+2, bh=Math.max(18,rowH-4);
    ctx.fillStyle=CHART_PALETTE[i%CHART_PALETTE.length]; ctx.beginPath();
    if (ctx.roundRect) ctx.roundRect(bx,by,bw,bh,4); else ctx.rect(bx,by,bw,bh);
    ctx.fill();
    ctx.fillStyle='#fff'; ctx.font=`bold ${Math.min(13,bh*0.55)}px system-ui,sans-serif`;
    ctx.textAlign='left'; ctx.textBaseline='middle'; ctx.fillText(shortenLabel(p.label,14),bx+10,by+bh/2);
    ctx.textAlign='right'; ctx.fillText(shortNum(p.value),bx+bw-8,by+bh/2);
    ctx.textBaseline='alphabetic';
    state.hitboxes.push({ x:bx,y:by,w:bw,h:bh,label:p.label,value:p.value,cx:bx+bw/2,cy:by-8,type:'bar' });
  });
  ctx.textAlign='center'; ctx.fillStyle=muteC; ctx.font='11px system-ui,sans-serif'; ctx.fillText(metricLabel,W/2,H-6); ctx.textAlign='left';
}

/* ── Pie/Donut ── */
function drawChartPie(ctx, points, W, H, metricLabel, C, state) {
  const { bg, muteC, textC } = C;
  let slices=points.filter(p=>p.value>0);
  if (!slices.length) { ctx.fillStyle=muteC; ctx.font='13px system-ui'; ctx.fillText('Нет данных',20,40); return; }
  if (slices.length>CHART_PALETTE.length) {
    const rest=slices.slice(CHART_PALETTE.length-1).reduce((s,p)=>s+p.value,0);
    slices=[...slices.slice(0,CHART_PALETTE.length-1),{ label:'Остальные', value:rest }];
  }
  const total=slices.reduce((s,p)=>s+p.value,0)||1;
  const cx=W/2,cy=H/2,r=Math.min(W*0.34,H*0.44),innerR=r*0.5;
  let a0=-Math.PI/2;
  slices.forEach((p,i) => {
    const sweep=(p.value/total)*Math.PI*2,a1=a0+sweep,mid=a0+sweep/2;
    ctx.beginPath(); ctx.moveTo(cx,cy); ctx.arc(cx,cy,r,a0,a1); ctx.closePath();
    ctx.fillStyle=CHART_PALETTE[i]; ctx.fill(); ctx.strokeStyle=bg; ctx.lineWidth=2; ctx.stroke();
    if (p.value/total>0.06) { const lr=(r+innerR)/2; ctx.fillStyle='#fff'; ctx.font=`bold ${Math.max(10,Math.round(r*0.1))}px system-ui,sans-serif`; ctx.textAlign='center'; ctx.fillText(`${Math.round(p.value/total*100)}%`,cx+Math.cos(mid)*lr,cy+Math.sin(mid)*lr+4); }
    state.hitboxes.push({ type:'pie',cx,cy,r,innerR,a0,a1,label:p.label,value:p.value,color:CHART_PALETTE[i] });
    a0=a1;
  });
  ctx.beginPath(); ctx.arc(cx,cy,innerR,0,Math.PI*2); ctx.fillStyle=bg; ctx.fill();
  ctx.textAlign='center'; ctx.fillStyle=muteC; ctx.font='11px system-ui,sans-serif'; ctx.fillText('итого',cx,cy-7);
  ctx.fillStyle=textC; ctx.font=`bold ${Math.max(13,Math.round(r*0.17))}px system-ui,sans-serif`; ctx.fillText(shortNum(total),cx,cy+12);
  ctx.textAlign='left';
}

/* ── Hover ── */
function bindChartHover(chartId, canvas, tooltip) {
  if (canvas._hoverBound) canvas.removeEventListener('mousemove', canvas._hoverBound);
  if (canvas._leaveBound) canvas.removeEventListener('mouseleave', canvas._leaveBound);
  canvas._hoverBound = ev => {
    const state=chartStateMap.get(chartId);
    if (!state||!tooltip) return;
    const rect=canvas.getBoundingClientRect();
    const hit=findChartHit(ev.clientX-rect.left, ev.clientY-rect.top, state.hitboxes);
    if (!hit) { tooltip.classList.add('hidden'); return; }
    const pct=state.total>0?` (${((Math.max(0,hit.value)/state.total)*100).toFixed(1)}%)`:'';
    tooltip.innerHTML=`<strong>${escHtml(shortenLabel(String(hit.label),40))}</strong><br>${escHtml(fmtNum(Math.round(hit.value*100)/100))}${escHtml(pct)}`;
    tooltip.style.left=`${hit.cx}px`; tooltip.style.top=`${hit.cy}px`;
    tooltip.classList.remove('hidden');
  };
  canvas._leaveBound = () => tooltip?.classList.add('hidden');
  canvas.addEventListener('mousemove', canvas._hoverBound);
  canvas.addEventListener('mouseleave', canvas._leaveBound);
}

function findChartHit(x, y, hitboxes) {
  for (let i=hitboxes.length-1; i>=0; i--) {
    const h=hitboxes[i];
    if (h.type==='pie') {
      const dx=x-h.cx,dy=y-h.cy,dist=Math.sqrt(dx*dx+dy*dy);
      if (dist>h.r||dist<h.innerR) continue;
      let ang=Math.atan2(dy,dx); if (ang<-Math.PI/2) ang+=Math.PI*2;
      if (ang>=h.a0&&ang<h.a1) return h; continue;
    }
    if (x>=h.x&&x<=h.x+h.w&&y>=h.y&&y<=h.y+h.h) return h;
  }
  return null;
}

/* ── Legend ── */
function renderChartLegend(chartId, points, type) {
  const wrap=document.getElementById('cl_'+chartId);
  if (!wrap) return;
  if (!points.length) { wrap.innerHTML=''; return; }
  const total=points.reduce((s,p)=>s+Math.max(0,p.value),0);
  const accent=getComputedStyle(document.documentElement).getPropertyValue('--accent').trim()||'#5b6ef5';
  wrap.innerHTML=points.slice(0,12).map((p,i)=>{
    const color=(type==='pie'||type==='bar'||type==='funnel') ? CHART_PALETTE[i%CHART_PALETTE.length]
              : type==='waterfall' ? (p.value>=0?'#34c759':'#ff375f') : accent;
    const pct=total>0?` · ${((Math.max(0,p.value)/total)*100).toFixed(1)}%`:'';
    return `<div class="analytics-legend-item"><span class="analytics-legend-dot" style="background:${color}"></span><span>${escHtml(shortenLabel(String(p.label),24))}: <b>${escHtml(shortNum(p.value))}</b>${escHtml(pct)}</span></div>`;
  }).join('');
}

/* ── Download ── */
function downloadChartById(id) {
  const canvas=document.getElementById('c_'+id);
  if (!canvas) return;
  const title=analyticsState.charts.find(c=>c.id===id)?.title||'chart';
  const a=document.createElement('a');
  a.href=canvas.toDataURL('image/png');
  a.download=`${title.replace(/[^a-zA-Zа-яёА-ЯЁ0-9]+/g,'_')}.png`;
  a.click();
}

/* ── Data table with color scale ── */
function renderAnalyticsDataTable(rows, cols) {
  const wrap=document.getElementById('analytics-table');
  if (!wrap) return;
  if (!rows.length) { wrap.innerHTML='<div class="empty-state">Нет данных</div>'; return; }
  const colorFmt=document.getElementById('an-colorfmt')?.checked;
  const numCols=colorFmt ? cols.filter(c=>rows.slice(0,50).some(r=>Number.isFinite(Number(r[c]))&&r[c]!=='')) : [];
  const colRange={};
  numCols.forEach(c => { const vals=rows.map(r=>Number(r[c])).filter(Number.isFinite); colRange[c]={ min:Math.min(...vals), max:Math.max(...vals) }; });
  const show=rows.slice(0,500);
  let html='<div class="result-table-wrap"><table class="result-table"><thead><tr>';
  cols.forEach(c => { html+=`<th>${escHtml(c)}</th>`; });
  html+='</tr></thead><tbody>';
  show.forEach(r => {
    html+='<tr>';
    cols.forEach(c => {
      const v=r[c]; let style='';
      if (colorFmt&&colRange[c]) {
        const {min,max}=colRange[c],n=Number(v);
        if (Number.isFinite(n)&&max>min) { const t=(n-min)/(max-min); style=` style="background:hsla(${Math.round(t*120)},70%,45%,0.25)"`; }
      }
      html+=`<td${style}>${escHtml(v==null?'':String(v))}</td>`;
    });
    html+='</tr>';
  });
  html+='</tbody></table></div>';
  if (rows.length>500) html+=`<div class="query-stat">Показаны первые 500 из ${rows.length} строк</div>`;
  wrap.innerHTML=html;
}

/* ── Stats selectors ── */
function fillStatsSelectors(cols) {
  ['st-col-x','st-col-y','st-group-col'].forEach(id => {
    const el=document.getElementById(id); if (!el) return;
    const old=el.value;
    el.innerHTML=`<option value="">—</option>`+cols.map(c=>`<option value="${escAttr(c)}">${escHtml(c)}</option>`).join('');
    if (cols.includes(old)) el.value=old;
  });
}

/* ── Stat method visibility ── */
const ST_CONTROLS = {
  'st-ctrl-xcol':   ['describe','percentile_analysis','freq','corr','covariance','regression','moving_avg','exp_smooth','ci_mean','ttest','ftest','anova','ztest','normalize','rank','bootstrap_mean','permutation_diff'],
  'st-ctrl-ycol':   ['corr','covariance','regression'],
  'st-ctrl-gcol':   ['ttest','ftest','anova','permutation_diff'],
  'st-ctrl-mcols':  ['corr_matrix'],
  'st-ctrl-window': ['moving_avg'],
  'st-ctrl-smooth': ['exp_smooth'],
  'st-ctrl-normm':  ['normalize'],
  'st-ctrl-hyp':    ['ci_mean','ttest','ztest','ftest','anova'],
  'st-ctrl-batch':  ['multi_hyp'],
  'st-ctrl-pctiles':['percentile_analysis'],
};

function onStatMethodChange() {
  const method = document.getElementById('st-method').value;
  Object.entries(ST_CONTROLS).forEach(([id, methods]) => {
    const el = document.getElementById(id);
    if (el) el.style.display = methods.includes(method) ? '' : 'none';
  });
  const xLabel = document.getElementById('st-xcol-label');
  if (xLabel) {
    if (method === 'freq') xLabel.textContent = 'Поле (любое)';
    else if (method === 'rank') xLabel.textContent = 'Числовое поле';
    else xLabel.textContent = 'Поле X / числовое';
  }
  runStatMethod();
}

/* ── Helpers ── */
function isLikelyMetricColumn(name) {
  return /(amount|sum|total|price|cost|qty|quantity|count|revenue|sales|score|age|value|metric|discount)/i.test(String(name||''));
}
function shortenLabel(s, n) { const t=String(s||''); return t.length>n?t.slice(0,Math.max(1,n-1))+'…':t; }
function shortNum(v) {
  const n=Number(v)||0, a=Math.abs(n);
  const fmt=x=>String(parseFloat(x.toFixed(1)));
  if (a>=1e9) return `${fmt(n/1e9)}B`;
  if (a>=1e6) return `${fmt(n/1e6)}M`;
  if (a>=1e3) return `${fmt(n/1e3)}K`;
  return `${Math.round(n*100)/100}`;
}


function runStatMethod() {
  const out = document.getElementById('stats-output');
  if (!out) return;
  const rows = analyticsState.currentRows || [];
  if (!rows.length) { out.textContent = 'Сначала выполните запрос.'; return; }

  const method    = document.getElementById('st-method')?.value || 'describe';
  const xCol      = document.getElementById('st-col-x')?.value || '';
  const yCol      = document.getElementById('st-col-y')?.value || '';
  const groupCol  = document.getElementById('st-group-col')?.value || '';
  const hOp       = document.getElementById('st-h-op')?.value || 'gt';
  const threshold = Number(document.getElementById('st-threshold')?.value || 0);
  const alpha     = Math.max(0.001, Math.min(0.5, Number(document.getElementById('st-alpha')?.value || 0.05)));

  const x = numericCol(rows, xCol);
  const y = numericCol(rows, yCol);

  /* ── 1. Описательная статистика ── */
  if (method === 'describe') {
    if (!xCol) { out.textContent = 'Выберите числовое поле X.'; return; }
    const s = descExt(x);
    if (!s) { out.textContent = 'Нет числовых данных в поле X.'; return; }
    const cv = s.mean ? Math.abs(s.sd / s.mean) * 100 : null;
    const data = [
      ['Поле', xCol], ['N', s.n],
      ['Mean (среднее)', rnd(s.mean)],
      ['Median (медиана)', rnd(s.median)],
      ['Mode (мода)', s.mode !== null ? rnd(s.mode) : '—'],
      ['StdDev (σ)', rnd(s.sd)],
      ['Variance (дисперсия)', rnd(s.variance)],
      ['CV (%)', cv !== null ? rnd(cv) + '%' : '—'],
      ['Min', rnd(s.min)], ['Max', rnd(s.max)], ['Range', rnd(s.max - s.min)],
      ['Q1 (25%)', rnd(s.q1)], ['Q3 (75%)', rnd(s.q3)], ['IQR', rnd(s.iqr)],
      ['Skewness (асимметрия)', rnd(s.skew)],
      ['Kurtosis эксцесс', rnd(s.kurt)],
    ];
    lastStatsReport = { method, created_at: new Date().toISOString(), summary: Object.fromEntries(data), rows: [] };
    out.innerHTML = statHtml(data);
    return;
  }

  /* ── Квантили и процентили ── */
  if (method === 'percentile_analysis') {
    if (!xCol) { out.textContent = 'Выберите числовое поле X.'; return; }
    if (!x.length) { out.textContent = 'Нет числовых данных.'; return; }
    const pctRaw = (document.getElementById('st-pctiles')?.value || '1,5,10,25,50,75,90,95,99')
      .split(',').map(v => Number(v.trim())).filter(v => Number.isFinite(v) && v >= 0 && v <= 100);
    const sorted = [...x].sort((a, b) => a - b);
    const data = pctRaw.map(p => [`P${p}`, rnd(pctOf(sorted, p))]);
    lastStatsReport = { method, created_at: new Date().toISOString(), summary: Object.fromEntries(data), rows: [] };
    out.innerHTML = statHtml([['Поле', xCol], ['N', x.length], ...data]);
    return;
  }

  /* ── Частотный анализ ── */
  if (method === 'freq') {
    if (!xCol) { out.textContent = 'Выберите поле.'; return; }
    const vals = rows.map(r => String(r[xCol] ?? '(пусто)'));
    const freq = new Map();
    vals.forEach(v => freq.set(v, (freq.get(v) || 0) + 1));
    const total = vals.length;
    const sorted = [...freq.entries()].sort((a, b) => b[1] - a[1]);
    const tableRows = sorted.map(([v, n], i) => ({
      rank: i + 1, value: v, count: n,
      pct: rnd(n / total * 100) + '%',
      cum_pct: rnd(sorted.slice(0, i + 1).reduce((s, [, c]) => s + c, 0) / total * 100) + '%'
    }));
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: { field: xCol, total, unique: freq.size }, rows: tableRows };
    out.innerHTML = freqHtml(xCol, total, freq.size, tableRows);
    return;
  }

  /* ── Корреляция Пирсона ── */
  if (method === 'corr') {
    if (!xCol || !yCol) { out.textContent = 'Выберите поля X и Y.'; return; }
    const r = pearson(x, y);
    const n = Math.min(x.length, y.length);
    const tStat = n > 2 ? r * Math.sqrt(n - 2) / Math.sqrt(1 - r * r) : 0;
    const p = n > 2 ? 2 * (1 - normalCdf(Math.abs(tStat))) : 1;
    const data = [
      ['Поле X', xCol], ['Поле Y', yCol], ['N пар', n],
      ['Pearson r', rnd(r)], ['r²', rnd(r * r)],
      ['t-stat', rnd(tStat)], ['p-value', rnd(p)],
      ['Интерпретация', corrLabel(r)],
      ['Значимость', p < 0.05 ? 'Значима (p < 0.05)' : 'Не значима (p ≥ 0.05)'],
    ];
    lastStatsReport = { method, created_at: new Date().toISOString(), summary: Object.fromEntries(data), rows: [] };
    out.innerHTML = statHtml(data);
    return;
  }

  /* ── Матрица корреляций ── */
  if (method === 'corr_matrix') {
    const input = document.getElementById('st-matrix-cols')?.value || '';
    let mCols = input.split(',').map(s => s.trim()).filter(Boolean);
    if (!mCols.length) {
      mCols = (analyticsState.currentCols || []).filter(c => {
        const v = numericCol(rows, c); return v.length >= Math.max(2, rows.length * 0.5);
      });
    }
    if (mCols.length < 2) { out.textContent = 'Нужно минимум 2 числовых поля. Укажите их явно или загрузите данные с числовыми столбцами.'; return; }
    const matrix = mCols.map(c1 => mCols.map(c2 => rnd(pearson(numericCol(rows, c1), numericCol(rows, c2)))));
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: { cols: mCols.join(', '), n: mCols.length }, rows: [] };
    out.innerHTML = corrMatrixHtml(mCols, matrix);
    return;
  }

  /* ── Ковариация ── */
  if (method === 'covariance') {
    if (!xCol || !yCol) { out.textContent = 'Выберите поля X и Y.'; return; }
    const n = Math.min(x.length, y.length);
    if (n < 2) { out.textContent = 'Нет данных.'; return; }
    const cov = covarianceCalc(x.slice(0, n), y.slice(0, n));
    const sx = descExt(x), sy = descExt(y);
    const data = [
      ['Поле X', xCol], ['Поле Y', yCol], ['N пар', n],
      ['Ковариация (выборочная)', rnd(cov)],
      ['StdDev X', rnd(sx?.sd || 0)], ['StdDev Y', rnd(sy?.sd || 0)],
      ['Pearson r', rnd(pearson(x, y))],
    ];
    lastStatsReport = { method, created_at: new Date().toISOString(), summary: Object.fromEntries(data), rows: [] };
    out.innerHTML = statHtml(data);
    return;
  }

  /* ── Линейная регрессия ── */
  if (method === 'regression') {
    if (!xCol || !yCol) { out.textContent = 'Выберите поля X и Y.'; return; }
    const reg = linreg(x, y);
    const n = reg.n;
    const data = [
      ['Поле X', xCol], ['Поле Y', yCol],
      ['Модель', `y = ${rnd(reg.a)} + ${rnd(reg.b)} × x`],
      ['R²', rnd(reg.r2)], ['R (корреляция)', rnd(Math.sqrt(reg.r2))],
      ['Intercept (a)', rnd(reg.a)], ['Slope (b)', rnd(reg.b)], ['N пар', n],
    ];
    const preview = x.slice(0, 30).map((xi, i) => ({
      x: rnd(xi), y_actual: rnd(y[i] ?? 0), y_predicted: rnd(reg.a + reg.b * xi),
      residual: rnd((y[i] ?? 0) - (reg.a + reg.b * xi))
    }));
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: Object.fromEntries(data), rows: preview };
    out.innerHTML = statHtml(data) + (n > 0 ? statRowsHtml(preview, ['x','y_actual','y_predicted','residual'], ['X','Y факт','Y прогноз','Остаток']) : '');
    return;
  }

  /* ── Скользящее среднее ── */
  if (method === 'moving_avg') {
    if (!xCol) { out.textContent = 'Выберите числовое поле X.'; return; }
    const win = Math.max(2, parseInt(document.getElementById('st-ma-window')?.value || 3));
    if (x.length < win) { out.textContent = `Нужно минимум ${win} значений.`; return; }
    const ma = movingAvg(x, win);
    const tableRows = x.slice(0, 100).map((v, i) => ({
      index: i + 1, original: rnd(v), ma: rnd(ma[i])
    }));
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: { field: xCol, window: win, n: x.length }, rows: tableRows };
    out.innerHTML = statHtml([['Поле', xCol], ['Окно', win], ['N', x.length],
      ['MA(последнее)', rnd(ma[ma.length - 1])], ['Тренд', ma[ma.length-1] > ma[0] ? '▲ Рост' : '▼ Снижение']]) +
      statRowsHtml(tableRows, ['index','original','ma'], ['#','Значение','MA']);
    return;
  }

  /* ── Экспоненциальное сглаживание ── */
  if (method === 'exp_smooth') {
    if (!xCol) { out.textContent = 'Выберите числовое поле X.'; return; }
    const al = Math.max(0.01, Math.min(0.99, parseFloat(document.getElementById('st-smooth-alpha')?.value || 0.3)));
    if (x.length < 2) { out.textContent = 'Нужно минимум 2 значения.'; return; }
    const sm = expSmooth(x, al);
    const tableRows = x.slice(0, 100).map((v, i) => ({
      index: i + 1, original: rnd(v), smoothed: rnd(sm[i])
    }));
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: { field: xCol, alpha: al, n: x.length, forecast_next: rnd(sm[sm.length - 1]) }, rows: tableRows };
    out.innerHTML = statHtml([['Поле', xCol], ['α', al], ['N', x.length],
      ['Прогноз (следующий)', rnd(sm[sm.length - 1])]]) +
      statRowsHtml(tableRows, ['index','original','smoothed'], ['#','Значение','Сглаженное']);
    return;
  }

  /* ── Доверительный интервал ── */
  if (method === 'ci_mean') {
    if (!xCol) { out.textContent = 'Выберите числовое поле X.'; return; }
    const s = descExt(x);
    if (!s) { out.textContent = 'Нет данных.'; return; }
    const z95 = 1.96, z99 = 2.576;
    const se = s.sd / Math.sqrt(s.n);
    const data = [
      ['Поле', xCol], ['N', s.n], ['Mean', rnd(s.mean)], ['StdDev', rnd(s.sd)], ['SE', rnd(se)],
      ['95% ДИ', `[${rnd(s.mean - z95 * se)}, ${rnd(s.mean + z95 * se)}]`],
      ['99% ДИ', `[${rnd(s.mean - z99 * se)}, ${rnd(s.mean + z99 * se)}]`],
      ['Гипотеза H1', hypothesisText(s.mean, threshold, hOp)],
    ];
    lastStatsReport = { method, created_at: new Date().toISOString(), summary: Object.fromEntries(data), rows: [] };
    out.innerHTML = statHtml(data);
    return;
  }

  /* ── t-тест (Welch) ── */
  if (method === 'ttest') {
    const g = splitGroups(rows, groupCol, xCol);
    if (!g) { out.textContent = 'Для t-теста выберите поле группы (2 категории) и числовое поле X.'; return; }
    const t = welchT(g.a.values, g.b.values);
    const da = descExt(g.a.values), db = descExt(g.b.values);
    const data = [
      ['Группа A', `${g.a.name} (n=${g.a.values.length})`],
      ['Группа B', `${g.b.name} (n=${g.b.values.length})`],
      ['Mean A', rnd(da?.mean || 0)], ['Mean B', rnd(db?.mean || 0)],
      ['StdDev A', rnd(da?.sd || 0)], ['StdDev B', rnd(db?.sd || 0)],
      ['t-статистика', rnd(t.t)], ['p-value (прибл.)', rnd(t.p)],
      ['alpha', alpha],
      ['Заключение', t.p < alpha ? `Различие значимо (p=${rnd(t.p)} < ${alpha})` : `Нет значимого различия (p=${rnd(t.p)} ≥ ${alpha})`],
    ];
    lastStatsReport = { method, created_at: new Date().toISOString(), summary: Object.fromEntries(data), rows: [] };
    out.innerHTML = statHtml(data);
    return;
  }

  /* ── F-тест (равенство дисперсий) ── */
  if (method === 'ftest') {
    const g = splitGroups(rows, groupCol, xCol);
    if (!g) { out.textContent = 'Для F-теста выберите поле группы (2 категории) и числовое поле X.'; return; }
    const da = descExt(g.a.values), db = descExt(g.b.values);
    if (!da || !db || da.variance === 0 || db.variance === 0) { out.textContent = 'Нет дисперсии в одной из групп.'; return; }
    const F = da.variance / db.variance;
    const df1 = da.n - 1, df2 = db.n - 1;
    const p = fPValue(F, df1, df2);
    const data = [
      ['Группа A', `${g.a.name} (n=${da.n})`], ['Var A', rnd(da.variance)],
      ['Группа B', `${g.b.name} (n=${db.n})`], ['Var B', rnd(db.variance)],
      ['F-статистика', rnd(F)], ['df1', df1], ['df2', df2],
      ['p-value (прибл.)', rnd(p)],
      ['Заключение', p < alpha ? `Дисперсии различаются значимо (p=${rnd(p)} < ${alpha})` : `Дисперсии не различаются (p=${rnd(p)} ≥ ${alpha})`],
    ];
    lastStatsReport = { method, created_at: new Date().toISOString(), summary: Object.fromEntries(data), rows: [] };
    out.innerHTML = statHtml(data);
    return;
  }

  /* ── Однофакторный ANOVA ── */
  if (method === 'anova') {
    if (!xCol || !groupCol) { out.textContent = 'Выберите числовое поле X и поле группы.'; return; }
    const res = oneWayAnova(rows, groupCol, xCol);
    if (!res) { out.textContent = 'Для ANOVA нужно минимум 2 группы с данными.'; return; }
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: { F: res.F, p: res.p, k: res.k, N: res.N }, rows: res.groups };
    out.innerHTML = anovaHtml(res);
    return;
  }

  /* ── z-тест ── */
  if (method === 'ztest') {
    if (!xCol) { out.textContent = 'Выберите числовое поле X.'; return; }
    const s = descExt(x);
    if (!s || !s.n) { out.textContent = 'Нет данных.'; return; }
    const z = s.sd > 0 ? (s.mean - threshold) / (s.sd / Math.sqrt(s.n)) : 0;
    let p = 1 - normalCdf(Math.abs(z));
    if (hOp === 'ne') p *= 2;
    else if (hOp === 'lt') p = normalCdf(z);
    else p = 1 - normalCdf(z);
    const data = [
      ['Поле', xCol], ['N', s.n], ['Mean', rnd(s.mean)], ['StdDev', rnd(s.sd)],
      ['Порог H0', threshold], ['H1', hOp === 'gt' ? `mean > ${threshold}` : hOp === 'lt' ? `mean < ${threshold}` : `mean ≠ ${threshold}`],
      ['z-статистика', rnd(z)], ['p-value (одностор./двустор.)', rnd(p)], ['alpha', alpha],
      ['Заключение', p < alpha ? 'H0 отвергается' : 'H0 не отвергается'],
    ];
    lastStatsReport = { method, created_at: new Date().toISOString(), summary: Object.fromEntries(data), rows: [] };
    out.innerHTML = statHtml(data);
    return;
  }

  /* ── Пакетная проверка гипотез ── */
  if (method === 'multi_hyp') {
    const colsRaw = (document.getElementById('st-multi-cols')?.value || '').split(',').map(s => s.trim()).filter(Boolean);
    const thrRaw  = (document.getElementById('st-multi-thresholds')?.value || '').split(',').map(s => Number(s.trim())).filter(Number.isFinite);
    const bAlpha  = Math.max(0.001, Math.min(0.5, Number(document.getElementById('st-alpha-batch')?.value || 0.05)));
    if (!colsRaw.length || !thrRaw.length) { out.textContent = 'Укажите поля и пороги.'; return; }
    const m = [];
    colsRaw.forEach((c, ci) => {
      const arr = numericCol(rows, c);
      const s = descExt(arr);
      if (!s || !s.n) return;
      thrRaw.forEach((thr, ti) => {
        const z = s.sd > 0 ? (s.mean - thr) / (s.sd / Math.sqrt(s.n)) : 0;
        let p = 1 - normalCdf(Math.abs(z));
        if (hOp === 'ne') p *= 2;
        else if (hOp === 'lt') p = normalCdf(z);
        else p = 1 - normalCdf(z);
        m.push({ id:`${ci+1}.${ti+1}`, field:c, n:s.n, mean:rnd(s.mean), sd:rnd(s.sd),
          threshold:thr, op:hOp, z_stat:rnd(z), p_value:rnd(p),
          decision: p < bAlpha ? '✓ reject H0' : '— fail to reject' });
      });
    });
    if (!m.length) { out.textContent = 'Нет валидных данных для пакетного теста.'; return; }
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: { alpha: bAlpha, count: m.length }, rows: m };
    out.innerHTML = statRowsHtml(m, ['id','field','n','mean','sd','threshold','op','z_stat','p_value','decision'],
      ['#','Поле','N','Mean','SD','Порог','H1','z','p','Решение']);
    return;
  }

  /* ── Нормализация ── */
  if (method === 'normalize') {
    if (!xCol) { out.textContent = 'Выберите числовое поле X.'; return; }
    const normMethod = document.getElementById('st-norm-method')?.value || 'zscore';
    const s = descExt(x);
    if (!s || !s.n) { out.textContent = 'Нет данных.'; return; }
    let normed;
    if (normMethod === 'zscore') {
      normed = s.sd > 0 ? x.map(v => (v - s.mean) / s.sd) : x.map(() => 0);
    } else {
      const range = s.max - s.min;
      normed = range > 0 ? x.map(v => (v - s.min) / range) : x.map(() => 0);
    }
    const sn = descExt(normed);
    const tableRows = x.slice(0, 60).map((v, i) => ({ index: i + 1, original: rnd(v), normalized: rnd(normed[i]) }));
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: { field: xCol, method: normMethod, mean_before: rnd(s.mean), sd_before: rnd(s.sd),
        mean_after: rnd(sn?.mean || 0), sd_after: rnd(sn?.sd || 0) }, rows: tableRows };
    out.innerHTML = statHtml([
      ['Поле', xCol], ['Метод', normMethod === 'zscore' ? 'Z-оценка' : 'Min-Max'],
      ['Mean до', rnd(s.mean)], ['SD до', rnd(s.sd)],
      ['Mean после', rnd(sn?.mean || 0)], ['SD после', rnd(sn?.sd || 0)],
      ['Min после', rnd(sn?.min || 0)], ['Max после', rnd(sn?.max || 0)],
    ]) + statRowsHtml(tableRows, ['index','original','normalized'], ['#','Исходное','Нормализованное']);
    return;
  }

  /* ── Ранжирование ── */
  if (method === 'rank') {
    if (!xCol) { out.textContent = 'Выберите числовое поле.'; return; }
    const indexed = x.map((v, i) => ({ i, v })).sort((a, b) => b.v - a.v);
    indexed.forEach((item, ri) => { item.rank = ri + 1; });
    indexed.sort((a, b) => a.i - b.i);
    const tableRows = indexed.slice(0, 100).map(item => ({
      row: item.i + 1, value: rnd(item.v), rank: item.rank,
      pct_rank: rnd((1 - (item.rank - 1) / (x.length - 1 || 1)) * 100) + '%'
    }));
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: { field: xCol, n: x.length }, rows: tableRows };
    out.innerHTML = statHtml([['Поле', xCol], ['N', x.length]]) +
      statRowsHtml(tableRows, ['row','value','rank','pct_rank'], ['Строка','Значение','Ранг','Процентиль']);
    return;
  }

  /* ── Bootstrap для среднего ── */
  if (method === 'bootstrap_mean') {
    if (!xCol) { out.textContent = 'Выберите числовое поле X.'; return; }
    const B = 1000;
    if (x.length < 5) { out.textContent = 'Для bootstrap нужно минимум 5 наблюдений.'; return; }
    const means = [];
    for (let b = 0; b < B; b++) {
      let s = 0;
      for (let i = 0; i < x.length; i++) s += x[(Math.random() * x.length) | 0];
      means.push(s / x.length);
    }
    means.sort((a, b) => a - b);
    const lo = means[Math.floor(0.025 * B)], hi = means[Math.floor(0.975 * B)];
    const mu = means.reduce((s, v) => s + v, 0) / means.length;
    const data = [
      ['Поле', xCol], ['N', x.length], ['Bootstrap итераций', B],
      ['Mean (bootstrap)', rnd(mu)], ['95% ДИ', `[${rnd(lo)}, ${rnd(hi)}]`],
    ];
    lastStatsReport = { method, created_at: new Date().toISOString(),
      summary: Object.fromEntries(data),
      rows: means.slice(0, 200).map((v, i) => ({ sample: i + 1, mean: rnd(v) })) };
    out.innerHTML = statHtml(data);
    return;
  }

  /* ── Permutation test ── */
  if (method === 'permutation_diff') {
    const g = splitGroups(rows, groupCol, xCol);
    if (!g) { out.textContent = 'Выберите поле группы (2 группы) и числовое поле X.'; return; }
    const A = g.a.values, B = g.b.values;
    const obs = meanOf(A) - meanOf(B);
    const all = A.concat(B);
    const nA = A.length;
    const R = 1000;
    let extreme = 0;
    for (let r = 0; r < R; r++) {
      shuffleInPlace(all);
      if (Math.abs(meanOf(all.slice(0, nA)) - meanOf(all.slice(nA))) >= Math.abs(obs)) extreme++;
    }
    const p = (extreme + 1) / (R + 1);
    const data = [
      ['Группа A', g.a.name], ['N(A)', A.length], ['Mean A', rnd(meanOf(A))],
      ['Группа B', g.b.name], ['N(B)', B.length], ['Mean B', rnd(meanOf(B))],
      ['Наблюд. разница mean', rnd(obs)], ['Перестановок', R],
      ['p-value', rnd(p)],
      ['Заключение', p < alpha ? `Различие значимо (p=${rnd(p)} < ${alpha})` : `Нет значимого различия (p=${rnd(p)} ≥ ${alpha})`],
    ];
    lastStatsReport = { method, created_at: new Date().toISOString(), summary: Object.fromEntries(data), rows: [] };
    out.innerHTML = statHtml(data);
    return;
  }
}

function numericCol(rows, col) {
  if (!col) return [];
  return rows.map(r => Number(r[col])).filter(Number.isFinite);
}

function pctOf(sorted, p) {
  if (!sorted.length) return 0;
  const idx = (p / 100) * (sorted.length - 1);
  const lo = Math.floor(idx), hi = Math.ceil(idx);
  return sorted[lo] + (idx - lo) * ((sorted[hi] ?? sorted[lo]) - sorted[lo]);
}

function descExt(arr) {
  if (!arr || !arr.length) return null;
  const n = arr.length;
  const sorted = [...arr].sort((a, b) => a - b);
  const mean = arr.reduce((s, v) => s + v, 0) / n;
  const variance = n > 1 ? arr.reduce((s, v) => s + (v - mean) ** 2, 0) / (n - 1) : 0;
  const sd = Math.sqrt(variance);
  const median = pctOf(sorted, 50);
  const q1 = pctOf(sorted, 25), q3 = pctOf(sorted, 75);
  const freq = new Map();
  arr.forEach(v => freq.set(v, (freq.get(v) || 0) + 1));
  let mode = null, maxF = 0;
  freq.forEach((f, v) => { if (f > maxF) { maxF = f; mode = v; } });
  if (maxF === 1) mode = null;
  const skew = sd > 0 ? arr.reduce((s, v) => s + ((v - mean) / sd) ** 3, 0) / n : 0;
  const kurt = sd > 0 ? arr.reduce((s, v) => s + ((v - mean) / sd) ** 4, 0) / n - 3 : 0;
  return { n, mean, variance, sd, min: sorted[0], max: sorted[n - 1], median, q1, q3, iqr: q3 - q1, mode, skew, kurt };
}

/* Keep desc as alias for backward compat with old callers */
function desc(arr) { return descExt(arr) || { n: 0, mean: 0, sd: 0, min: 0, max: 0 }; }

function pearson(a, b) {
  const n = Math.min(a.length, b.length);
  if (n < 2) return 0;
  const x = a.slice(0, n), y = b.slice(0, n);
  const mx = x.reduce((s, v) => s + v, 0) / n;
  const my = y.reduce((s, v) => s + v, 0) / n;
  let num = 0, dx = 0, dy = 0;
  for (let i = 0; i < n; i++) {
    const vx = x[i] - mx, vy = y[i] - my;
    num += vx * vy; dx += vx * vx; dy += vy * vy;
  }
  return dx && dy ? num / Math.sqrt(dx * dy) : 0;
}

function covarianceCalc(a, b) {
  const n = Math.min(a.length, b.length);
  if (n < 2) return 0;
  const mx = a.slice(0,n).reduce((s,v)=>s+v,0)/n;
  const my = b.slice(0,n).reduce((s,v)=>s+v,0)/n;
  return a.slice(0,n).reduce((s,v,i) => s + (v-mx)*(b[i]-my), 0) / (n-1);
}

function linreg(a, b) {
  const n = Math.min(a.length, b.length);
  if (n < 2) return { a: 0, b: 0, r2: 0, n };
  const x = a.slice(0, n), y = b.slice(0, n);
  const mx = x.reduce((s, v) => s + v, 0) / n;
  const my = y.reduce((s, v) => s + v, 0) / n;
  let num = 0, den = 0;
  for (let i = 0; i < n; i++) { num += (x[i] - mx) * (y[i] - my); den += (x[i] - mx) ** 2; }
  const b1 = den ? num / den : 0;
  const a0 = my - b1 * mx;
  const r = pearson(x, y);
  return { a: a0, b: b1, r2: r * r, n };
}

function movingAvg(arr, win) {
  return arr.map((_, i) => {
    const sl = arr.slice(Math.max(0, i - win + 1), i + 1);
    return sl.reduce((s, v) => s + v, 0) / sl.length;
  });
}

function expSmooth(arr, al) {
  if (!arr.length) return [];
  const r = [arr[0]];
  for (let i = 1; i < arr.length; i++) r.push(al * arr[i] + (1 - al) * r[i - 1]);
  return r;
}

function oneWayAnova(rows, groupCol, valCol) {
  if (!groupCol || !valCol) return null;
  const groups = new Map();
  rows.forEach(r => {
    const k = String(r[groupCol] ?? '');
    const v = Number(r[valCol]);
    if (!Number.isFinite(v)) return;
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(v);
  });
  const gl = [...groups.entries()].filter(([, v]) => v.length > 0);
  const k = gl.length;
  if (k < 2) return null;
  const allVals = gl.flatMap(([, v]) => v);
  const N = allVals.length;
  const grandMean = allVals.reduce((s, v) => s + v, 0) / N;
  let SSB = 0, SSW = 0;
  gl.forEach(([, v]) => {
    const n = v.length, gm = v.reduce((s, x) => s + x, 0) / n;
    SSB += n * (gm - grandMean) ** 2;
    SSW += v.reduce((s, x) => s + (x - gm) ** 2, 0);
  });
  const dfB = k - 1, dfW = N - k;
  const MSB = SSB / dfB, MSW = dfW > 0 ? SSW / dfW : 0;
  const F = MSW > 0 ? MSB / MSW : 0;
  const p = fPValue(F, dfB, dfW);
  return {
    k, N, SSB: rnd(SSB), SSW: rnd(SSW), dfB, dfW,
    MSB: rnd(MSB), MSW: rnd(MSW), F: rnd(F), p: rnd(p),
    significant: p < 0.05,
    groups: gl.map(([name, v]) => {
      const s = descExt(v);
      return { group: name, n: v.length, mean: rnd(s?.mean||0), sd: rnd(s?.sd||0), min: rnd(s?.min||0), max: rnd(s?.max||0) };
    })
  };
}

function splitGroups(rows, groupCol, valCol) {
  if (!groupCol || !valCol) return null;
  const m = new Map();
  rows.forEach(r => {
    const k = String(r[groupCol] ?? '(empty)');
    const v = Number(r[valCol]);
    if (!Number.isFinite(v)) return;
    if (!m.has(k)) m.set(k, []);
    m.get(k).push(v);
  });
  const keys = [...m.keys()].filter(k => m.get(k).length > 1);
  if (keys.length < 2) return null;
  const a = keys[0], b = keys[1];
  return { a: { name: a, values: m.get(a) }, b: { name: b, values: m.get(b) } };
}

function welchT(a, b) {
  const da = descExt(a) || {mean:0,sd:0,n:0}, db = descExt(b) || {mean:0,sd:0,n:0};
  const se = Math.sqrt((da.sd ** 2 / (da.n||1)) + (db.sd ** 2 / (db.n||1)));
  const t = se ? (da.mean - db.mean) / se : 0;
  const p = 2 * (1 - normalCdf(Math.abs(t)));
  return { t, p };
}

function normalCdf(x) {
  return 0.5 * (1 + erf(x / Math.SQRT2));
}

function erf(x) {
  const s = x < 0 ? -1 : 1;
  const ax = Math.abs(x);
  const t = 1 / (1 + 0.3275911 * ax);
  const y = 1 - (((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t) * Math.exp(-ax * ax);
  return s * y;
}

function chiSqPValue(chi2, df) {
  /* Wilson-Hilferty normal approximation for chi-square p-value */
  if (df <= 0 || chi2 < 0) return 1;
  const x = Math.pow(chi2 / df, 1 / 3);
  const mu = 1 - 2 / (9 * df);
  const sigma = Math.sqrt(2 / (9 * df));
  return 1 - normalCdf((x - mu) / sigma);
}

function fPValue(F, df1, df2) {
  /* F-distribution p-value via chi-square approximation */
  if (F <= 0 || df1 <= 0 || df2 <= 0) return 1;
  return chiSqPValue(F * df1, df1);
}

function corrLabel(r) {
  const ar = Math.abs(r);
  if (ar >= 0.8) return 'Сильная связь';
  if (ar >= 0.5) return 'Умеренная связь';
  if (ar >= 0.3) return 'Слабая связь';
  return 'Связь почти отсутствует';
}

function hypothesisText(mean, threshold, op) {
  const ok = (op === 'gt' && mean > threshold) || (op === 'lt' && mean < threshold) || (op === 'ne' && Math.abs(mean - threshold) > 1e-9);
  return ok ? 'H1 поддерживается на уровне среднего' : 'H1 не поддерживается на уровне среднего';
}

function statHtml(rows) {
  return `<div class="result-table-wrap"><table class="result-table st-kv-table"><tbody>${
    rows.map(([k, v]) => `<tr><td class="st-key">${escHtml(String(k))}</td><td>${escHtml(String(v))}</td></tr>`).join('')
  }</tbody></table></div>`;
}

function statRowsHtml(rows, columns, headers) {
  const hdrs = headers || columns;
  let html = '<div class="result-table-wrap" style="margin-top:0.75rem"><table class="result-table"><thead><tr>';
  html += hdrs.map(c => `<th>${escHtml(c)}</th>`).join('');
  html += '</tr></thead><tbody>';
  rows.forEach(r => {
    html += '<tr>' + columns.map(c => `<td>${escHtml(String(r[c] ?? ''))}</td>`).join('') + '</tr>';
  });
  html += '</tbody></table></div>';
  return html;
}

function corrMatrixHtml(cols, matrix) {
  const n = cols.length;
  let html = '<div class="result-table-wrap" style="margin-top:0.5rem"><table class="result-table corr-matrix">';
  html += '<thead><tr><th></th>' + cols.map(c => `<th>${escHtml(c)}</th>`).join('') + '</tr></thead><tbody>';
  matrix.forEach((row, i) => {
    html += `<tr><td class="st-key">${escHtml(cols[i])}</td>` + row.map((v, j) => {
      const cls = i === j ? 'corr-diag' : v > 0 ? 'corr-pos' : 'corr-neg';
      const intensity = Math.round(Math.abs(v) * 180);
      const bg = i === j ? `rgba(91,110,245,0.15)` :
        v > 0 ? `rgba(52,199,89,${(Math.abs(v)*0.4).toFixed(2)})` :
                `rgba(255,55,95,${(Math.abs(v)*0.4).toFixed(2)})`;
      return `<td class="${cls}" style="background:${bg};text-align:center">${v}</td>`;
    }).join('') + '</tr>';
  });
  html += '</tbody></table></div>';
  return html;
}

function freqHtml(field, total, unique, rows) {
  const maxCount = rows[0]?.count || 1;
  let html = `<div class="st-section-title">Поле: ${escHtml(field)} · Всего: ${total} · Уникальных: ${unique}</div>`;
  html += '<div class="result-table-wrap"><table class="result-table"><thead><tr>';
  html += '<th>Значение</th><th>Кол-во</th><th>%</th><th>Накоп.%</th><th style="width:120px">Частота</th>';
  html += '</tr></thead><tbody>';
  rows.forEach(r => {
    const barW = Math.round(r.count / maxCount * 100);
    html += `<tr>
      <td>${escHtml(r.value)}</td>
      <td style="text-align:right">${r.count}</td>
      <td style="text-align:right">${r.pct}</td>
      <td style="text-align:right">${r.cum_pct}</td>
      <td><div class="freq-bar" style="width:${barW}%"></div></td>
    </tr>`;
  });
  html += '</tbody></table></div>';
  return html;
}

function anovaHtml(res) {
  let html = `<div class="st-section-title">Однофакторный ANOVA · k=${res.k} групп · N=${res.N}</div>`;
  const anovaTable = [
    { source:'Между группами', SS:res.SSB, df:res.dfB, MS:res.MSB, F:res.F, p:res.p },
    { source:'Внутри групп',   SS:res.SSW, df:res.dfW, MS:res.MSW, F:'',   p:'' },
  ];
  html += statRowsHtml(anovaTable, ['source','SS','df','MS','F','p'],
    ['Источник вариации','SS','df','MS','F','p-value']);
  html += `<div class="st-conclusion ${res.significant ? 'st-sig' : 'st-ns'}">`;
  html += res.significant
    ? `✓ Значимые различия между группами (F=${res.F}, p=${res.p} < 0.05)`
    : `— Значимых различий не обнаружено (F=${res.F}, p=${res.p} ≥ 0.05)`;
  html += '</div>';
  html += '<div class="st-section-title" style="margin-top:0.75rem">Статистика по группам</div>';
  html += statRowsHtml(res.groups, ['group','n','mean','sd','min','max'],
    ['Группа','N','Mean','SD','Min','Max']);
  return html;
}

function exportStatsReport(format) {
  if (!lastStatsReport) {
    showToast('Сначала выполните статистический расчет', 'warn');
    return;
  }
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  if (format === 'json') {
    const text = JSON.stringify(lastStatsReport, null, 2);
    downloadText(`stats-report-${stamp}.json`, 'application/json', text);
    return;
  }
  const summaryRows = Object.entries(lastStatsReport.summary || {}).map(([k, v]) => ({ section: 'summary', key: k, value: v }));
  const detailRows = (lastStatsReport.rows || []).map(r => ({ section: 'detail', ...r }));
  const all = summaryRows.concat(detailRows);
  if (!all.length) {
    downloadText(`stats-report-${stamp}.csv`, 'text/csv', 'section,key,value\n');
    return;
  }
  const cols = Array.from(all.reduce((s, r) => {
    Object.keys(r).forEach(k => s.add(k));
    return s;
  }, new Set()));
  const lines = [cols.join(',')];
  all.forEach(r => {
    lines.push(cols.map(c => csvEscape(r[c])).join(','));
  });
  downloadText(`stats-report-${stamp}.csv`, 'text/csv', lines.join('\n'));
}

/* ── Save stats result to backend ── */
const STAT_METHOD_LABELS = {
  describe: 'Описательная статистика',
  percentile_analysis: 'Квантили и процентили',
  freq: 'Частотный анализ',
  corr: 'Корреляция Пирсона',
  corr_matrix: 'Матрица корреляций',
  covariance: 'Ковариация',
  regression: 'Линейная регрессия',
  moving_avg: 'Скользящее среднее',
  exp_smooth: 'Экспоненциальное сглаживание',
  ci_mean: 'Доверительный интервал',
  ttest: 't-тест',
  ftest: 'F-тест',
  anova: 'ANOVA',
  ztest: 'z-тест',
  multi_hyp: 'Пакетная проверка гипотез',
  normalize: 'Нормализация',
  rank: 'Ранжирование',
  bootstrap_mean: 'Bootstrap',
  permutation_diff: 'Permutation test',
};

function saveStatsResult() {
  if (!lastStatsReport) {
    showToast('Сначала выполните статистический расчёт', 'warn');
    return;
  }

  /* Build a default name */
  const method = lastStatsReport.method || 'stats';
  const label  = STAT_METHOD_LABELS[method] || method;
  const xCol   = document.getElementById('st-col-x')?.value || '';
  const yCol   = document.getElementById('st-col-y')?.value || '';
  const fieldPart = [xCol, yCol].filter(Boolean).join(' / ');
  const sql    = (document.getElementById('an-sql')?.value || '').trim();
  const defaultName = fieldPart ? `${label}: ${fieldPart}` : label;

  const nameEl = document.getElementById('save-result-name');
  if (nameEl) nameEl.value = defaultName;

  /* Store pending type so confirmSaveResult knows to use stats data */
  document.getElementById('save-result-modal')._saveType = 'stats';
  document.getElementById('save-result-modal').style.display = 'flex';
  setTimeout(() => nameEl?.select(), 50);
}

function _statsReportToTable(report) {
  /* Convert lastStatsReport {summary, rows} → {columns, rows} for the API */
  const summaryEntries = Object.entries(report.summary || {});
  const detailRows     = report.rows || [];

  if (detailRows.length) {
    /* Tabular result (correlation matrix, ANOVA, normalize, rank, MA, regression…) */
    const detailCols = Array.from(detailRows.reduce((s, r) => {
      Object.keys(r).forEach(k => s.add(k)); return s;
    }, new Set()));

    const allCols  = summaryEntries.length
      ? ['параметр', 'значение', ...detailCols]   /* mixed: summary block + detail table */
      : detailCols;

    const allRows = [];
    /* summary as leading rows */
    summaryEntries.forEach(([k, v]) => {
      const row = new Array(allCols.length).fill('');
      row[0] = k; row[1] = String(v ?? '');
      allRows.push(row);
    });
    /* separator if both present */
    if (summaryEntries.length && detailRows.length) {
      allRows.push(new Array(allCols.length).fill('---'));
    }
    /* detail rows */
    detailRows.forEach(r => {
      const row = new Array(allCols.length).fill('');
      detailCols.forEach((c, i) => {
        const offset = summaryEntries.length ? 2 : 0;
        row[offset + i] = String(r[c] ?? '');
      });
      allRows.push(row);
    });
    return { columns: allCols, rows: allRows };
  }

  /* Summary-only result (describe, percentiles, t-test…) */
  return {
    columns: ['параметр', 'значение'],
    rows: summaryEntries.map(([k, v]) => [k, String(v ?? '')]),
  };
}

function downloadText(filename, mime, text) {
  const blob = new Blob([text], { type: `${mime};charset=utf-8` });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 500);
  showToast(`Экспортирован отчет: ${filename}`, 'ok');
}

function csvEscape(v) {
  const s = String(v ?? '');
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function meanOf(arr) {
  return arr.length ? arr.reduce((s, v) => s + v, 0) / arr.length : 0;
}

function shuffleInPlace(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = (Math.random() * (i + 1)) | 0;
    const t = arr[i]; arr[i] = arr[j]; arr[j] = t;
  }
}

function rnd(v) {
  return Math.round((Number(v) || 0) * 10000) / 10000;
}


/* ── API Keys ── */
async function loadApiKeys() {
  const el = document.getElementById('settings-apikeys-list');
  if (!el) return;
  try {
    const keys = await apiFetch('/api/auth/apikeys');
    if (!Array.isArray(keys) || !keys.length) {
      el.innerHTML = '<div class="settings-loading">Ключей нет</div>';
      return;
    }
    const roleLabel = { admin: 'admin', analyst: 'analyst', viewer: 'viewer' };
    el.innerHTML = keys.map(k => {
      const keyId = escAttr(k.key || '');
      const created = k.created_at ? new Date(k.created_at * 1000).toLocaleDateString('ru') : '—';
      return `
        <div class="settings-row" id="akrow-${keyId}">
          <div class="settings-row-label">
            <div class="settings-row-title" style="font-family:var(--mono);font-size:.85rem">${escHtml(k.key || '—')}</div>
            <div class="settings-row-desc">
              user: <b>${escHtml(k.user_id || '—')}</b> · роль: <b>${escHtml(k.role || '—')}</b> · создан: ${escHtml(created)}
            </div>
          </div>
          <button class="btn btn-sm btn-danger" onclick="deleteApiKey('${keyId}')">Удалить</button>
        </div>`;
    }).join('');
  } catch(err) {
    el.innerHTML = `<div class="settings-loading" style="color:var(--red)">${escHtml(String(err))}</div>`;
  }
}

async function createApiKey() {
  const userEl = document.getElementById('new-apikey-user');
  const roleEl = document.getElementById('new-apikey-role');
  const user_id = (userEl?.value || '').trim();
  const role    = roleEl?.value || 'analyst';
  if (!user_id) { showToast('Введите user_id', 'warn'); return; }
  try {
    const res = await apiPost('/api/auth/apikeys', { user_id, role });
    showToast(`Ключ создан: ${res.key || '—'}`, 'ok');
    if (userEl) userEl.value = '';
    loadApiKeys();
  } catch(err) {
    showToast(`Ошибка: ${err}`, 'error');
  }
}

async function deleteApiKey(key) {
  if (!confirm('Удалить API-ключ?')) return;
  try {
    await apiDelete(`/api/auth/apikeys/${encodeURIComponent(key)}`);
    showToast('Ключ удалён', 'ok');
    const row = document.getElementById(`akrow-${key}`);
    if (row) row.remove();
  } catch(err) {
    showToast(`Ошибка: ${err}`, 'error');
  }
}

/* ═══════════════════════════════════════════════════
   SETTINGS
═══════════════════════════════════════════════════ */
async function loadSettings() {
  applyPrefsToSettingsForm();

  /* ── Server stat cards ── */
  const grid = document.getElementById('server-info-grid');
  try {
    const h = await apiFetch('/health');
    const m = h.metrics || {};
    const cards = [
      ['Статус',              `<span class="badge badge-ok">${escHtml(h.status||'ok')}</span>`],
      ['Версия',              escHtml(h.version || '1.0.0')],
      ['Аптайм',              fmtUptime(m.uptime || 0)],
      ['Строк загружено',     fmtNum(m.total_rows || 0)],
      ['Запросов выполнено',  fmtNum(m.total_queries || 0)],
      ['Запусков конвейеров', fmtNum(m.total_pipelines_run || 0)],
    ];
    grid.innerHTML = cards.map(([label, val]) =>
      `<div class="stat-card"><div class="stat-val" style="font-size:1.25rem">${val}</div><div class="stat-label">${label}</div></div>`
    ).join('');
  } catch (err) {
    grid.innerHTML = `<div class="settings-loading" style="color:var(--red)">Нет связи с сервером</div>`;
  }

  /* ── Tables management ── */
  const tbl = document.getElementById('settings-tables-list');
  try {
    const tables = await apiFetch('/api/tables');
    if (!tables.length) {
      tbl.innerHTML = '<div class="settings-loading">Таблиц нет</div>';
    } else {
      tbl.innerHTML = tables.map(t => `
        <div class="settings-row" id="strow-${escAttr(t.name)}">
          <div class="settings-row-label">
            <div class="settings-row-title">${escHtml(t.name)}</div>
            <div class="settings-row-desc">${fmtNum(t.rows||0)} строк · ${(t.columns||[]).length} столбцов ·
              <span class="source-badge ${t.source==='ingest'?'ingest':'pipeline'}">${t.source==='ingest'?'загрузка':'конвейер'}</span>
            </div>
          </div>
          <button class="btn btn-sm btn-danger" onclick="settingsDropTable('${escAttr(t.name)}')">Удалить</button>
        </div>`).join('');
    }
  } catch (err) {
    tbl.innerHTML = `<div class="settings-loading" style="color:var(--red)">${escHtml(String(err))}</div>`;
  }
  loadApiKeys();
  loadClusterStatus();
  renderConnectionsPanel();
}

/* ── External protocol connections panel ────────────────────────
 * Lists the four protocols DataFlow OS exposes: HTTP REST, Arrow
 * Flight (gRPC), PostgreSQL wire, MCP (stdio). Connection strings
 * are templated against the current location.host. The actual
 * runtime status of each (whether the pgwire / Flight / MCP
 * server is started) isn't exposed by the gateway today, so we
 * present them as documented endpoints with copy-to-clipboard. */
function _connectionCard(opts) {
  const { icon, title, status, body, docs } = opts;
  return `<div class="settings-row" style="display:block;padding:.75rem 0;border-bottom:1px solid var(--border)">
    <div style="display:flex;align-items:center;gap:.5rem;margin-bottom:.5rem">
      <span style="font-size:1.2rem">${icon}</span>
      <strong>${title}</strong>
      ${status ? `<span class="badge badge-ok" style="font-size:.65rem">${status}</span>` : ''}
      ${docs ? `<a href="${docs}" target="_blank" style="margin-left:auto;font-size:.7rem;color:var(--muted)">docs ↗</a>` : ''}
    </div>
    ${body}
  </div>`;
}

function _copyableLine(label, code) {
  const id = 'cp_' + Math.random().toString(36).slice(2, 9);
  return `<div style="display:flex;align-items:center;gap:.4rem;margin:.25rem 0;font-size:.75rem">
    <span style="min-width:90px;color:var(--muted)">${label}</span>
    <code id="${id}" style="flex:1;background:var(--surface2);padding:.2rem .4rem;border-radius:3px;overflow:auto;white-space:pre-wrap;word-break:break-all">${escHtml(code)}</code>
    <button class="btn btn-sm" onclick="navigator.clipboard.writeText(document.getElementById('${id}').textContent);showToast('Скопировано','ok')">⧉</button>
  </div>`;
}

function renderConnectionsPanel() {
  const el = document.getElementById('settings-connections');
  if (!el) return;
  const h = location.hostname || 'localhost';
  const port = location.port || '8080';
  const base = `${location.protocol}//${h}:${port}`;

  const cards = [];

  /* 1. HTTP REST */
  cards.push(_connectionCard({
    icon: '🌐',
    title: 'HTTP REST API',
    status: 'always on',
    body:
      _copyableLine('Base URL',  base) +
      _copyableLine('curl test', `curl -X POST ${base}/api/auth/token -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}'`) +
      `<div style="font-size:.7rem;color:var(--muted);margin-top:.3rem">JSON ввод/вывод. Auth: JWT через POST /api/auth/token. Используется UI и SDK.</div>`,
    docs: 'docs/API.md',
  }));

  /* 2. PostgreSQL wire */
  cards.push(_connectionCard({
    icon: '🐘',
    title: 'PostgreSQL wire-protocol',
    status: 'opt-in (pgwire_port в config)',
    body:
      _copyableLine('psql',  `PGPASSWORD=admin psql -h ${h} -p 5432 -U admin -d dataflow`) +
      _copyableLine('psycopg2', `psycopg2.connect("host=${h} port=5432 user=admin password=admin dbname=dataflow")`) +
      _copyableLine('JDBC', `jdbc:postgresql://${h}:5432/dataflow?user=admin&password=admin`) +
      `<div style="font-size:.7rem;color:var(--muted);margin-top:.3rem">Включи в config: <code>"pgwire_enabled":1, "pgwire_port":5432</code>. Поддержано: Simple+Extended Query, pg_catalog, type inference. БЕЗ TLS — для loopback/VPN.</div>`,
    docs: 'docs/POSTGRES_WIRE.md',
  }));

  /* 3. Arrow Flight */
  cards.push(_connectionCard({
    icon: '✈️',
    title: 'Apache Arrow Flight (gRPC)',
    status: 'отдельный бинарь dfo_flight_server',
    body:
      _copyableLine('grpc URL', `grpc://${h}:8815`) +
      _copyableLine('PyArrow', `pyarrow.flight.FlightClient("grpc://${h}:8815").do_get(pyarrow.flight.Ticket(b"sql:SELECT 1"))`) +
      _copyableLine('запуск', `dfo_flight_server --gateway ${base} --api-key $TOKEN --port 8815`) +
      `<div style="font-size:.7rem;color:var(--muted);margin-top:.3rem">Zero-JSON Arrow IPC — ~10× быстрее REST для больших query. Ставится через <code>conda install -c conda-forge libarrow-all</code>, потом <code>make flight</code>.</div>`,
    docs: 'docs/ARROW_FLIGHT.md',
  }));

  /* 4. MCP (Model Context Protocol) */
  cards.push(_connectionCard({
    icon: '🤖',
    title: 'MCP (Model Context Protocol)',
    status: 'для AI-агентов',
    body:
      _copyableLine('бинарь', `dfo_mcp_server --gateway ${base} --api-key $TOKEN`) +
      _copyableLine('Claude Desktop', `~/Library/Application Support/Claude/claude_desktop_config.json`) +
      `<pre style="background:var(--surface2);padding:.4rem;border-radius:3px;font-size:.7rem;margin:.3rem 0">{
  "mcpServers": {
    "dataflow-os": {
      "command": "dfo_mcp_server",
      "args": ["--gateway", "${base}", "--api-key", "&lt;JWT&gt;"]
    }
  }
}</pre>` +
      `<div style="font-size:.7rem;color:var(--muted);margin-top:.3rem">10 tools: query / list_tables / describe_table / ingest_csv / list_pipelines / get_metrics / analyze / + ещё 3. Подключается к Claude Desktop, Cursor, Cline.</div>`,
    docs: 'docs/MCP.md',
  }));

  el.innerHTML = cards.join('');
}

async function settingsDropTable(name) {
  if (!confirm(`Удалить таблицу "${name}"? Это действие необратимо.`)) return;
  try {
    await apiFetch(`/api/tables/${encodeURIComponent(name)}`, 'DELETE');
    showToast(`Таблица "${name}" удалена`, 'ok');
    const row = document.getElementById(`strow-${name}`);
    if (row) row.remove();
  } catch (err) {
    showToast(`Ошибка: ${err}`, 'error');
  }
}


/* ── Saved Results ── */
async function loadSavedResults() {
  try {
    const list = await apiGet('/api/analytics/results');
    const el = document.getElementById('an-saved-list');
    if (!el) return;
    if (!list.length) {
      el.innerHTML = '<div class="qs-empty">Нет сохранённых результатов.<br>Нажмите «+ Добавить аналитику» чтобы создать первый анализ.</div>';
      return;
    }
    el.innerHTML = '';
    list.forEach(r => {
      const card = document.createElement('div');
      card.className = 'table-card';
      card.id = `sr_${r.id}`;
      const cols = Array.isArray(r.columns_json) ? r.columns_json
        : (r.columns_json ? JSON.parse(r.columns_json) : []);
      const pills = cols.map(c => `<span class="col-pill">${escHtml(c)}</span>`).join('');
      card.innerHTML = `
        <div class="table-card-head">
          <h3>${escHtml(r.name)}</h3>
          <button class="btn btn-sm btn-danger" title="Удалить">✕</button>
        </div>
        <div class="meta">${r.row_count} строк · ${new Date(r.created_at*1000).toLocaleDateString('ru')}</div>
        ${pills ? `<div class="col-list">${pills}</div>` : ''}
      `;
      card.querySelector('button').addEventListener('click', ev => {
        ev.stopPropagation();
        deleteSavedResult(r.id);
      });
      card.onclick = () => openSavedResult(r.id);
      el.appendChild(card);
    });
  } catch(e) { console.error('loadSavedResults', e); }
}

function saveAnalyticsResult() {
  if (!analyticsState.currentRows.length) return;
  const nameEl = document.getElementById('save-result-name');
  const sql = (document.getElementById('an-sql')?.value || '').trim();
  if (nameEl) nameEl.value = sql.replace(/^SELECT\s+/i,'').substring(0,60);
  document.getElementById('save-result-modal').style.display = 'flex';
  setTimeout(() => nameEl?.focus(), 50);
}

function closeSaveModal() {
  const m = document.getElementById('save-result-modal');
  m.style.display = 'none';
  delete m._saveType;
}

async function confirmSaveResult() {
  const name = (document.getElementById('save-result-name')?.value || '').trim();
  if (!name) { document.getElementById('save-result-name')?.focus(); return; }
  const modal   = document.getElementById('save-result-modal');
  const saveType = modal._saveType || 'query';
  const sql = (document.getElementById('an-sql')?.value || '').trim();

  let columns, rows;
  if (saveType === 'stats' && lastStatsReport) {
    const tbl = _statsReportToTable(lastStatsReport);
    columns = tbl.columns;
    rows    = tbl.rows;
  } else {
    columns = analyticsState.currentCols;
    rows    = analyticsState.currentRows.map(r => columns.map(c => r[c] ?? ''));
  }

  try {
    await apiPost('/api/analytics/results', { name, sql, columns, rows });
    closeSaveModal();
    showToast(`Сохранено: "${name}"`, 'ok');
    closeAnalyticsEditor();
    loadSavedResults();
  } catch(e) { alert('Ошибка при сохранении: ' + e.message); }
}

async function openSavedResult(id) {
  try {
    const r = await apiGet(`/api/analytics/results/${id}`);
    const cols = JSON.parse(r.columns_json || '[]');
    const rawRows = JSON.parse(r.rows_json || '[]');
    const rows = rawRows.map(arr => {
      const obj = {};
      cols.forEach((c,i) => { obj[c] = arr[i] ?? ''; });
      return obj;
    });

    document.getElementById('an-view-title').textContent = r.name;
    document.getElementById('an-view-meta').textContent =
      `${r.row_count} строк · ${new Date(r.created_at * 1000).toLocaleDateString('ru')}`;
    document.getElementById('an-view-sql').value = r.sql_text || '';
    document.getElementById('an-view-status').textContent = '';

    renderViewCharts(cols, rows);
    renderViewDataTable(rows, cols);

    showAnScreen('view');
  } catch(e) { alert('Ошибка загрузки: ' + e.message); }
}

function renderViewDataTable(rows, cols) {
  const card = document.getElementById('an-view-data-card');
  const el   = document.getElementById('an-view-table');
  if (!rows.length) { card.style.display = 'none'; return; }
  card.style.display = '';
  const head = cols.map(c => `<th>${escHtml(c)}</th>`).join('');
  const body = rows.slice(0, 500).map(r =>
    `<tr>${cols.map(c => `<td>${escHtml(String(r[c] ?? ''))}</td>`).join('')}</tr>`
  ).join('');
  el.innerHTML = `<div class="table-scroll"><table class="result-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}

async function runViewQuery() {
  const sql = document.getElementById('an-view-sql').value.trim();
  if (!sql) return;
  const status = document.getElementById('an-view-status');
  status.textContent = 'Выполняется…';
  const t0 = performance.now();
  try {
    const data = await apiPost('/api/tables/query', { sql });
    const cols = data.columns || [];
    const rows = (data.rows || []).map(r => {
      const obj = {}, vals = Array.isArray(r) ? r : cols.map(c => r[c]);
      cols.forEach((c, i) => { obj[c] = vals[i]; });
      return obj;
    });
    status.textContent = `${rows.length} строк · ${(performance.now()-t0).toFixed(0)} мс`;
    renderViewDataTable(rows, cols);
  } catch(err) {
    let msg = String(err);
    try { const j = JSON.parse(msg.replace(/^Error:\s*/,'')); if (j.error) msg = j.error; } catch(_) {}
    status.textContent = `Ошибка: ${msg}`;
  }
}

function renderViewCharts(cols, rows) {
  /* reuse analyticsState temporarily for chart rendering */
  const prevRows = analyticsState.currentRows;
  const prevCols = analyticsState.currentCols;
  analyticsState.currentRows = rows;
  analyticsState.currentCols = cols;

  const grid = document.getElementById('an-view-charts');
  grid.innerHTML = '';

  const numCols = cols.filter(c => rows.slice(0,20).some(r => Number.isFinite(Number(r[c])) && r[c] !== ''));
  const lblCols = cols.filter(c => !numCols.includes(c));
  const xCol    = lblCols.find(c => !/^id$|_id$/i.test(c)) || lblCols[0] || cols[0] || '';
  const nonId   = numCols.filter(c => !/^id$|_id$/i.test(c));
  const yCol    = nonId[0] || numCols[0] || cols[1] || '';
  const dateCol = lblCols.find(c => /date|time|month|year|day|week/i.test(c));

  const charts = [{ type: 'bar', xCol, yCol }];
  if (dateCol && yCol)        charts.push({ type: 'area',    xCol: dateCol, yCol });
  else if (nonId.length >= 2) charts.push({ type: 'scatter', xCol: nonId[0], yCol: nonId[1] });

  charts.forEach(({ type, xCol, yCol }, i) => {
    const cid = `vc${i}`;
    const wrap = document.createElement('div');
    wrap.className = 'card an-view-chart-card';
    /* drawChartOnCanvas needs: canvas#c_{id}, div#cw_{id}, div#ct_{id}; renderChartLegend needs div#cl_{id} */
    wrap.innerHTML = `
      <div id="cw_${cid}" class="an-chart-canvas-wrap">
        <canvas id="c_${cid}"></canvas>
        <div id="ct_${cid}" class="analytics-tooltip hidden"></div>
      </div>
      <div id="cl_${cid}" class="analytics-legend"></div>`;
    grid.appendChild(wrap);

    const cfg = newChartCfg({ type, xCol, yCol });
    cfg.id = cid;
    requestAnimationFrame(() => {
      const pts = aggregateForChart(cfg, rows);
      const aggL = cfg.agg === 'count' ? 'COUNT(*)' : `${cfg.agg.toUpperCase()}(${cfg.yCol})`;
      drawChartOnCanvas(cid, pts, type, aggL, xCol);
      renderChartLegend(cid, pts, type);
    });
  });

  analyticsState.currentRows = prevRows;
  analyticsState.currentCols = prevCols;
}

async function deleteSavedResult(id) {
  if (!confirm('Удалить этот результат?')) return;
  try {
    await apiDelete(`/api/analytics/results/${id}`);
    loadSavedResults();
  } catch(e) { alert('Ошибка: ' + e.message); }
}


/* ═══════════════════════════════════════════════════
   API HELPERS
═══════════════════════════════════════════════════ */
async function apiFetch(path, method = 'GET', body = null) {
  const headers = {};
  if (jwtToken) headers['Authorization'] = `Bearer ${jwtToken}`;
  const resp = await fetch(API + path, { method, headers, body });
  if (resp.status === 401) {
    // Unauthorized, redirect to login
    logout();
    return;
  }
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return resp.json();
}

async function apiGet(path) {
  const headers = {};
  if (jwtToken) headers['Authorization'] = `Bearer ${jwtToken}`;
  const resp = await fetch(API + path, { method: 'GET', headers });
  if (resp.status === 401) { logout(); return; }
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return resp.json();
}

async function apiDelete(url) {
  const headers = {};
  if (jwtToken) headers['Authorization'] = `Bearer ${jwtToken}`;
  const r = await fetch(API + url, { method: 'DELETE', headers });
  if (r.status === 401) { logout(); return; }
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

async function apiPost(path, body) {
  const headers = { 'Content-Type': 'application/json' };
  if (jwtToken) headers['Authorization'] = `Bearer ${jwtToken}`;
  const resp = await fetch(API + path, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
  if (resp.status === 401) { logout(); return; }
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || `HTTP ${resp.status}`);
  }
  return resp.json();
}

async function apiPostRaw(path, body, contentType) {
  const headers = { 'Content-Type': contentType };
  if (jwtToken) headers['Authorization'] = `Bearer ${jwtToken}`;
  const resp = await fetch(API + path, {
    method: 'POST',
    headers,
    body,
  });
  if (resp.status === 401) { logout(); return; }
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || `HTTP ${resp.status}`);
  }
  return resp.json();
}


/* ═══════════════════════════════════════════════════
   FORMATTERS / UTILS
═══════════════════════════════════════════════════ */
function fmtNum(n)    { return Number(n).toLocaleString('ru'); }
function fmtBytes(n)  { if (n < 1024) return n+' Б'; if (n < 1048576) return (n/1024).toFixed(1)+' КБ'; return (n/1048576).toFixed(1)+' МБ'; }
function fmtUptime(s) { const h=Math.floor(s/3600),m=Math.floor((s%3600)/60); return `${h}ч ${m}м`; }
function pluralRows(n) {
  const mod10 = n % 10, mod100 = n % 100;
  if (mod10 === 1 && mod100 !== 11) return 'строка';
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 10 || mod100 >= 20)) return 'строки';
  return 'строк';
}

function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

/* ═══════════════════════════════════════════════════
   AUTH
═══════════════════════════════════════════════════ */
function showLogin() {
  document.getElementById('login-screen').style.display = 'flex';
  document.getElementById('main-app').style.display = 'none';
}

function hideLogin() {
  document.getElementById('login-screen').style.display = 'none';
  document.getElementById('main-app').style.display = 'flex';
}

async function login(username, password) {
  try {
    const resp = await fetch(API + '/api/auth/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    });
    if (!resp.ok) throw new Error('Invalid credentials');
    const data = await resp.json();
    jwtToken = data.token;
    sessionStorage.setItem('dfo_jwt', jwtToken);
    isLoggedIn = true;
    hideLogin();
    initApp();
  } catch (e) {
    alert('Login failed: ' + e.message);
  }
}

function logout() {
  jwtToken = null;
  sessionStorage.removeItem('dfo_jwt');
  isLoggedIn = false;
  showLogin();
}

document.getElementById('login-form').addEventListener('submit', e => {
  e.preventDefault();
  const username = document.getElementById('login-username').value;
  const password = document.getElementById('login-password').value;
  login(username, password);
});

/* ═══════════════════════════════════════════════════
   INIT
═══════════════════════════════════════════════════ */
function initApp() {
  loadPrefs();
  applyPrefs();
  applyPrefsToSettingsForm();

  const { view, id } = parseHash();

  if (view === 'builder' && id) {
    /* '#builder/<id>' — fetch the pipeline and reopen the builder on it. */
    restoreBuilderFromHash(id);
  } else {
    const v = VALID_VIEWS.has(view) ? view : 'pipelines';
    /* '#builder' without an id is a stale URL — go to list. */
    const fallback = (v === 'builder') ? 'pipelines' : v;
    switchView(fallback, { pushState: false });
    if (fallback !== view) history.replaceState({ view: fallback }, '', '#' + fallback);
  }

  connectWS();
  restartMetricsTimer(); /* clears any existing timer before starting a new one */
}

document.addEventListener('DOMContentLoaded', () => {
  if (isLoggedIn) {
    hideLogin();
    initApp();
  } else {
    showLogin();
  }
});
function escAttr(s) {
  return String(s).replace(/'/g,'&#39;').replace(/"/g,'&quot;').replace(/\n/g,'').replace(/\r/g,'');
}

function safeParse(str, def) {
  if (!str) return def;
  try { return JSON.parse(str); } catch (_) { return def; }
}

function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

/* legacy compat */
function splitCSVLine(line) { return splitCSVLineDelim(line, ','); }


/* ═══════════════════════════════════════════════════
   MATERIALIZED VIEWS
═══════════════════════════════════════════════════ */
const MV_REFRESH = ['Вручную', 'По расписанию'];

async function loadMatviews() {
  const el = document.getElementById('matviews-list');
  try {
    const list = await apiFetch('/api/matviews');
    if (!list || !list.length) {
      el.innerHTML = '<div class="settings-loading">Нет представлений. Создайте первое.</div>';
      return;
    }
    el.innerHTML = list.map(mv => `
      <div class="settings-row">
        <div class="settings-row-label">
          <div class="settings-row-title">${escHtml(mv.name)}
            ${mv.is_stale ? '<span class="badge badge-warn" style="margin-left:.4rem;font-size:.7rem">устарело</span>' : ''}
          </div>
          <div class="settings-row-desc">
            ${MV_REFRESH[mv.refresh_mode] || 'Вручную'} ·
            ${fmtNum(mv.row_count || 0)} строк ·
            ${mv.last_refreshed_at ? 'обновлено ' + new Date(mv.last_refreshed_at * 1000).toLocaleString('ru') : 'ещё не обновлялось'}
          </div>
        </div>
        <div style="display:flex;gap:.4rem">
          <button class="btn btn-sm" onclick="queryMatview('${escAttr(mv.name)}')">Запрос</button>
          <button class="btn btn-sm" onclick="refreshMatview('${escAttr(mv.name)}')">↻ Обновить</button>
          <button class="btn btn-sm btn-danger" onclick="dropMatview('${escAttr(mv.name)}')">Удалить</button>
        </div>
      </div>`).join('');
  } catch (err) {
    el.innerHTML = `<div class="settings-loading" style="color:var(--red)">${escHtml(String(err))}</div>`;
  }
}

function openCreateMatview() {
  document.getElementById('matview-create-form').style.display = '';
  document.getElementById('mv-name').focus();
}

function closeCreateMatview() {
  document.getElementById('matview-create-form').style.display = 'none';
  document.getElementById('mv-status').textContent = '';
}

document.addEventListener('DOMContentLoaded', () => {
  const sel = document.getElementById('mv-refresh-mode');
  if (sel) sel.addEventListener('change', () => {
    document.getElementById('mv-cron-group').style.display = sel.value === '1' ? '' : 'none';
  });
});

async function createMatview() {
  const name = document.getElementById('mv-name').value.trim();
  const sql  = document.getElementById('mv-sql').value.trim();
  const status = document.getElementById('mv-status');
  if (!name || !sql) { status.textContent = 'Укажите название и SQL'; return; }
  const sources = document.getElementById('mv-sources').value.split(',').map(s => s.trim()).filter(Boolean);
  const refresh_mode = parseInt(document.getElementById('mv-refresh-mode').value);
  const refresh_cron = document.getElementById('mv-cron').value.trim();
  try {
    await apiPost('/api/matviews', { name, definition_sql: sql, source_tables: sources, refresh_mode, refresh_cron });
    status.textContent = '';
    closeCreateMatview();
    loadMatviews();
    showToast(`Представление ${name} создано`, 'ok');
  } catch (err) {
    status.textContent = String(err);
  }
}

async function refreshMatview(name) {
  try {
    await apiPost(`/api/matviews/${encodeURIComponent(name)}/refresh`, {});
    showToast(`${name} обновлено`, 'ok');
    loadMatviews();
  } catch (err) {
    showToast(String(err), 'error');
  }
}

async function dropMatview(name) {
  if (!confirm(`Удалить представление "${name}"?`)) return;
  try {
    await apiFetch(`/api/matviews/${encodeURIComponent(name)}`, 'DELETE');
    showToast(`${name} удалено`, 'ok');
    loadMatviews();
  } catch (err) {
    showToast(String(err), 'error');
  }
}

function queryMatview(name) {
  openTablePreview(name);
}

/* ═══════════════════════════════════════════════════
   SECURITY — RBAC + AUDIT
═══════════════════════════════════════════════════ */
const RBAC_ROLES   = ['Admin', 'Analyst', 'Viewer'];
const RBAC_ACTIONS = ['', 'Чтение', 'Запись', 'Чтение + Запись'];
const AUDIT_TYPES  = ['', 'QUERY', 'INGEST', 'PIPELINE_RUN', 'AUTH_LOGIN', 'AUTH_FAIL', 'SCHEMA_CHANGE', 'POLICY_CHANGE'];

function switchSecTab(tab) {
  ['rbac','audit'].forEach(t => {
    document.getElementById(`sec-tab-${t}`).classList.toggle('active', t === tab);
    document.getElementById(`sec-pane-${t}`).style.display = t === tab ? '' : 'none';
  });
  if (tab === 'audit') loadAuditLog();
}

/* ── RBAC ── */
async function loadRbacPolicies() {
  const el = document.getElementById('rbac-policies-list');
  try {
    const list = await apiFetch('/api/rbac/policies');
    if (!list || !list.length) {
      el.innerHTML = '<div class="settings-loading">Политик нет. Добавьте первую.</div>';
      return;
    }
    el.innerHTML = `<table class="result-table" style="width:100%">
      <thead><tr><th>ID</th><th>Роль</th><th>Таблица (шаблон)</th><th>Права</th><th>Фильтр строк</th><th></th></tr></thead>
      <tbody>${list.map(p => `<tr>
        <td>${p.id}</td>
        <td>${RBAC_ROLES[p.role] ?? p.role}</td>
        <td><code>${escHtml(p.table_pattern)}</code></td>
        <td>${RBAC_ACTIONS[p.allowed_actions] ?? p.allowed_actions}</td>
        <td>${p.row_filter ? `<code>${escHtml(p.row_filter)}</code>` : '—'}</td>
        <td><button class="btn btn-sm btn-danger" onclick="deleteRbacPolicy(${p.id})">Удалить</button></td>
      </tr>`).join('')}</tbody></table>`;
  } catch (err) {
    el.innerHTML = `<div class="settings-loading" style="color:var(--red)">${escHtml(String(err))}</div>`;
  }
}

async function addRbacPolicy() {
  const role    = parseInt(document.getElementById('rbac-new-role').value);
  const pattern = document.getElementById('rbac-new-pattern').value.trim();
  const actions = parseInt(document.getElementById('rbac-new-actions').value);
  const rf      = document.getElementById('rbac-new-rowfilter').value.trim();
  const status  = document.getElementById('rbac-status');
  if (!pattern) { status.textContent = 'Укажите шаблон таблицы'; return; }
  try {
    await apiPost('/api/rbac/policies', { role, table_pattern: pattern, allowed_actions: actions, row_filter: rf });
    document.getElementById('rbac-new-pattern').value = '';
    document.getElementById('rbac-new-rowfilter').value = '';
    status.textContent = '';
    loadRbacPolicies();
    showToast('Политика добавлена', 'ok');
  } catch (err) {
    status.textContent = String(err);
  }
}

async function deleteRbacPolicy(id) {
  if (!confirm('Удалить политику?')) return;
  try {
    await apiFetch(`/api/rbac/policies/${id}`, 'DELETE');
    loadRbacPolicies();
    showToast('Политика удалена', 'ok');
  } catch (err) {
    showToast(String(err), 'error');
  }
}

/* ── Audit ── */
async function loadAuditLog() {
  const el    = document.getElementById('audit-log-list');
  const user  = document.getElementById('audit-filter-user').value.trim();
  const limit = document.getElementById('audit-filter-limit').value || 50;
  const status = document.getElementById('audit-status');
  el.innerHTML = '<div class="settings-loading">Загрузка…</div>';
  try {
    let qs = `limit=${limit}`;
    if (user) qs += `&user_id=${encodeURIComponent(user)}`;
    const list = await apiFetch(`/api/audit?${qs}`);
    status.textContent = `${list.length} записей`;
    if (!list.length) {
      el.innerHTML = '<div class="settings-loading">Событий нет</div>';
      return;
    }
    el.innerHTML = `<table class="result-table" style="width:100%;font-size:.8rem">
      <thead><tr><th>Время</th><th>Тип</th><th>Пользователь</th><th>IP</th><th>Ресурс</th><th>Код</th><th>Детали</th></tr></thead>
      <tbody>${list.map(e => `<tr>
        <td style="white-space:nowrap">${new Date(e.ts * 1000).toLocaleString('ru')}</td>
        <td><span class="badge ${e.event_type === 5 ? 'badge-danger' : 'badge-ok'}">${AUDIT_TYPES[e.event_type] ?? e.event_type}</span></td>
        <td>${escHtml(e.user_id || '—')}</td>
        <td>${escHtml(e.client_ip || '—')}</td>
        <td>${escHtml(e.resource || '—')}</td>
        <td>${e.result_code === 0 ? '✓' : '<span style="color:var(--red)">✗ ' + e.result_code + '</span>'}</td>
        <td style="max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escAttr(e.action_detail||'')}">${escHtml(e.action_detail || '—')}</td>
      </tr>`).join('')}</tbody></table>`;
  } catch (err) {
    el.innerHTML = `<div class="settings-loading" style="color:var(--red)">${escHtml(String(err))}</div>`;
    status.textContent = '';
  }
}

/* ═══════════════════════════════════════════════════
   CLUSTER STATUS
═══════════════════════════════════════════════════ */
async function loadClusterStatus() {
  const el = document.getElementById('settings-cluster-status');
  if (!el) return;
  try {
    const s = await apiFetch('/api/cluster/status');
    if (!s.cluster_mode && !s.is_leader && !s.replica_count) {
      el.innerHTML = '<div class="settings-loading">Кластерный режим отключён</div>';
      return;
    }
    const replicaRows = (s.replicas || []).map(r =>
      `<div class="settings-row" style="padding:.3rem 0">
        <div class="settings-row-label">
          <div class="settings-row-title">${escHtml(r.host)}:${r.port}</div>
        </div>
        <span class="badge ${r.connected ? 'badge-ok' : 'badge-danger'}">${r.connected ? 'подключён' : 'недоступен'}</span>
      </div>`).join('');
    el.innerHTML = `
      <div class="settings-row">
        <div class="settings-row-label"><div class="settings-row-title">Роль</div></div>
        <span class="badge ${s.is_leader ? 'badge-ok' : 'badge-warn'}">${s.is_leader ? 'Лидер' : 'Реплика'}</span>
      </div>
      <div class="settings-row">
        <div class="settings-row-label"><div class="settings-row-title">Node ID</div></div>
        <code>${escHtml(s.node_id || '—')}</code>
      </div>
      <div class="settings-row">
        <div class="settings-row-label"><div class="settings-row-title">Последний LSN</div></div>
        <span>${fmtNum(s.last_acked_lsn || 0)}</span>
      </div>
      <div class="settings-row">
        <div class="settings-row-label"><div class="settings-row-title">Отставание (записей)</div></div>
        <span>${fmtNum(s.lag_count || 0)}</span>
      </div>
      ${replicaRows || '<div class="settings-loading">Реплик нет</div>'}`;
  } catch (err) {
    el.innerHTML = `<div class="settings-loading" style="color:var(--red)">${escHtml(String(err))}</div>`;
  }
}

/* ═══════════════════════════════════════════════════
   BOOT
═══════════════════════════════════════════════════ */
loadPrefs();
applyPrefs();
connectWS();
restartMetricsTimer();

/* Restore view from URL hash on initial load.
 * - Unknown / removed views (e.g. legacy '#ingest', '#query') → pipelines
 * - '#builder' on a cold load has no pipeline in memory → pipelines
 * - Valid hash → stays as-is. URL is rewritten to reflect the actual view so
 *   the next refresh keeps the user where they actually are. */
(function bootView() {
  const { view, id } = parseHash();
  /* '#builder/<id>' is handled later by initApp (it needs network).
   * Here at module-load we only show *something* valid before login resolves. */
  if (view === 'builder' && id) {
    /* leave URL alone; initApp will reopen the builder once logged in */
    return;
  }
  let v = VALID_VIEWS.has(view) ? view : 'pipelines';
  if (v === 'builder') v = 'pipelines';
  history.replaceState({ view: v }, '', '#' + v);
  switchView(v, { pushState: false });
})();
