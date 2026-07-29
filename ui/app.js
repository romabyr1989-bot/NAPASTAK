/* NAPASTAK — frontend application */
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
/* Токен в localStorage — переживает закрытие браузера, чтобы не логиниться каждый раз
 * (sessionStorage очищался при закрытии вкладки). Протухший токен сбрасываем ниже. */
let jwtToken = localStorage.getItem('dfo_jwt') || null;
let isLoggedIn = !!jwtToken;

/* Текущий пользователь из JWT (payload {sub, role}). Единственный админ — встроенный 'admin'. */
function parseJwt(t) {
  try {
    let p = t.split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
    while (p.length % 4) p += '=';
    return JSON.parse(decodeURIComponent(atob(p).split('').map(c => '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2)).join('')));
  } catch (_) { return null; }
}
let currentUser = jwtToken ? parseJwt(jwtToken) : null;
/* Просроченный токен → сбрасываем сразу (иначе мигнёт «вошёл» и тут же 401). */
if (currentUser && currentUser.exp && currentUser.exp * 1000 < Date.now()) {
  jwtToken = null; currentUser = null; isLoggedIn = false;
  try { localStorage.removeItem('dfo_jwt'); } catch (_) {}
}
function userIsAdmin() { return !!currentUser && currentUser.sub === 'admin'; }

/* Раздел «Администрирование» доступен только пользователю admin — прячем пункт меню остальным. */
function applyRoleVisibility() {
  const adm = document.querySelector('.nav-item[data-view="admin"]');
  if (adm) adm.style.display = userIsAdmin() ? '' : 'none';
}

/* Pipeline builder */
const pb = { steps: [], editId: null, max_retries: 3, retry_delay_sec: 30, webhook_url: '', webhook_on: 'failure' };

/* ── Форма коннектора, общая для конструктора и раздела «Подключения» ─────────
 * makeConnectorConfigHTML и её обработчики адресуют шаг по индексу в pb.steps.
 * Чтобы ТУ ЖЕ САМУЮ форму (все 10 типов, селектор защиты Kafka, блоки TLS,
 * Avro-реестр, кнопки «Подключиться»/предпросмотра) показать в редакторе
 * подключения, вводим виртуальный индекс: CONN_FORM_IDX адресует не шаг
 * конвейера, а временный объект редактора. Разметка формы при этом не меняется
 * ни на строку. */
/* Заглушка секрета: сервер отдаёт её вместо непустого пароля. */
const CONN_SECRET_MASK = '********';

const CONN_FORM_IDX = -1;
let _connFormStep = null;

/* Единственная точка разыменования индекса шага для формы коннектора. */
function pbStep(idx) {
  return (idx === CONN_FORM_IDX) ? _connFormStep : pb.steps[idx];
}

/* User preferences */
let prefs = {};



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

const VALID_VIEWS = new Set(['pipelines','builder','analytics','matviews','connections','security','metrics','settings','admin']);
const ADMIN_TABS  = ['settings','security'];   /* grouped under «Администрирование» (порядок вкладок) */
let _adminTab = 'settings';                    /* какая вкладка открывается при входе */

function switchView(name, { pushState = true } = {}) {
  /* Администрирование (Настройки/Безопасность) — только для встроенного admin. */
  if ((name === 'admin' || name === 'settings' || name === 'security') && !userIsAdmin()) {
    name = 'pipelines';
  }
  if (name === 'admin') name = ADMIN_TABS[0];    /* клик по «Администрирование» → всегда первая вкладка (Настройки) */
  if (!VALID_VIEWS.has(name)) name = 'pipelines';
  const isAdmin = ADMIN_TABS.includes(name);
  if (isAdmin) _adminTab = name;

  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  const navEl = document.querySelector(`[data-view="${isAdmin ? 'admin' : name}"]`);
  if (navEl) navEl.classList.add('active');
  const viewEl = document.getElementById('view-' + name);
  if (viewEl) viewEl.classList.add('active');

  /* admin tab bar: visible only under Администрирование, active tab synced */
  const tb = document.getElementById('admin-tabbar');
  if (tb) {
    tb.style.display = isAdmin ? '' : 'none';
    tb.querySelectorAll('.ingest-tab').forEach(b =>
      b.classList.toggle('active', b.dataset.admintab === name));
  }

  /* hide builder nav item when not editing */
  const nb = document.getElementById('nav-builder');
  if (name !== 'builder') nb.classList.remove('visible');

  if (name === 'pipelines') loadPipelinesView();
  if (name === 'analytics') loadAnalyticsModule();
  if (name === 'matviews')  loadMatviews();
  if (name === 'connections') loadConnectionsView();
  if (name === 'security')  restoreSecTab();
  if (name === 'metrics')   loadMetrics();
  if (name === 'settings')  loadSettings();

  /* Запоминаем вид — чтобы перезагрузка (F5) оставляла на той же странице
   * даже если URL-хэш потерян. Билдер не сохраняем (нужен id конкретного конвейера). */
  if (name !== 'builder') { try { localStorage.setItem('dfo_view', name); } catch (_) {} }

  if (pushState && location.hash !== '#' + name)
    history.pushState({ view: name }, '', '#' + name);
}

/* Восстанавливает сохранённую под-вкладку раздела «Безопасность» (RBAC/Аудит). */
function restoreSecTab() {
  let st;
  try { st = localStorage.getItem('dfo_sec_tab'); } catch (_) {}
  switchSecTab(['users', 'rbac', 'audit', 'apikeys'].includes(st) ? st : 'users');
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
  if (ev === 'pipeline_created')     { loadPipelines({ silent: true }); }
  if (ev === 'pipeline_run_started') { loadPipelines({ silent: true }); }
  if (ev === 'pipeline_triggered')   { loadPipelines({ silent: true }); }
  if (ev === 'pipeline_done')        { loadPipelines({ silent: true }); }   /* завершение → обновить статус/историю */
}

function setBadge(cls, text) {
  const el = document.getElementById('ws-status');
  if (!el) return;   /* индикатор заменён кнопкой выхода — тихо пропускаем */
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
    let tables = await apiFetch('/api/tables');
    if (!tables) return;
    tables = tables.filter(t => !(t.name || '').startsWith('__'));   /* hide internal __dfo_* pipe tables */
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
      <button class="btn btn-sm btn-danger" title="Удалить таблицу">✕</button>
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
    step._lastSqlResult = data;            /* remember for the table-view modal */
    const status = document.createElement('div');
    status.style.cssText = 'font-size:.78rem;color:var(--muted);margin-bottom:.4rem';
    status.innerHTML = `▶ SQL — ${rows.length} ${pluralRows(rows.length)} · ${ms} мс · клик по таблице → окно запроса`;
    panel.appendChild(status);
    const wrap = document.createElement('div');
    wrap.style.cursor = 'pointer';
    wrap.title = 'Открыть в окне и сделать SQL-запрос по таблицам DFO';
    wrap.onclick = () => openSqlDataModal(idx);
    wrap.appendChild(makeResultTable(data));
    panel.appendChild(wrap);
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
    connection_id:      s.connection_id || '',
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


/* Универсальный рендер таблицы результата {columns, rows} → DOM-элемент.
   Строки принимает и массивом значений, и объектом {колонка: значение};
   NULL/undefined выводит как <span class="null-val">. Используется во всех превью. */
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
let _pipelinesPerPage = 10;   /* 10 блоков на странице по умолчанию (Infinity = «Все») */

/* Растягивает контейнер списка до нижней кромки панели (min-height по геометрии), чтобы
 * .pager с margin-top:auto всегда был внизу страницы — на ЛЮБОЙ форме, включая
 * многоэкранные (Аналитика/Безопасность): трогаем ТОЛЬКО сам список, а не раздел,
 * поэтому соседние формы не ломаются. Для списков карточек (cards!==false) заодно
 * подбираем высоту строки так, чтобы РОВНО 10 умещались без скролла; при переполнении
 * список растёт и прокручивается общий #main. */
function sizeListRows(listId, opts) {
  const sizeCards = !opts || opts.cards !== false;
  const list = document.getElementById(listId);
  if (!list || list.offsetParent === null) return;   /* нет в DOM или скрыт (display:none) */
  const main = document.getElementById('main');
  if (!main) return;
  const mainRect  = main.getBoundingClientRect();
  const padBottom = parseFloat(getComputedStyle(main).paddingBottom) || 0;
  /* Свободная высота от верха списка до нижней кромки панели (внутри её padding). */
  const avail = Math.floor((mainRect.bottom - padBottom) - list.getBoundingClientRect().top);
  if (avail <= 0) return;
  list.style.minHeight = avail + 'px';               /* прижать пагинатор к низу */
  if (sizeCards) {
    const pager  = list.querySelector('.pager');
    const pagerH = pager ? pager.offsetHeight + 12 : 0;
    const row    = list.querySelector('.pipeline-item');
    const rowMargin = row ? (parseFloat(getComputedStyle(row).marginBottom) || 0) : 6;
    const rowH = Math.max(44, Math.floor((avail - pagerH) / 10) - rowMargin);
    document.documentElement.style.setProperty('--pipe-row-h', rowH + 'px');
  }
}

/* Пересчёт для активной формы (ресайз/переключение). Скрытые отсеются по offsetParent,
 * видимый список выставит min-height (и высоту строк) под свою область. */
function sizeActiveList() {
  ['pipelines-list', 'metrics-pipelines-list', 'metrics-matviews-list', 'matviews-list', 'connections-list', 'an-saved-list']
    .forEach(id => sizeListRows(id));
  sizeListRows('audit-log-list', { cards: false });   /* аудит — таблица, высоту строк не трогаем */
}

/* Совместимость со старым именем вызова. */
function pipelinesSizeRows() { sizeListRows('pipelines-list'); }
let _pipelinesAll = [];
let _pipelinesPage = 0;

async function loadPipelines({ silent = false } = {}) {
  const list = document.getElementById('pipelines-list');
  /* silent — обновление на месте (без «Загрузка…» и сброса страницы):
     для запуска конвейера и WS-событий, чтобы не было мелькания/переброса. */
  if (!silent) list.innerHTML = '<div style="color:var(--muted);padding:.5rem">Загрузка…</div>';
  try {
    const pipelines = await apiFetch('/api/pipelines');
    if (!pipelines) return;
    if (!pipelines.length) {
      _pipelinesAll = [];
      list.innerHTML = `
        <div class="empty-state">
          Конвейеров пока нет.<br>
          <button class="btn btn-primary" style="margin-top:.75rem" onclick="openPipelineBuilder(null)">
            + Создать первый конвейер
          </button>
        </div>`;
      return;
    }
    _pipelinesAll  = pipelines;
    if (!silent) _pipelinesPage = 0;   /* тихое обновление сохраняет текущую страницу */
    renderPipelinesPage();
  } catch (err) {
    list.innerHTML = `<div style="color:var(--red)">Error: ${escHtml(String(err))}</div>`;
  }
}

/* Render the current page of pipelines + pager controls (page nav + page size). */
function renderPipelinesPage() {
  const list = document.getElementById('pipelines-list');
  if (!list) return;
  const total   = _pipelinesAll.length;
  const perPage = _pipelinesPerPage;
  const all     = perPage === Infinity;
  const pages   = all ? 1 : Math.max(1, Math.ceil(total / perPage));
  if (_pipelinesPage > pages - 1) _pipelinesPage = pages - 1;
  if (_pipelinesPage < 0) _pipelinesPage = 0;
  const start = all ? 0 : _pipelinesPage * perPage;
  const end   = all ? total : Math.min(start + perPage, total);
  const slice = all ? _pipelinesAll : _pipelinesAll.slice(start, end);
  /* Атомарная замена через фрагмент (без промежуточного innerHTML='',
     который схлопывает список и перебрасывает прокрутку наверх). */
  const frag = document.createDocumentFragment();
  slice.forEach(p => frag.appendChild(makePipelineRow(p)));
  if (total > 0) {
    const nav = document.createElement('div');
    nav.className = 'pager';
    const navBtns = pages > 1 ? `
      <button class="btn btn-sm" ${_pipelinesPage === 0 ? 'disabled' : ''} onclick="pipelinesGoPage(${_pipelinesPage - 1})">← Назад</button>
      <span class="pager-info">${start + 1}–${end} из ${total} · стр. ${_pipelinesPage + 1}/${pages}</span>
      <button class="btn btn-sm" ${_pipelinesPage >= pages - 1 ? 'disabled' : ''} onclick="pipelinesGoPage(${_pipelinesPage + 1})">Вперёд →</button>`
      : `<span class="pager-info">всего: ${total}</span>`;
    const opts = [10, 25, 50].map(n => `<option value="${n}" ${_pipelinesPerPage === n ? 'selected' : ''}>${n}</option>`).join('')
               + `<option value="all" ${all ? 'selected' : ''}>Все</option>`;
    nav.innerHTML = navBtns +
      `<label class="pager-size">На странице:
         <select onchange="pipelinesSetPerPage(this.value)">${opts}</select>
       </label>`;
    frag.appendChild(nav);
  }
  list.replaceChildren(frag);
  /* Зафиксировать высоту блока так, чтобы 10 блоков заполняли область. */
  pipelinesSizeRows();
}

function pipelinesGoPage(p) {
  _pipelinesPage = p;
  renderPipelinesPage();
  document.getElementById('pipelines-list').scrollIntoView({ block: 'start', behavior: 'smooth' });
}

function pipelinesSetPerPage(v) {
  _pipelinesPerPage = (v === 'all') ? Infinity : parseInt(v, 10);
  _pipelinesPage = 0;
  renderPipelinesPage();
}

/* Пересчитываем высоту блока (чтобы 10 заполняли область) при ресайзе окна. */
let _pipResizeT;
window.addEventListener('resize', () => {
  clearTimeout(_pipResizeT);
  _pipResizeT = setTimeout(sizeActiveList, 120);
});

const STATUS_LABELS = ['ожидание','выполняется','успех','ошибка','отменён'];
const STATUS_BADGE  = ['badge-warn','badge-run','badge-ok','badge-err','badge-warn'];

const _pipelineCache = {};
/* id конвейеров, по которым прямо сейчас идёт ручной прогон (клиентский индикатор,
   переживает ре-рендеры списка → карточка показывает «выполняется» до завершения). */
const _runningPipelines = new Set();

function makePipelineRow(p) {
  _pipelineCache[p.id] = p;
  const div    = document.createElement('div');
  div.className = 'pipeline-item pipeline-item-clickable';
  div.onclick = (e) => {
    if (e.target.closest('button')) return;
    openPipelineBuilder(_pipelineCache[p.id]);
  };
  const running = _runningPipelines.has(p.id);
  const status  = running ? 1 : (p.status || 0);   /* 1 = выполняется */
  const steps  = (p.steps || []).map(s =>
    `<span class="step-badge">${escHtml(s.name || s.id || 'step')}</span>`).join('');
  const nextRunStr = p.next_run && p.next_run > 0 && p.next_run < 9e15
    ? new Date(p.next_run * 1000).toLocaleString('ru') : '—';
  div.innerHTML = `
    <div style="flex:1;min-width:0">
      <div style="display:flex;align-items:center;gap:.5rem;flex-wrap:wrap">
        <span class="pipeline-name">${escHtml(p.name || p.id)}</span>
        <span class="badge ${STATUS_BADGE[status]}">${running ? 'выполняется' : STATUS_LABELS[status]}</span>
        ${!p.enabled ? '<span class="badge badge-warn">отключён</span>' : ''}
      </div>
      <div class="pipeline-meta">
        расписание: <code>${escHtml(p.cron || 'вручную')}</code>
        &nbsp;·&nbsp; последний запуск: ${p.last_run ? new Date(p.last_run * 1000).toLocaleString('ru') : '—'}
        &nbsp;·&nbsp; следующий: ${nextRunStr}
      </div>
      <div class="step-list">${steps}</div>
    </div>
    <div class="pipeline-actions">
      <button class="btn btn-sm btn-primary${running ? ' pulse' : ''}" onclick="triggerPipeline('${escAttr(p.id)}')">${running ? 'Выполняется…' : '▶ Запустить'}</button>
      <button class="btn btn-sm" onclick="showPipelineRuns('${escAttr(p.id)}','${escAttr(p.name||p.id)}')">История</button>
      <button class="btn btn-sm btn-danger" onclick="deletePipeline('${escAttr(p.id)}','${escAttr(p.name||p.id)}')">✕</button>
    </div>
  `;
  return div;
}

async function triggerPipeline(id) {
  if (_runningPipelines.has(id)) return;     /* уже идёт */
  _runningPipelines.add(id);
  renderPipelinesPage();                     /* мгновенно: карточка → «выполняется», кнопка disabled */
  showToast('Запуск конвейера…', 'info');
  logActivity(`запуск конвейера ${id}`);
  try {
    await apiPost(`/api/pipelines/${id}/run`, {});
    /* Бэкенд выполняет конвейер синхронно и всегда отвечает "triggered",
       поэтому реальный исход берём из последнего прогона. */
    let last = null;
    try {
      const runs = await apiFetch(`/api/pipelines/${id}/runs`);
      if (Array.isArray(runs) && runs.length) last = runs[0];
    } catch (_) {}
    if (last && last.status !== 0)
      showToast('Конвейер завершился с ошибкой: ' + (humanizeError(last.error) || 'см. историю'), 'error');
    else
      showToast('Конвейер выполнен', 'ok');
  } catch (err) {
    showToast('Не удалось запустить: ' + (humanizeError(String(err)) || String(err)), 'error');
  } finally {
    _runningPipelines.delete(id);
    loadPipelines({ silent: true });         /* снять индикатор + показать итоговый статус */
  }
}

async function deletePipeline(id, name) {
  if (!confirm(`Удалить конвейер "${name || id}"?`)) return;
  try {
    await apiFetch(`/api/pipelines/${id}`, 'DELETE');
    showToast(`Конвейер "${name || id}" удалён`, 'ok');
    loadPipelines({ silent: true });
  } catch (err) { showToast(String(err), 'error'); }
}

/* Перевод технической ошибки прогона в человекопонятный русский текст.
   Полный оригинал сохраняется в подсказке (title), здесь — короткая суть. */
/* Превращает «сырую» ошибку сервера/трейсбек в короткое человекочитаемое сообщение
   по ключевым словам (sink/python/sql/connection/timeout/…), при возможности
   добавляя номер шага. Если ничего не распознано — возвращает первую строку (обрезанную). */
function humanizeError(raw) {
  if (!raw) return '';
  const s = String(raw).trim();
  const low = s.toLowerCase();
  const stepM = s.match(/(\w+)\s+step\s+(s\d+)/i);
  const step  = stepM ? ` (шаг ${stepM[2]})` : '';
  let msg = null;
  if (low.includes('write_batch failed') || (low.includes('sink') && low.includes('fail')))
    msg = 'Не удалось записать данные в приёмник';
  else if (low.includes('traceback') || low.includes('python step')) {
    const k = s.match(/KeyError:\s*'([^']+)'/);
    const f = s.match(/(\w+Error):/);
    msg = k ? `Ошибка Python: нет столбца «${k[1]}»`
        : (f ? `Ошибка Python: ${f[1]}` : 'Ошибка в Python-шаге');
  }
  else if (low.includes('scala step')) msg = 'Ошибка в Scala-шаге';
  else if (low.includes('parse') || low.includes('syntax') || /near ['"]/.test(low))
    msg = 'Ошибка в SQL-запросе';
  else if (low.includes('connection refused') || low.includes('could not connect') || low.includes('connect failed'))
    msg = 'Не удалось подключиться к источнику/приёмнику';
  else if (low.includes('timeout') || low.includes('timed out'))
    msg = 'Превышено время ожидания (таймаут)';
  else if (low.includes('unauthorized') || low.includes('permission denied') || low.includes('password') || low.includes('access denied'))
    msg = 'Ошибка доступа/авторизации';
  else if (low.includes('not found') || low.includes('no such table') || low.includes('does not exist'))
    msg = 'Объект не найден (таблица/файл/ресурс)';
  else if (low.includes('exit='))
    msg = 'Шаг завершился с ошибкой';
  if (msg) return msg + step;
  /* запасной вариант — первая строка, обрезанная */
  const first = s.split('\n')[0];
  return first.length > 100 ? first.slice(0, 100) + '…' : first;
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
    /* per-pipeline metric summary */
    const okN  = list.filter(r => r.status === 0).length;
    const durs = list.filter(r => r.started && r.finished).map(r => r.finished - r.started);
    const avgD = durs.length ? durs.reduce((a, b) => a + b, 0) / durs.length : 0;
    const maxD = durs.length ? Math.max(...durs) : 0;
    const summary = `<div style="margin-bottom:.85rem">
      <div class="mstat-row"><span class="mstat-k">Запусков</span><span class="mstat-v">${list.length}</span></div>
      <div class="mstat-row"><span class="mstat-k">Успешных</span><span class="mstat-v ok">${okN}</span></div>
      <div class="mstat-row"><span class="mstat-k">Ошибок</span><span class="mstat-v ${list.length - okN ? 'err' : ''}">${list.length - okN}</span></div>
      <div class="mstat-row"><span class="mstat-k">Средняя длительность</span><span class="mstat-v">${avgD.toFixed(1)} с</span></div>
      <div class="mstat-row"><span class="mstat-k">Макс. длительность</span><span class="mstat-v">${maxD} с</span></div>
    </div>`;
    let html = '<table class="runs-table"><thead><tr><th>Начало</th><th>Статус</th><th>Длит.</th><th>Повторы</th><th>Ошибка</th></tr></thead><tbody>';
    list.forEach(r => {
      const statusLabel = r.status === 0 ? '<span class="badge badge-ok">успех</span>'
        : '<span class="badge badge-err">ошибка</span>';
      const dur = (r.started && r.finished) ? (r.finished - r.started) + ' с' : '—';
      html += `<tr>
        <td>${r.started ? new Date(r.started*1000).toLocaleString('ru') : '—'}</td>
        <td>${statusLabel}</td>
        <td>${dur}</td>
        <td>${typeof r.retry_count === 'number' ? r.retry_count : 0}</td>
        <td style="${r.error ? 'color:var(--red)' : ''}">${r.error ? `
          <div>${escHtml(humanizeError(r.error))}</div>
          <div style="margin-top:.25rem;font-family:monospace;font-size:.78rem;white-space:pre-wrap;word-break:break-word;opacity:.85">${escHtml(r.error)}</div>
        ` : ''}</td>
      </tr>`;
    });
    html += '</tbody></table>';
    body.innerHTML = summary + html;
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

/* Открывает конструктор конвейера: заполняет глобальное состояние pb и поля формы
   из объекта pipeline (или пустыми значениями для нового), рисует шаги/триггеры,
   восстанавливает свёрнутые секции и переключается на вид builder с URL
   #builder/<id> (или /new), чтобы контекст пережил F5. */
function openPipelineBuilder(pipeline) {
  pb.editId = pipeline ? (pipeline.id || null) : null;
  pb.steps  = pipeline ? (pipeline.steps || []).map(s => ({...s, deps: s.deps || []})) : [];
  pb.max_retries     = pipeline ? (pipeline.max_retries || 3) : 3;
  pb.retry_delay_sec = pipeline ? (pipeline.retry_delay_sec || 30) : 30;
  pb.webhook_url     = pipeline ? (pipeline.webhook_url || '') : '';
  pb.webhook_on      = pipeline ? (pipeline.webhook_on || 'failure') : 'failure';
  /* Event-driven triggers beyond the cron schedule (webhook / file_arrival).
   * cron lives in its own field; `manual` is a no-op (a pipeline is always
   * runnable manually), so both are filtered out here. */
  pb.triggers = pipeline && Array.isArray(pipeline.triggers)
    ? pipeline.triggers.filter(t => t.type !== 'cron' && t.type !== 'manual').map(t => ({...t}))
    : [];

  document.getElementById('pb-name').value      = pipeline ? (pipeline.name || '') : '';
  document.getElementById('pb-enabled').checked = pipeline ? !!pipeline.enabled : true;
  document.getElementById('pb-cron').value       = pipeline ? (pipeline.cron || '') : '';
  document.getElementById('pb-max-retries').value = pb.max_retries;
  document.getElementById('pb-retry-delay').value = pb.retry_delay_sec;
  document.getElementById('pb-webhook-url').value = pb.webhook_url;
  syncCronPreset();

  const title = document.getElementById('builder-title');
  if (title) title.textContent = pb.editId ? `Редактирование — ${pipeline.name || pb.editId}` : '';

  /* builder nav item stays hidden — the "Конструктор" label should not appear
     in the sidebar while configuring a pipeline/step. */

  renderBuilderSteps();
  renderTriggers();
  pbRestoreSections();   /* применить сохранённое сворачивание секций */

  /* Справочник подключений нужен селектору в карточке шага. Функция синхронная,
   * поэтому подтягиваем кэш вторым проходом и перерисовываем шаги — иначе при
   * заходе по прямой ссылке «#builder/<id>» список подключений будет пуст. */
  loadConnectionsCache().then(() => {
    if (document.getElementById('view-builder').classList.contains('active'))
      renderBuilderSteps();
  });

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
  /* Prefer the autosaved draft (survives F5, incl. an unsaved new pipeline)
     when it matches the current builder URL. */
  const draft = loadBuilderDraft();
  if (draft && draft.snap &&
      (draft.hash === location.hash || (draft.snap.id || 'new') === (id || 'new'))) {
    openPipelineBuilder(draft.snap);
    return;
  }
  if (!id || id === 'new') { switchView('pipelines', { pushState: false }); return; }
  try {
    const pipelines = await apiFetch('/api/pipelines');
    const p = (pipelines || []).find(x => x.id === id);
    if (p) { openPipelineBuilder(p); return; }
  } catch (_) { /* fall through to pipelines */ }
  switchView('pipelines', { pushState: false });
  history.replaceState({ view: 'pipelines' }, '', '#pipelines');
}

/* ── Builder draft autosave (so a page refresh keeps you in the builder) ── */
const BUILDER_DRAFT_KEY = 'dfo_builder_draft';

/* Pipeline-shaped snapshot of the current builder state (pb + form fields). */
function snapshotBuilder() {
  const v = (id, dflt) => { const el = document.getElementById(id); return el ? el.value : dflt; };
  const num = (id, dflt) => parseInt(v(id, dflt), 10) || dflt;
  const enabledEl = document.getElementById('pb-enabled');
  return {
    id:              pb.editId || null,
    name:            v('pb-name', ''),
    enabled:         enabledEl ? enabledEl.checked : true,
    cron:            v('pb-cron', ''),
    max_retries:     num('pb-max-retries', 3),
    retry_delay_sec: num('pb-retry-delay', 30),
    webhook_url:     v('pb-webhook-url', ''),
    webhook_on:      v('pb-webhook-on', 'failure'),
    /* Keep the RAW steps (UI flags like _dbok / _ui_type included) so the form
     * state — not just the API-shaped data — survives a refresh. Drop the bulky
     * _lastPreview result set to keep the draft small. */
    steps:           (pb.steps || []).map(s => { const { _lastPreview, ...rest } = s; return rest; }),
    triggers:        (pb.triggers || []).map(t => ({ ...t })),
  };
}
function saveBuilderDraft() {
  try { localStorage.setItem(BUILDER_DRAFT_KEY,
        JSON.stringify({ hash: location.hash, snap: snapshotBuilder() })); } catch (_) {}
}
function clearBuilderDraft() {
  try { localStorage.removeItem(BUILDER_DRAFT_KEY); } catch (_) {}
}
function loadBuilderDraft() {
  try { return JSON.parse(localStorage.getItem(BUILDER_DRAFT_KEY) || 'null'); } catch (_) { return null; }
}
/* Persist the draft on unload while the builder is the active view. `pagehide`
 * is more reliable than `beforeunload` (fires on mobile/bfcache); keep both. */
function saveDraftIfBuilderActive() {
  const bv = document.getElementById('view-builder');
  if (bv && bv.classList.contains('active')) saveBuilderDraft();
}
window.addEventListener('beforeunload', saveDraftIfBuilderActive);
window.addEventListener('pagehide', saveDraftIfBuilderActive);

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

/* Рисует список событийных триггеров конвейера (webhook / появление файла).
   Для webhook показывает готовый URL с секретным токеном, для file_arrival —
   папку и маску файлов (только Linux). cron задаётся отдельным полем, не здесь. */
function renderTriggers() {
  const el = document.getElementById('pb-triggers');
  if (!el) return;
  if (!pb.triggers || pb.triggers.length === 0) {
    el.innerHTML = '';
    return;
  }
  el.innerHTML = pb.triggers.map((t, i) => {
    let body = '';
    const CTL = 'font-size:.78rem;height:32px;box-sizing:border-box';   /* uniform field height */
    if (t.type === 'webhook') {
      const port = location.port || '8080';
      const url  = `${location.protocol}//${location.hostname}:${port}/api/triggers/${escAttr(t.webhook_token || '')}`;
      body = `
        <input type="text" placeholder="секретный токен"
               value="${escAttr(t.webhook_token || '')}"
               oninput="pbUpdateTrigger(${i},'webhook_token',this.value);renderTriggers()"
               style="flex:1;font-family:var(--mono);${CTL}">
        <code style="font-size:.7rem;color:var(--muted);flex:2;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
              title="${escAttr(url)}">${escHtml(url)}</code>`;
    } else if (t.type === 'file_arrival') {
      body = `
        <input type="text" placeholder="папка для наблюдения"
               value="${escAttr(t.watch_dir || '')}"
               oninput="pbUpdateTrigger(${i},'watch_dir',this.value)"
               style="flex:2;font-family:var(--mono);${CTL}">
        <input type="text" placeholder="маска: *.csv"
               value="${escAttr(t.file_pattern || '*')}"
               oninput="pbUpdateTrigger(${i},'file_pattern',this.value)"
               style="flex:1;font-family:var(--mono);${CTL}">
        <span style="font-size:.7rem;color:var(--muted)">только Linux</span>`;
    }
    return `<div style="display:flex;gap:.4rem;align-items:center;margin-bottom:.4rem">
      <select onchange="pbChangeTriggerType(${i},this.value)" style="width:170px;${CTL}">
        <option value="webhook"      ${t.type==='webhook'      ?'selected':''}>По внешнему HTTP-запросу (webhook)</option>
        <option value="file_arrival" ${t.type==='file_arrival' ?'selected':''}>При появлении файла в папке</option>
      </select>
      ${body}
      <button class="btn btn-sm btn-danger" onclick="pbRemoveTrigger(${i})" title="Удалить триггер">✕</button>
    </div>`;
  }).join('');
}



function syncCronPreset() {
  updateCronHuman(document.getElementById('pb-cron').value.trim());
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
    connector_type: 'postgresql',   /* по умолчанию новый шаг — Источник */
    connector_config: '{}',
    connection_id: '',              /* пусто = параметры доступа заданы в самом шаге */
    transform_sql: '',
    target_table: '',
    deps: [],
    max_retries: 3,
    retry_delay_sec: 30,
  });
  renderBuilderSteps();
  if (typeof saveBuilderDraft === 'function') saveBuilderDraft();
  /* scroll to the freshly-opened next-step card */
  const last = document.getElementById('pb-steps').lastElementChild;
  if (last) last.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

/* Свернуть/развернуть секцию конструктора (Граф шагов / Общие настройки).
   Состояние сохраняется в localStorage по id секции → переживает перезагрузку. */
function pbToggleSection(head) {
  const sec = head.closest('.pb-section');
  if (!sec) return;
  const collapsed = sec.classList.toggle('collapsed');
  if (sec.id) try {
    const m = JSON.parse(localStorage.getItem('dfo_pb_sections') || '{}');
    m[sec.id] = collapsed;
    localStorage.setItem('dfo_pb_sections', JSON.stringify(m));
  } catch (_) {}
}

/* Применить сохранённое состояние секций (вызывается при открытии конструктора). */
function pbRestoreSections() {
  let m = {};
  try { m = JSON.parse(localStorage.getItem('dfo_pb_sections') || '{}'); } catch (_) {}
  Object.entries(m).forEach(([id, collapsed]) => {
    const sec = document.getElementById(id);
    if (sec) sec.classList.toggle('collapsed', !!collapsed);
  });
}

/* Свернуть/развернуть карточку шага. Состояние в _collapsed попадает в черновик
   (snapshotBuilder хранит сырые шаги) → переживает перезагрузку. */
function pbToggleStep(idx) {
  const card = document.getElementById(`step-card-${idx}`);
  if (!card) return;
  const collapsed = card.classList.toggle('collapsed');
  if (pb.steps[idx]) pb.steps[idx]._collapsed = collapsed;
  saveBuilderDraft();
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

function pbUpdateStep(idx, field, val) {
  pb.steps[idx][field] = val;
  if (typeof saveBuilderDraft === 'function') saveBuilderDraft();
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
  const cfg = safeParse(pbStep(idx).connector_config, {});
  cfg[field] = val;
  pbStep(idx).connector_config = JSON.stringify(cfg);
  if (typeof saveBuilderDraft === 'function') saveBuilderDraft();
}


/* ── Запрос к данным против параметров доступа ────────────────────────────────
 * Подключение — это только «как попасть в систему»: адрес, учётные данные, TLS.
 * ЧТО именно читать — свойство конкретного конвейера, поэтому запрос живёт в
 * шаге. Ниже перечислены ключи запроса по типам; всё остальное в форме
 * коннектора считается параметрами доступа.
 *
 * Таблица одна, а применяется зеркально: в карточке шага прячем доступ, в
 * редакторе подключения — запрос. Благодаря этому обе формы остаются той же
 * самой makeConnectorConfigHTML и не расходятся между собой. */
const CONN_REQUEST_KEYS = {
  postgresql: ['read_mode','table','query','cursor_column','cdc_slot','primary_key'],
  greenplum:  ['read_mode','table','query','cursor_column','primary_key'],
  oracle:     ['read_mode','table','query','cursor_column','primary_key'],
  kafka:      ['topic','group_id','offset_reset','offset_start_mode','isolation_level','data_format'],
  json_http:  ['data_path','post_body','page_param','page_type','page_size','total_field'],
  xml:        ['row_tag','root_tag','data_path','post_body','page_param','page_size'],
  soap:       ['soap_action','operation','request_template','row_tag','sink_row_tag','data_path','page_size'],
  siebel:     ['io_name','eim_object','data_path','read_body','page_param','size_param','page_size'],
  csv:        [],   /* сам файл и есть источник — запрашивать нечего */
  parquet:    [],
};


/* Ключи, видимые в ОБЕИХ формах. Такой ключ одновременно является настройкой
 * шага и переключателем, раскрывающим поля другой половины. Спрятать его в
 * одной из форм нельзя: либо теряется настройка, либо становится недоступным
 * то, что он раскрывает.
 *
 * data_format: формат сообщений задаётся по топику (на одном кластере бывают и
 * JSON-, и Avro-топики), поэтому нужен в шаге; он же раскрывает блок Schema
 * Registry, который относится к доступу и настраивается в подключении.
 * Значение из подключения работает как значение по умолчанию, шаг его
 * переопределяет — ровно как остальной конфиг. */
const CONN_SHARED_KEYS = {
  kafka: ['data_format'],
};

/* Обработчики, правящие конфиг не через pbUpdateConnConfig. */
const CONN_HANDLER_KEYS = {
  pbSetKafkaSecurity: 'security_protocol',
  pbSetKafkaOffsetStartMode: 'offset_start_mode',
};

/* Прячет в отрисованной форме половину полей. mode='request' оставляет только
 * запрос (карточка шага), mode='access' — только параметры доступа (редактор
 * подключения). Работает по готовой разметке, поэтому саму форму править не
 * пришлось. */
function applyConnFieldFilter(containerId, type, mode) {
  const el = document.getElementById(containerId);
  if (!el) return;
  const req = CONN_REQUEST_KEYS[type];
  if (!req) return;                       /* незнакомый тип — показываем всё */

  el.querySelectorAll('[oninput],[onchange],[onclick]').forEach(node => {
    const h = (node.getAttribute('oninput') || '') + ';' +
              (node.getAttribute('onchange') || '') + ';' +
              (node.getAttribute('onclick') || '');
    let key = null;
    const m = h.match(/pbUpdateConnConfig\(\s*-?\d+\s*,\s*'([a-zA-Z_0-9]+)'/);
    if (m) key = m[1];
    else for (const fn in CONN_HANDLER_KEYS)
      if (h.indexOf(fn + '(') >= 0) { key = CONN_HANDLER_KEYS[fn]; break; }
    if (!key) return;
    if ((CONN_SHARED_KEYS[type] || []).indexOf(key) >= 0) return;   /* виден везде */
    const isRequest = req.indexOf(key) >= 0;
    if (mode === 'request' ? !isRequest : isRequest) {
      const box = node.closest('.form-group') || node.parentElement;
      if (box) box.style.display = 'none';
    }
  });

  /* Кнопки, относящиеся к другой половине: «Подключиться» — это про доступ,
   * предпросмотр — про запрос. Каждая остаётся только на своей форме. */
  const foreignBtn = mode === 'request'
    ? 'pbTestConnection('
    : null;   /* в редакторе подключения предпросмотр оставляем: им удобно
                 проверить, что креды верные и данные вообще читаются */
  if (foreignBtn) {
    el.querySelectorAll('button[onclick]').forEach(bt => {
      if (bt.getAttribute('onclick').indexOf(foreignBtn) >= 0) {
        const box = bt.closest('.form-group') || bt.parentElement;
        if (box) box.style.display = 'none';
      }
    });
  }

  /* Заголовок секции («Подключение к Oracle») — часть блока параметров доступа. */
  if (mode === 'request')
    el.querySelectorAll('.conn-group-title').forEach(t => { t.style.display = 'none'; });

  /* Схлопнуть контейнеры без единого видимого поля, чтобы не зияли пустоты. */
  el.querySelectorAll('div').forEach(d => {
    const groups = d.querySelectorAll('.form-group');
    if (!groups.length) return;
    let visible = false;
    groups.forEach(g => { if (g.style.display !== 'none') visible = true; });
    if (!visible && !d.querySelector('button:not([style*="display:none"]), textarea')) d.style.display = 'none';
  });
}

/* Конфиг для ОТРИСОВКИ формы в карточке шага: база из подключения, поверх —
 * ключи шага. Без слияния поля, которые форма раскрывает по переключателю из
 * подключения (например «Тело запроса» появляется только при method=POST),
 * в шаге не рисовались бы никогда: у шага этого переключателя нет.
 * Правки по-прежнему уходят в конфиг ШАГА — pbUpdateConnConfig не меняется. */
function stepRenderView(step) {
  if (!step || !step.connection_id) return step;
  const rec = (connCache || []).find(c => c.id === step.connection_id);
  if (!rec) return step;
  const merged = Object.assign({}, rec.config || {}, safeParse(step.connector_config, {}));
  return Object.assign({}, step, { connector_config: JSON.stringify(merged) });
}

/* Единая точка перерисовки формы запроса в карточке шага. */
function pbRerenderStepCfg(idx) {
  /* Редактор подключения собирается только своей функцией — иначе после
   * переключателя пропадали поля варианта приёмника и секция умолчаний. */
  if (idx === CONN_FORM_IDX) { renderConnEditorBody(); return; }
  const el = document.getElementById('step-conn-cfg-' + idx);
  if (!el) return;
  const step = pbStep(idx);
  el.innerHTML = makeConnectorConfigHTML(stepRenderView(step), idx);
  /* В карточке шага фильтр обязателен и после частичной перерисовки, иначе
   * туда всплывут параметры доступа. */
  applyConnFieldFilter('step-conn-cfg-' + idx, stepConnType(step), 'request');
}

/* Привязать шаг к подключению из справочника (или отвязать при пустом id). */
function pbSetStepConnection(idx, connId) {
  const s = pb.steps[idx];
  s.connection_id = connId || '';
  if (connId) {
    const rec = connCache.find(c => c.id === connId);
    /* connector_type обязателен: stepType() определяет вид шага по нему, и без
     * него карточка перестанет считаться источником/приёмником. */
    if (rec && rec.type) s.connector_type = rec.type;
    /* Из шага убираем параметры доступа (их даёт подключение), но СОХРАНЯЕМ
     * запрос: он принадлежит этому конвейеру и при смене подключения не теряется. */
    const req = CONN_REQUEST_KEYS[rec ? rec.type : s.connector_type] || [];
    const cfg = safeParse(s.connector_config, {});
    const kept = {};
    req.forEach(k => { if (cfg[k] !== undefined) kept[k] = cfg[k]; });
    s.connector_config = JSON.stringify(kept);
  }
  s._dbok = false;             /* прежняя отметка «✓ Подключено» больше не про этот адрес */
  renderBuilderSteps();
  if (typeof saveBuilderDraft === 'function') saveBuilderDraft();
}

/* Селектор подключения в карточке шага. dir — 'source' или 'sink': список
 * фильтруется по НАПРАВЛЕНИЮ ЗАПИСИ справочника, а не по типу коннектора
 * (все плагины умеют и читать, и писать, поэтому по типу отфильтровать нельзя). */
/* Убирает из конфига значения-заглушки секретов. Отправлять их обратно нельзя:
 * на сервере они легли бы ПОВЕРХ настоящего пароля, и коннектор получил бы
 * строку "********". Сервер такие значения тоже отбрасывает, но чинить надо с
 * обеих сторон — иначе проба связи для сохранённого подключения всегда врёт. */
function stripMaskedSecrets(cfg) {
  const out = {};
  Object.keys(cfg || {}).forEach(k => { if (cfg[k] !== CONN_SECRET_MASK) out[k] = cfg[k]; });
  return out;
}

function makeConnectionPickerHTML(step, idx, dir) {
  /* Подключение не привязано к роли шага: это просто система, куда мы ходим.
   * Показываем весь справочник и для источника, и для приёмника. */
  const list = (connCache || []);
  const cur  = step.connection_id || '';
  const missing = cur && !list.some(c => c.id === cur);
  const rec = list.find(c => c.id === cur) || null;
  /* Старый шаг: параметры заданы внутри него, подключение не выбрано. Форму мы
   * из конвейера убрали, поэтому предлагаем перенести их в справочник. */
  const inlineCfg = safeParse(step.connector_config, {});
  const hasInline = !cur && Object.keys(inlineCfg).length > 0;
  /* Живое состояние постоянной сессии — видно прямо в шаге, что источник на связи. */
  const st = rec ? (rec.status || 'idle') : null;
  const dot = st === 'ok' ? '<span class="badge badge-ok">● на связи</span>'
            : st === 'error' ? '<span class="badge badge-err">● нет связи</span>'
            : (rec ? '<span class="badge">○ подключается…</span>' : '');
  const opts = [`<option value="">— выберите подключение —</option>`]
    .concat(list.map(c =>
      `<option value="${escAttr(c.id)}" ${c.id === cur ? 'selected' : ''}>${escHtml(c.name)} · ${escHtml(CONN_TYPE_LABELS[c.type] || c.type)}</option>`))
    .concat(missing ? [`<option value="${escAttr(cur)}" selected>подключение удалено (${escHtml(cur)})</option>`] : [])
    .join('');
  return `
    <div class="builder-settings-row" style="margin-bottom:.5rem">
      <div class="bsr-field" style="flex:1 1 60%;max-width:none">
        <label>Подключение <span class="label-hint" style="text-transform:none;font-weight:400">${dir === 'sink' ? 'система-приёмник' : 'система-источник'}</span></label>
        <select onchange="pbSetStepConnection(${idx}, this.value)">${opts}</select>
      </div>
      <div class="bsr-field" style="flex:0 0 auto;justify-content:flex-end">
        <label>&nbsp;</label>
        <div style="display:flex;gap:.35rem;align-items:center">
          ${dot}
          ${cur && !missing
            ? `<button class="btn btn-sm" onclick="pbOpenConnection(${idx})" title="Открыть подключение в разделе «Подключения»">✎ Редактировать</button>`
            : (hasInline
              ? `<button class="btn btn-sm btn-primary" onclick="pbConnectionFromStep(${idx}, '${dir}')" title="Перенести параметры, заданные внутри шага, в справочник подключений">↗ Перенести в подключения</button>`
              : `<button class="btn btn-sm" onclick="pbCreateConnectionFor(${idx}, '${dir}')" title="Создать подключение и привязать к шагу">＋ Создать подключение</button>`)}
        </div>
      </div>
    </div>
    ${missing ? `<div style="color:var(--red);font-size:.75rem;margin:-.25rem 0 .5rem">Подключение больше не существует — выберите другое, иначе шаг не запустится.</div>` : ''}
    ${hasInline ? `<div style="font-size:.74rem;color:var(--muted);border-left:2px solid var(--border);padding:.3rem .5rem;margin:-.15rem 0 .5rem">
        Старый формат: параметры доступа (${escHtml(CONN_TYPE_LABELS[step.connector_type] || step.connector_type)}) заданы внутри шага.
        Конвейер работает, но настроить их здесь больше нельзя — перенесите в подключения.
      </div>` : ''}
    ${/* Предпросмотр остаётся в конвейере: видно, какие данные реально придут из
          выбранной системы. Читает по эффективному конфигу — параметры берутся
          из подключения, секреты подставляет сервер по connection_id. */
      (cur || hasInline) ? `
    <div class="conn-actions" style="display:flex;margin:-.15rem 0 .35rem">
      <button type="button" class="btn btn-primary btn-sm" style="min-width:200px;justify-content:center"
              onclick="event.stopPropagation();${stepConnType(step) === 'kafka' && !step.is_sink ? `pbPreviewKafka(${idx})` : `pbPreviewSource(${idx})`}"
              title="${dir === 'sink' ? 'Показать, что сейчас лежит в приёмнике' : 'Прочитать пробную порцию данных из источника'}">${
                dir === 'sink' ? '▶ Что сейчас в приёмнике'
                : (stepConnType(step) === 'kafka' ? '▶ Показать сообщения' : '▶ Показать данные')}</button>
    </div>
    ${/* Свой контейнер вывода с ОТДЕЛЬНЫМ id. Одноимённый с формой давал бы два
         элемента с одинаковым id, и getElementById возвращал бы первый попавшийся —
         результат уходил бы не в тот блок. При этом форма рисует свой контейнер не
         для всех типов (у CSV его нет), поэтому просто убрать этот нельзя. */''}
    <div id="pb-steppreview-${idx}"></div>` : ''}`;
}

/* Переход из шага к редактированию его подключения. Возврат в конструктор — по
 * пункту меню «Конструктор»; состояние конвейера живёт в pb и не теряется. */
function pbOpenConnection(idx) {
  const id = pbStep(idx).connection_id;
  if (!id) return;
  openConnectionEditor(id, null, async () => {
    await loadConnectionsCache();
    switchView('builder');
    renderBuilderSteps();
  });
}

/* Перенос старого шага в справочник: открываем редактор подключения, заранее
 * заполненный параметрами из самого шага, и по сохранении привязываем шаг к
 * созданному подключению (pbSetStepConnection очистит инлайновый конфиг). */
function pbConnectionFromStep(idx, dir) {
  const s = pbStep(idx);
  const type = s.connector_type || 'postgresql';
  const cfg  = s.connector_config || '{}';
  const name = (s.name && s.name.trim())
    ? s.name.trim()
    : `${CONN_TYPE_LABELS[type] || type} — из шага ${idx + 1}`;

  openConnectionEditor(null, dir, async (newId) => {
    await loadConnectionsCache();
    switchView('builder');
    pbSetStepConnection(idx, newId);
    showToast('Параметры перенесены в подключение', 'ok');
  });
  /* openConnectionEditor уже создал пустой _connFormStep — подменяем его
   * параметрами шага и перерисовываем форму. */
  _connFormStep.connector_type   = type;
  _connFormStep.connector_config = cfg;
  document.getElementById('conn-name').value = name;
  renderConnEditorForm();
}

/* «＋ Создать» из карточки шага: открыть редактор и по сохранении привязать. */
function pbCreateConnectionFor(idx, dir) {
  const back = pb.editId;
  openConnectionEditor(null, dir, async (newId) => {
    await loadConnectionsCache();
    switchView('builder');
    if (back !== undefined) { /* конструктор уже держит состояние в pb */ }
    pbSetStepConnection(idx, newId);
  });
}

/* Смена «Защиты соединения» в форме Kafka. При выборе SASL-протокола сразу
 * фиксируем механизм PLAIN, если он ещё не задан: иначе бэкенд получит пустой
 * sasl.mechanism, а librdkafka по умолчанию уходит в GSSAPI (Kerberos) и падает
 * на отсутствующем keytab. Затем перерисовываем форму (появятся поля SASL/TLS). */
function pbSetKafkaSecurity(idx, val) {
  pbUpdateConnConfig(idx, 'security_protocol', val);
  if (val.indexOf('SASL') === 0) {
    const cfg = safeParse(pbStep(idx).connector_config, {});
    if (!cfg.sasl_mechanism) pbUpdateConnConfig(idx, 'sasl_mechanism', 'PLAIN');
  }
  const el = document.getElementById('step-conn-cfg-' + idx);
  if (el) pbRerenderStepCfg(idx);
}

/* Смена «Стартовой позиции». 'numeric' — не литеральное значение бэкенда, а
 * UI-состояние: реальный offset_start_mode должен быть конкретным числом
 * (строкой), поэтому сразу подставляем "0" и перерисовываем форму, чтобы
 * появилось поле ввода точного offset. */
function pbSetKafkaOffsetStartMode(idx, val) {
  pbUpdateConnConfig(idx, 'offset_start_mode', val === 'numeric' ? '0' : val);
  const el = document.getElementById('step-conn-cfg-' + idx);
  if (el) pbRerenderStepCfg(idx);
}

/* Preflight-проверка конфига Kafka до обращения к брокеру: ловим очевидные
 * пропуски (адрес, парная mTLS, логин/пароль SASL) и отдаём понятную причину
 * с указанием поля, а не сырую ошибку librdkafka после таймаута. Возвращает
 * { block:[...], warn:[...] }: block — жёсткие ошибки (не пускаем), warn —
 * предупреждения (пускаем, но подсвечиваем риск). */
function kafkaCfgProblems(cfg) {
  const block = [], warn = [];
  const sec = cfg.security_protocol || '';
  const mech = cfg.sasl_mechanism || 'PLAIN';
  const needsTls  = sec === 'SSL' || sec === 'SASL_SSL';
  const needsSasl = sec.indexOf('SASL') === 0;
  if (!(cfg.brokers || '').trim()) block.push('Укажите адрес брокеров (host:port).');
  if (needsSasl && mech !== 'GSSAPI') {
    if (!(cfg.sasl_username || '').trim()) block.push('Механизм ' + mech + ' требует поле «Пользователь».');
    if (!(cfg.sasl_password || '').trim()) block.push('Механизм ' + mech + ' требует поле «Пароль».');
  }
  if (needsTls) {
    const cert     = (cfg.ssl_certificate_location || '').trim();
    const keystore = (cfg.ssl_keystore_location || '').trim();
    const hasKey   = (cfg.ssl_key_location || '').trim() || keystore;
    const looksP12 = /\.(p12|pfx)$/i.test(cert) || keystore;
    /* НЕ блокируем: коннектор определяет PKCS#12 по содержимому файла, а не по
     * расширению — валидный .p12 без расширения не должен запрещать подключение.
     * Только мягко предупреждаем. */
    if (cert && !hasKey && !looksP12)
      warn.push('Клиентский сертификат указан без ключа: если это не PKCS#12-контейнер (cert+key одним файлом), заполните «Клиентский ключ».');
    if (!(cfg.ssl_ca_location || '').trim() && !(cfg.ssl_truststore_location || '').trim())
      warn.push('CA‑сертификат не задан — проверка брокера пойдёт по системному хранилищу (частный банковский CA обычно нужно указать явно).');
  }
  return { block, warn };
}

/* Перевод сырой ошибки librdkafka/коннектора в понятную причину с привязкой к
 * полю формы. Возвращает подсказку; исходный текст добавляем в скобках. */
function kafkaFriendlyError(err) {
  const e = String(err || '');
  const m = [
    [/unknown topic|does not exist|UNKNOWN_TOPIC/i,                         'Топик не найден на брокере — проверьте имя топика.'],
    /* ВЫШЕ общего правила про сертификат брокера: если truststore не открылся
     * (обычно неверный пароль), плагин отдаёт исходный .p12 в ssl.ca.location, и
     * OpenSSL сообщает про «no certificate», что уводит не туда. */
    [/ssl\.ca\.location failed|no certificate or crl found/i,
                                                                            'Не удалось прочитать доверенные сертификаты — проверьте «Пароль truststore» и путь к файлу (.p12/.pfx или PEM).'],
    [/certificate verify|verify failed|broker certificate|SSL handshake|ssl handshake|CERTIFICATE_VERIFY|unable to get local issuer|self.signed/i,
                                                                            'Сертификат брокера не прошёл проверку — проверьте «CA‑сертификат» и соответствие имени хоста в сертификате.'],
    [/PKCS12|keystore|private key|bad decrypt|key values mismatch|ssl.key/i,'Проблема с клиентским сертификатом/ключом — проверьте пути и «Пароль ключа / keystore».'],
    /* Kerberos/GSSAPI — ВЫШЕ общего SASL: текст ошибок GSSAPI часто содержит
     * «sasl» (напр. "sasl_cyrus: GSSAPI Error"), иначе маршрутизируется в
     * подсказку про логин/пароль, которых для GSSAPI нет. */
    [/Kerberos|GSSAPI|krb5|keytab|kinit|gss/i,                              'Ошибка Kerberos/GSSAPI — проверьте krb5.conf и keytab/ccache на сервере gateway.'],
    /* Authorization (ACL) — ВЫШЕ общей SASL-аутентификации: креды верны, залогинились
     * успешно, но брокер не даёт READ на конкретный топик/группу. Другая причина, другое
     * лечение (ACL у администратора кластера, не пароль/механизм). */
    [/not authorized|AUTHORIZATION_FAILED|TOPIC_AUTHORIZATION|GROUP_AUTHORIZATION|CLUSTER_AUTHORIZATION/i,
                                                                            'Учётные данные верны, но нет прав (ACL) на этот топик/группу — обратитесь к администратору кластера Kafka.'],
    [/authentication failed|SASL|sasl|Authentication|SaslAuthentication|invalid credentials/i,
                                                                            'Аутентификация не прошла — проверьте механизм SASL, «Пользователь» и «Пароль».'],
    [/resolve|nodename|Name or service|getaddrinfo/i,                       'Не удалось разрешить адрес брокера — проверьте host в поле «Брокеры».'],
    [/refused|Disconnected|brokers are down|transport failure|timed out|Connection setup timeout|all broker connections/i,
                                                                            'Брокер недоступен — проверьте адрес/порт «Брокеры», сетевой доступ и протокол.'],
  ];
  for (const [re, hint] of m) if (re.test(e)) return e ? `${hint} (${e})` : hint;
  return e || 'неизвестная причина';
}

/* Toggle the DB connection for a step. First click tests + (on success) turns
 * the button green and remembers it (step._dbok, survives re-render). Clicking
 * the green button "disconnects" — resets the state. */
async function pbTestConnection(idx) {
  const btn = document.getElementById(`pb-dbtest-${idx}`);
  if (!btn) return;
  const step = pbStep(idx);
  const set = (bg, fg, txt) => {
    btn.style.background = bg; btn.style.borderColor = bg; btn.style.color = fg; btn.textContent = txt;
  };

  /* already connected → disconnect (toggle off) */
  if (step._dbok) {
    step._dbok = false;
    set('', '', 'Подключиться');
    showToast('Соединение разорвано', 'ok');
    return;
  }

  const cfg = safeParse(step.connector_config, {});
  /* Kafka: preflight — очевидные пропуски ловим до похода к брокеру. */
  if (step.connector_type === 'kafka') {
    const pr = kafkaCfgProblems(cfg);
    if (pr.block.length) { showToast(pr.block.join(' '), 'error'); return; }
    if (pr.warn.length)  showToast(pr.warn.join(' '), 'warn');
  }
  const friendly = (r, e) => step.connector_type === 'kafka'
    ? kafkaFriendlyError((r && r.error) || e) : ((r && r.error) || e || 'неизвестная причина');
  set('', '', 'Проверка…'); btn.disabled = true;
  try {
    /* connection_id — чтобы сервер подставил настоящие секреты: UI их не видит. */
    const r = await apiPost('/api/connector/probe/ping',
      { type: stepConnType(step), config: stripMaskedSecrets(cfg),
        connection_id: step.connection_id || '' });
    if (r && r.ok) {
      step._dbok = true;
      set('var(--green)', '#fff', '✓ Подключено');
    } else {
      step._dbok = false;
      set('', '', 'Подключиться');   /* the toast carries the reason */
      showToast(`Не удалось подключиться: ${friendly(r)}`, 'error');
    }
  } catch (e) {
    step._dbok = false;
    set('', '', 'Подключиться');
    showToast(`Не удалось подключиться: ${friendly(null, e)}`, 'error');
  } finally { btn.disabled = false; }
}

/* Run the source table/SQL against the DB and show the rows inline below. */
/* Preview recent messages from a Kafka topic. Reads from the start under a
 * throwaway consumer group so it doesn't disturb the pipeline's real group. */
async function pbPreviewKafka(idx) {
  const step = pbStep(idx);
  const out  = document.getElementById(`pb-steppreview-${idx}`)
            || document.getElementById(`pb-dbpreview-${idx}`);
  if (!out) return;
  /* Как и в pbPreviewSource: база из подключения + оверлей шага. */
  const stepCfg = safeParse(step.connector_config, {});
  const linked  = step.connection_id
    ? (connCache || []).find(c => c.id === step.connection_id) : null;
  const cfg = Object.assign({}, linked ? (linked.config || {}) : {}, stepCfg);
  if (!cfg.topic) {
    out.innerHTML = '<div style="padding:.4rem;color:var(--amber);font-size:.8rem">Укажи топик в подключении</div>';
    return;
  }
  /* preflight: понятные пропуски до чтения (адрес, mTLS-пара, логин/пароль SASL) */
  const pr = kafkaCfgProblems(cfg);
  if (pr.block.length) {
    out.innerHTML = `<div style="padding:.4rem;color:var(--red);font-size:.8rem">${escHtml(pr.block.join(' '))}</div>`;
    return;
  }
  /* fresh group each time → always reads from the beginning, no offset commits
     to the real consumer group */
  const previewCfg = { ...cfg, group_id: `dfo_preview_${idx}_${Date.now()}`, offset_reset: 'earliest' };
  out.innerHTML = '<div style="padding:.4rem;color:var(--muted);font-size:.8rem">Читаю сообщения…</div>';
  try {
    const r = await apiPost('/api/connector/probe/preview',
      { type: 'kafka', config: stripMaskedSecrets(previewCfg), query: cfg.topic, limit: 50,
        connection_id: step.connection_id || '' });
    if (!r) { out.innerHTML = '<div style="padding:.4rem;color:var(--red);font-size:.8rem">Пустой ответ</div>'; return; }
    if (r.error) {
      out.innerHTML = `<div style="padding:.4rem;color:var(--red);font-size:.8rem;white-space:pre-wrap">${escHtml(kafkaFriendlyError(r.error))}</div>`;
      return;
    }
    out.innerHTML = '';
    const status = document.createElement('div');
    status.style.cssText = 'font-size:.78rem;color:var(--muted);margin-bottom:.35rem';
    const n = (r.rows || []).length;
    status.textContent = n ? `${n} ${pluralRows(n)} из топика «${cfg.topic}»` : `В топике «${cfg.topic}» пока нет сообщений`;
    out.appendChild(status);
    if (n) out.appendChild(makeResultTable(r));
  } catch (e) {
    out.innerHTML = `<div style="padding:.4rem;color:var(--red);font-size:.8rem">${escHtml(String(e))}</div>`;
  }
}

/* Показывает первые строки источника шага под его карточкой через
   /api/connector/probe/preview. Файловые/HTTP-источники берут адрес из конфига,
   БД-источники — таблицу/SQL. Результат запоминается в step._lastPreview (для
   модалки-обозревателя); по клику на таблицу БД открывается SQL-модалка. */
async function pbPreviewSource(idx) {
  const step = pbStep(idx);
  const out  = document.getElementById(`pb-steppreview-${idx}`)
            || document.getElementById(`pb-dbpreview-${idx}`);
  if (!out) return;
  /* Эффективный конфиг: база из подключения, поверх — инлайновые ключи шага.
   * Ровно так же слияние делает сервер при запуске (conn_merge_config), поэтому
   * предпросмотр показывает то же, что увидит конвейер. Секреты в конфиге
   * подключения замаскированы, но адрес/путь/запрос — нет, а настоящие пароли
   * сервер подставит сам по connection_id. */
  const stepCfg = safeParse(step.connector_config, {});
  const linked  = step.connection_id
    ? (connCache || []).find(c => c.id === step.connection_id) : null;
  const cfg = Object.assign({}, linked ? (linked.config || {}) : {}, stepCfg);
  /* file/HTTP sources have no SQL — the source is in their own config */
  const effType = stepConnType(step);
  const isHttp = ['json_http','csv','parquet','siebel','xml','soap'].includes(effType);
  /* У приёмника «что читать» — это его НАЗНАЧЕНИЕ: смотрим, что уже лежит там,
   * куда шаг собирается писать. Полезно перед режимом «перезаписать». */
  const query = step.is_sink
    ? (step.sink_entity || cfg.topic || cfg.table || step.target_table || '').trim()
    : (cfg.query || cfg.table || '').trim();
  if (step.is_sink) {
    if (!query && !cfg.url && !cfg.path) {
      out.innerHTML = '<div style="padding:.4rem;color:var(--amber);font-size:.8rem">Укажи назначение — таблицу, топик или путь файла</div>';
      return;
    }
  } else if (isHttp) {
    if (!cfg.url && !cfg.path) { out.innerHTML = '<div style="padding:.4rem;color:var(--amber);font-size:.8rem">Укажи адрес (URL) или путь к файлу</div>'; return; }
  } else if (!query) {
    out.innerHTML = '<div style="padding:.4rem;color:var(--amber);font-size:.8rem">Укажи таблицу или SQL источника</div>';
    return;
  }
  out.innerHTML = '<div style="padding:.4rem;color:var(--muted);font-size:.8rem">Выполняется…</div>';
  try {
    const r = await apiPost('/api/connector/probe/preview',
      { type: effType, config: stripMaskedSecrets(cfg), query: query || '', limit: 200,
        connection_id: step.connection_id || '' });
    if (!r) { out.innerHTML = '<div style="padding:.4rem;color:var(--red);font-size:.8rem">Пустой ответ</div>'; return; }
    if (r.error) {
      out.innerHTML = `<div style="padding:.4rem;color:var(--red);font-size:.8rem;white-space:pre-wrap">${escHtml(r.error)}</div>`;
      return;
    }
    out.innerHTML = '';
    step._lastPreview = r;          /* remember for the data-explorer modal */
    const n = (r.rows || []).length;
    const status = document.createElement('div');
    status.style.cssText = 'font-size:.78rem;color:var(--muted);margin-bottom:.35rem';
    if (isHttp) {
      /* REST: no SQL queries — just show the fetched rows. */
      status.textContent = `${n} ${pluralRows(n)}`;
      out.appendChild(status);
      out.appendChild(makeResultTable(r));
    } else {
      status.textContent = `${n} ${pluralRows(n)} · клик по таблице → SQL-запрос`;
      out.appendChild(status);
      const wrap = document.createElement('div');
      wrap.style.cursor = 'pointer';
      wrap.title = 'Открыть в окне и сделать SQL-запрос';
      wrap.onclick = () => openSrcDataModal(idx);
      wrap.appendChild(makeResultTable(r));
      out.appendChild(wrap);
    }
  } catch (e) {
    out.innerHTML = `<div style="padding:.4rem;color:var(--red);font-size:.8rem">${escHtml(String(e))}</div>`;
  }
}

/* ── Source data explorer: modal over the previewed data with a SQL editor that
 * re-queries the source connector. Opened by clicking the preview table. ── */
let sdmIdx = -1;
function openSrcDataModal(idx) {
  sdmIdx = idx;
  const step = pbStep(idx);
  const cfg  = safeParse(step.connector_config, {});
  document.getElementById('sdm-sql').value = cfg.query || cfg.table || '';
  const res = step._lastPreview;
  const out = document.getElementById('sdm-result');
  out.innerHTML = '';
  if (res) out.appendChild(makeResultTable(res));
  document.getElementById('sdm-status').textContent =
    res ? `${(res.rows || []).length} ${pluralRows((res.rows || []).length)}` : '';
  document.getElementById('src-data-modal').classList.remove('hidden');
}
function closeSrcDataModal() {
  document.getElementById('src-data-modal').classList.add('hidden');
}
async function sdmRun() {
  if (sdmIdx < 0) return;
  const step = pb.steps[sdmIdx];
  const cfg  = safeParse(step.connector_config, {});
  const query = document.getElementById('sdm-sql').value.trim();
  const out = document.getElementById('sdm-result');
  const status = document.getElementById('sdm-status');
  if (!query) { status.style.color = 'var(--amber)'; status.textContent = 'Пустой запрос'; return; }
  status.style.color = 'var(--muted)'; status.textContent = 'Выполняется…'; out.innerHTML = '';
  try {
    const r = await apiPost('/api/connector/probe/preview',
      { type: step.connector_type, config: cfg, query, limit: 200,
        connection_id: step.connection_id || '' });
    if (!r) { status.textContent = 'Пустой ответ'; return; }
    if (r.error) {
      status.style.color = 'var(--red)'; status.textContent = 'Ошибка';
      out.innerHTML = `<div style="padding:.5rem;color:var(--red);font-size:.82rem;white-space:pre-wrap">${escHtml(r.error)}</div>`;
      return;
    }
    status.style.color = 'var(--muted)';
    status.textContent = `${(r.rows || []).length} ${pluralRows((r.rows || []).length)}`;
    out.appendChild(makeResultTable(r));
    step._lastPreview = r;
  } catch (e) {
    status.style.color = 'var(--red)'; status.textContent = 'Ошибка';
    out.innerHTML = `<div style="padding:.5rem;color:var(--red);font-size:.82rem">${escHtml(String(e))}</div>`;
  }
}

/* ── SQL-transform table view: modal with a SQL editor over DFO's own tables
 * (/api/tables/query). Opened by clicking the step's SQL result table. ── */
let sqmIdx = -1;
function openSqlDataModal(idx) {
  sqmIdx = idx;
  document.getElementById('sqm-sql').value = pb.steps[idx].transform_sql || '';
  const res = pb.steps[idx]._lastSqlResult;
  const out = document.getElementById('sqm-result');
  out.innerHTML = '';
  if (res) out.appendChild(makeResultTable(res));
  document.getElementById('sqm-status').textContent =
    res ? `${(res.rows || []).length} ${pluralRows((res.rows || []).length)}` : '';
  document.getElementById('sql-data-modal').classList.remove('hidden');
}
function closeSqlDataModal() {
  document.getElementById('sql-data-modal').classList.add('hidden');
}
async function sqmRun() {
  if (sqmIdx < 0) return;
  const step = pb.steps[sqmIdx];
  const sql = document.getElementById('sqm-sql').value.trim();
  const out = document.getElementById('sqm-result');
  const status = document.getElementById('sqm-status');
  if (!sql) { status.style.color = 'var(--amber)'; status.textContent = 'Пустой запрос'; return; }
  status.style.color = 'var(--muted)'; status.textContent = 'Выполняется…'; out.innerHTML = '';
  try {
    const data = await apiPost('/api/tables/query', { sql });
    if (!data) { status.textContent = 'Пустой ответ'; return; }
    status.textContent = `${(data.rows || []).length} ${pluralRows((data.rows || []).length)}`;
    out.appendChild(makeResultTable(data));
    step._lastSqlResult = data;
  } catch (e) {
    let msg = String(e);
    try { const j = JSON.parse(msg.replace(/^Error:\s*/, '')); if (j.error) msg = j.error; } catch (_) {}
    status.style.color = 'var(--red)'; status.textContent = 'Ошибка';
    out.innerHTML = `<div style="padding:.5rem;color:var(--red);font-size:.82rem;white-space:pre-wrap">${escHtml(msg)}</div>`;
  }
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

const CONNECTOR_TYPES = ['csv','parquet','json_http','postgresql','greenplum','oracle','kafka','siebel','xml','soap'];

/* Human-readable connector list for the unified «Источник» sub-selector. */
const SOURCE_OPTIONS = [
  ['postgresql','PostgreSQL'], ['greenplum','Greenplum'], ['oracle','Oracle'],
  ['kafka','Kafka'], ['json_http','HTTP / REST'], ['soap','SOAP'], ['xml','XML'],
  ['csv','CSV'], ['parquet','Parquet'], ['siebel','Siebel / EIM'],
];

/* Connectors that can act as sinks (write_batch in ABI v2). */
const SINK_TYPES = ['csv','json_http','postgresql','kafka','greenplum','oracle','parquet','siebel','xml','soap'];

/* Human-readable sink list for the unified «Приёмник» sub-selector. */
const SINK_OPTIONS = [
  ['postgresql','PostgreSQL'], ['greenplum','Greenplum'], ['oracle','Oracle'],
  ['kafka','Kafka'], ['json_http','HTTP / Webhook'], ['soap','SOAP'], ['xml','XML'],
  ['csv','CSV'], ['parquet','Parquet'], ['siebel','Siebel / EIM'],
];

/* Определяет тип шага для UI: явный s._ui_type имеет приоритет, иначе выводится
   из заполненных полей (sink → 'sink:<коннектор>', match_rules, scd2, python, scala,
   connector_type или по умолчанию 'sql'). */
/* Эффективный тип коннектора шага: свой, либо из привязанного подключения.
 * Шаг может быть задан одной ссылкой (connector_type пуст) — тогда тип знает
 * только справочник. */
function stepConnType(step) {
  if (step.connector_type) return step.connector_type;
  const rec = step.connection_id ? (connCache || []).find(c => c.id === step.connection_id) : null;
  return rec ? rec.type : '';
}

function stepType(step) {
  if (step._ui_type) return step._ui_type;
  /* Шаг может быть задан ОДНОЙ ссылкой на подключение, без connector_type —
   * так его принимает и исполняет гейтвей (тип берётся из справочника при
   * запуске). Без этой ветки конструктор считал такой шаг обычным SQL и не
   * показывал ни выбор подключения, ни предпросмотр. */
  if (step.connection_id && !step.connector_type) {
    const rec = (connCache || []).find(c => c.id === step.connection_id);
    const t = rec ? rec.type : '';
    if (step.is_sink) return t ? 'sink:' + t : 'sink';
    if (t) return t;
    return step.is_sink ? 'sink' : 'postgresql';   /* справочник ещё не загружен */
  }
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

/* Смена типа шага из выпадающего списка: обнуляет все «различающие» поля и заново
   заполняет только выбранное (стартовый код/дефолтный коннектор/поля SCD2/…),
   затем перерисовывает список шагов. type может быть 'source'/'sink'/'transform'
   (унифицированные, выбор коннектора/языка внутри формы) или конкретным. */
function pbChangeStepType(idx, type) {
  const s = pb.steps[idx];
  s._ui_type = type;
  /* Reset every discriminating field so only the chosen one stays set. */
  s.connector_type = '';
  s.connector_config = '';
  /* Ссылку на подключение тоже сбрасываем: у подключения есть направление, и
   * при смене «Источник» → «Приёмник» прежняя ссылка вылетела бы из фильтра
   * списка, оставшись выбранной. */
  s.connection_id = '';
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
  } else if (type === 'transform') {
    /* Unified transform step: SQL by default; language switched in-form. */
    s._ui_type = '';      /* let stepType derive from the filled field */
    s._lang = 'sql';
  } else if (type === 'source') {
    /* Unified source step: pick the connector in-form (default PostgreSQL). */
    s._ui_type = '';
    s.connector_type = 'postgresql';
    s.connector_config = '{}';
  } else if (type === 'sink') {
    /* Unified sink step: pick the connector in-form (default PostgreSQL). */
    s._ui_type = '';
    s.is_sink = true;
    s.connector_type = 'postgresql';
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

/* Switch the language of a unified «Трансформация» step (SQL / Python / Scala).
 * transform_sql is kept (it's the SQL transform in SQL mode, or the input SELECT
 * feeding df in Python/Scala mode); only the chosen code field is seeded. */
function pbSetTransformLang(idx, lang) {
  const s = pb.steps[idx];
  s._ui_type = '';
  s._lang = lang;
  s.python_code = ''; s.python_file = ''; s.scala_code = '';
  if (lang === 'python') {
    s.python_code = PY_STARTER;
  } else if (lang === 'scala') {
    s.scala_code = SCALA_STARTER;
    if (s.scala_timeout_sec == null) s.scala_timeout_sec = 600;
  }
  renderBuilderSteps();
  if (typeof saveBuilderDraft === 'function') saveBuilderDraft();
}

/* Switch the method of a unified «Сопоставление» step: 'match' (fuzzy SQL) or
 * 'matchrules' (rule engine). transform_sql (candidate set) is kept. */
function pbSetMatchMethod(idx, method) {
  const s = pb.steps[idx];
  if (method === 'matchrules') {
    s._ui_type = 'matchrules';
    s.match_rules = s.match_rules || MATCH_RULES_STARTER;
  } else {
    s._ui_type = 'match';
    s.match_rules = '';
  }
  renderBuilderSteps();
  if (typeof saveBuilderDraft === 'function') saveBuilderDraft();
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

/* Build a match-rules JSON (one rule) from the human-readable Russian fields. */
function pbBuildMatchRule(idx) {
  const s = pb.steps[idx];
  const cols = v => (v || '').split(',').map(x => x.trim()).filter(Boolean);
  const rule = {
    rule_name:       (s._mr_name || 'Правило 1').trim(),
    id_column:       (s._mr_id || 'party_id').trim(),
    scan_columns:    cols(s._mr_scan),
    test_columns:    cols(s._mr_test),
    metric_function: s._mr_metric || 'word_similarity',
    operator:        '>=',
    threshold:       s._mr_threshold != null ? Number(s._mr_threshold) : 0.80,
  };
  if (s._mr_normalize) rule.normalize = s._mr_normalize;
  s.match_rules = JSON.stringify([rule], null, 2);
  renderBuilderSteps();
  if (typeof saveBuilderDraft === 'function') saveBuilderDraft();
}

/* Sink (приёмник) fields: destination entity + write mode. The connection
 * config (path/host/bucket/brokers/...) is rendered by makeConnectorConfigHTML
 * above, since a sink reuses the connector's own config form. */
function makeSinkFieldsHTML(step, idx) {
  /* Kafka / REST: назначение задаётся в блоке подключения (топик / URL), как у
     источника; отдельного поля назначения и режима записи не нужно. */
  if (step.connector_type === 'kafka' || step.connector_type === 'json_http') return '';
  const ENT = {
    csv:        ['Путь файла',        './data/export.csv'],
    parquet:    ['Путь файла',        './data/export.parquet'],
    json_http:  ['URL (пусто = из конфига)', 'https://hooks.example.com/ingest'],
    postgresql: ['Таблица назначения', 'public.export_table'],
    greenplum:  ['Таблица назначения', 'public.export_table'],
    oracle:     ['Таблица назначения', 'EXPORT_TABLE'],
    kafka:      ['Топик',              'dfo_export'],
  }[step.connector_type] || ['Назначение', ''];
  const mode = step.sink_mode || 'append';
  return `
    <div style="margin-top:.75rem">
      <div class="step-row-2" style="align-items:center">
        <div class="form-group" style="margin:0">
          <label>${ENT[0]}</label>
          <input type="text" value="${escAttr(step.sink_entity || '')}" placeholder="${escAttr(ENT[1])}"
                 oninput="pbUpdateStep(${idx},'sink_entity',this.value)">
        </div>
        <div class="form-group" style="margin:0">
          <label>Режим записи</label>
          <select onchange="pbUpdateStep(${idx},'sink_mode',this.value)">
            <option value="append"    ${mode==='append'   ?'selected':''}>Дозапись — добавить к существующим</option>
            <option value="overwrite" ${mode==='overwrite'?'selected':''}>Перезапись — заменить всё</option>
          </select>
        </div>
      </div>
    </div>`;
}

/* Поля SCD2-историзации (медленно меняющиеся измерения 2-го типа): бизнес-ключ,
   целевая dim-таблица, колонки начала/конца действия версии записи. */
function makeScd2FieldsHTML(step, idx) {
  const f = (label, key, ph) => `
    <div class="form-group" style="margin:0"><label>${label}</label>
      <input type="text" value="${escAttr(step[key] || '')}" placeholder="${ph}"
             oninput="pbUpdateStep(${idx},'${key}',this.value)"></div>`;
  return `
    <div style="margin-top:.75rem">
      <div class="step-row-2">
        ${f('Бизнес-ключ записи','scd2_business_key','customer_id — через запятую для составного')}
        ${f('Целевая таблица историзации','target_table','dim_customers')}
      </div>
      <div class="step-row-2" style="margin-top:.4rem">
        ${f('Колонка начала действия','scd2_effective_from_col','valid_from')}
        ${f('Колонка конца действия','scd2_effective_to_col','valid_to')}
      </div>
      <div class="form-group" style="margin:.4rem 0 0">
        <label>Колонки сравнения (пусто = все колонки)</label>
        <input type="text" value="${escAttr(step.scd2_compare_columns || '')}" placeholder="name, email, city"
               oninput="pbUpdateStep(${idx},'scd2_compare_columns',this.value)">
      </div>
    </div>`;
}

/* Поля нечёткого сопоставления (метод «match»): две таблицы, колонка сравнения и
   порог схожести. Кнопка «Построить запрос» генерирует SQL с jaro_winkler
   (pbGenerateMatchSQL) в поле transform_sql. Значения хранятся в UI-полях _match_*. */
function makeMatchFieldsHTML(step, idx) {
  const thr = step._match_threshold != null ? step._match_threshold : 0.85;
  return `
    <div style="margin-top:.75rem">
      <div class="step-row-2">
        <div class="form-group" style="margin:0"><label>Первая таблица</label>
          <input type="text" value="${escAttr(step._match_left || '')}" placeholder="клиенты_crm"
                 oninput="pbUpdateStep(${idx},'_match_left',this.value)"></div>
        <div class="form-group" style="margin:0"><label>Вторая таблица</label>
          <input type="text" value="${escAttr(step._match_right || '')}" placeholder="клиенты_erp"
                 oninput="pbUpdateStep(${idx},'_match_right',this.value)"></div>
      </div>
      <div class="step-row-2" style="margin-top:.4rem;align-items:center">
        <div class="form-group" style="margin:0"><label>Колонка для сравнения</label>
          <input type="text" value="${escAttr(step._match_col || '')}" placeholder="имя"
                 oninput="pbUpdateStep(${idx},'_match_col',this.value)"></div>
        <div class="form-group" style="margin:0"><label>Порог схожести (0–1)</label>
          <input type="number" min="0" max="1" step="0.05" value="${escAttr(thr)}"
                 oninput="pbUpdateStep(${idx},'_match_threshold',parseFloat(this.value))"></div>
      </div>
      <div style="display:flex;justify-content:flex-end;margin-top:.5rem">
        <button type="button" class="btn btn-primary btn-sm" style="min-width:140px;justify-content:center"
                onclick="event.stopPropagation();pbGenerateMatchSQL(${idx})">⚙ Построить запрос</button>
      </div>
    </div>`;
}

/* Match-rules: declarative entity-resolution engine (run_match_step). The
 * candidate set comes from the SQL-трансформация field below; match_rules is a
 * JSON array of rules. See docs/MATCH_RULES.md. */
function makeMatchRulesFieldsHTML(step, idx) {
  /* Pre-fill the structured fields from the first rule in the JSON (round-trip). */
  let first = {};
  try { const arr = JSON.parse(step.match_rules || '[]'); if (Array.isArray(arr) && arr[0]) first = arr[0]; } catch (_) {}
  const val   = (uiKey, jsonKey) => step[uiKey] != null ? step[uiKey] : (first[jsonKey] || '');
  const name  = val('_mr_name', 'rule_name');
  const idCol = val('_mr_id', 'id_column');
  const scan  = step._mr_scan != null ? step._mr_scan : (first.scan_columns || []).join(', ');
  const test  = step._mr_test != null ? step._mr_test : (first.test_columns || []).join(', ');
  const metric = step._mr_metric || first.metric_function || 'word_similarity';
  const thr   = step._mr_threshold != null ? step._mr_threshold : (first.threshold != null ? first.threshold : 0.80);
  const norm  = step._mr_normalize != null ? step._mr_normalize : (first.normalize || '');
  return `
    <div style="margin-top:.75rem">
      <div class="step-row-2">
        <div class="form-group" style="margin:0"><label>Название правила</label>
          <input type="text" value="${escAttr(name)}" placeholder="Правило 1: документ → ФИО"
                 oninput="pbUpdateStep(${idx},'_mr_name',this.value)"></div>
        <div class="form-group" style="margin:0"><label>Колонка идентификатора</label>
          <input type="text" value="${escAttr(idCol)}" placeholder="party_id"
                 oninput="pbUpdateStep(${idx},'_mr_id',this.value)"></div>
      </div>
      <div class="step-row-2" style="margin-top:.4rem">
        <div class="form-group" style="margin:0"><label>Колонки группировки (точное совпадение, через запятую)</label>
          <input type="text" value="${escAttr(scan)}" placeholder="номер_документа"
                 oninput="pbUpdateStep(${idx},'_mr_scan',this.value)"></div>
        <div class="form-group" style="margin:0"><label>Колонки сравнения (по схожести, через запятую)</label>
          <input type="text" value="${escAttr(test)}" placeholder="фамилия, имя"
                 oninput="pbUpdateStep(${idx},'_mr_test',this.value)"></div>
      </div>
      <div class="step-row-2" style="margin-top:.4rem;align-items:center">
        <div class="form-group" style="margin:0"><label>Метрика схожести</label>
          <select onchange="pbUpdateStep(${idx},'_mr_metric',this.value)">
            <option value="word_similarity" ${metric==='word_similarity'?'selected':''}>Похожесть слов</option>
            <option value="jaro_winkler"    ${metric==='jaro_winkler'   ?'selected':''}>Джаро–Винклер</option>
            <option value="levenshtein"     ${metric==='levenshtein'    ?'selected':''}>Левенштейн</option>
          </select></div>
        <div class="form-group" style="margin:0"><label>Порог схожести (0–1)</label>
          <input type="number" min="0" max="1" step="0.05" value="${escAttr(thr)}"
                 oninput="pbUpdateStep(${idx},'_mr_threshold',parseFloat(this.value))"></div>
      </div>
      <div class="step-row-2" style="margin-top:.4rem;align-items:center">
        <div class="form-group" style="margin:0"><label>Нормализация значений</label>
          <select onchange="pbUpdateStep(${idx},'_mr_normalize',this.value)">
            <option value=""     ${norm===''    ?'selected':''}>Нет</option>
            <option value="name" ${norm==='name'?'selected':''}>Имена / ФИО</option>
          </select></div>
        <div class="form-group" style="margin:0"></div>
      </div>
      <div style="display:flex;justify-content:flex-end;margin-top:.5rem">
        <button type="button" class="btn btn-primary btn-sm" style="min-width:140px;justify-content:center"
                onclick="event.stopPropagation();pbBuildMatchRule(${idx})">⚙ Построить правило</button>
      </div>
      <div class="form-group" style="margin:.6rem 0 0">
        <label>Все правила в формате JSON</label>
        <textarea class="mono-textarea" rows="8"
          oninput="pbStep(${idx}).match_rules=this.value"
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
    container.innerHTML = '';
    container.style.display = 'none';   /* no phantom flex-gap when empty */
    return;
  }
  container.style.display = '';
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
  /* Оставляем в карточках только поля запроса — доступ живёт в подключении. */
  pb.steps.forEach((st, i) => {
    const t = stepType(st);
    if (CONNECTOR_TYPES.includes(t) || t.startsWith('sink:'))
      applyConnFieldFilter('step-conn-cfg-' + i, stepConnType(st), 'request');
  });
}

/* Цвет узла графа по типу шага — для цветовой индикации разных типов. */
function stepColor(t) {
  /* Приглушённая палитра — спокойные тона, белый текст бейджа читается. */
  if (t.startsWith('sink:'))         return '#c0894f';  /* приёмник — приглушённый янтарный */
  if (CONNECTOR_TYPES.includes(t))   return '#5e9b78';  /* источник — приглушённый зелёный */
  if (t === 'python')                return '#b3994d';  /* Python — приглушённое золото */
  if (t === 'scala')                 return '#bd6f6f';  /* Scala — приглушённый красный */
  if (t === 'matchrules')            return '#8d7cb3';  /* MDM-сопоставление — приглушённый фиолетовый */
  if (t === 'scd2')                  return '#519089';  /* SCD2 — приглушённый бирюзовый */
  return '#6b7cc2';                                     /* SQL-трансформация — приглушённый синий */
}

/* Короткая подпись типа шага для бейджа на ноде графа. */
function stepTypeShort(t) {
  if (t.startsWith('sink:'))       return 'ПРИЁМНИК';
  if (CONNECTOR_TYPES.includes(t)) return 'ИСТОЧНИК';
  return { python: 'PYTHON', scala: 'SCALA', matchrules: 'MDM', scd2: 'SCD2', sql: 'SQL' }[t] || 'SQL';
}

/* Открыть граф шагов крупнее в отдельной модалке (клон текущего SVG). */
function openFlowModal() {
  const src = document.getElementById('pb-flow-graph');
  if (!src || !src.querySelector('svg')) return;   /* нет графа (только заглушка) — нечего показывать */
  document.getElementById('flow-modal-body').innerHTML = src.innerHTML;
  document.getElementById('flow-modal').classList.remove('hidden');
}

/* Рисует DAG конвейера как inline-SVG: раскладывает шаги по колонкам-уровням
   (уровень = длиннейший путь по deps), параллельные шаги уровня — стопкой по рядам,
   связи — кривыми Безье со стрелкой. Клик по ноде скроллит к её карточке. */
function renderFlowGraph() {
  const wrap = document.getElementById('pb-flow-graph');
  if (!wrap) return;
  const outer = wrap.closest('.pb-flow-wrap');
  if (!pb.steps.length) {
    /* Область графа видна всегда — при пустых шагах показываем заглушку. */
    wrap.innerHTML = '<div class="pb-flow-empty">Шагов пока нет — добавьте шаг, чтобы построить граф</div>';
    if (outer) outer.style.display = '';
    return;
  }
  if (outer) outer.style.display = '';

  const steps = pb.steps;
  const ACCENT  = '#5b6ef5';
  const NODE_BG = '#252d4a';
  const TEXT    = '#f1f5f9';
  const SUB     = '#94a3b8';

  /* deps → индексы шагов (поддержка и числовых индексов, и строковых id);
     самозависимость отбрасывается. */
  const depIdx = (st, self) => (st.deps || [])
    .map(d => typeof d === 'number' ? d : steps.findIndex(x => x.id === d))
    .filter(j => j >= 0 && j < steps.length && j !== self);

  /* Уровень (колонка) = длиннейший путь от корней (шагов без зависимостей).
     Шаги одного уровня без связи между собой идут параллельно. */
  const level = new Array(steps.length).fill(0);
  for (let pass = 0; pass < steps.length; pass++) {
    let changed = false;
    steps.forEach((st, i) => {
      const ds = depIdx(st, i);
      const lv = ds.length ? Math.max(...ds.map(d => level[d] + 1)) : 0;
      if (lv > level[i]) { level[i] = lv; changed = true; }
    });
    if (!changed) break;
  }
  /* защита от циклов/битых deps: уровень не выше числа шагов */
  for (let i = 0; i < steps.length; i++) level[i] = Math.min(level[i], steps.length - 1);
  const maxLevel = Math.max(0, ...level);
  /* ряд шага внутри его уровня (параллельные шаги — стопкой) */
  const rowOf = new Array(steps.length);
  let maxRows = 1;
  for (let lv = 0; lv <= maxLevel; lv++) {
    let r = 0;
    steps.forEach((_, i) => { if (level[i] === lv) rowOf[i] = r++; });
    maxRows = Math.max(maxRows, r);
  }

  const NW = 200, NH = 80, GX = 72, GY = 52, PADX = 34, PADY = 48;
  const posX = i => PADX + level[i] * (NW + GX);
  const posY = i => PADY + rowOf[i] * (NH + GY);
  const W = (maxLevel + 1) * (NW + GX) - GX + PADX * 2;
  const H = maxRows * (NH + GY) - GY + PADY * 2;

  let s = `<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <marker id="arh" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto">
      <polygon points="0 0,10 4,0 8" fill="${ACCENT}"/>
    </marker>
  </defs>`;

  /* Связи: от правого края каждой зависимости к левому краю шага (кривая Безье,
     чтобы аккуратно соединять параллельные ветки на разных рядах). */
  steps.forEach((st, i) => {
    depIdx(st, i).forEach(d => {
      const x1 = posX(d) + NW, y1 = posY(d) + NH / 2;
      const x2 = posX(i) - 3,  y2 = posY(i) + NH / 2;
      const mx = (x1 + x2) / 2;
      s += `<path d="M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}"
              fill="none" stroke="${ACCENT}" stroke-width="2" marker-end="url(#arh)"/>`;
    });
  });

  /* Узлы */
  steps.forEach((step, i) => {
    const x = posX(i), y = posY(i), cx = x + NW / 2;
    const name  = (step.name || `Шаг ${i + 1}`).slice(0, 24);
    const table = step.target_table ? step.target_table.slice(0, 26) : '';
    const tp    = stepType(step);
    const col   = stepColor(tp);               /* цвет рамки/бейджа по типу шага */
    const tShort = stepTypeShort(tp);          /* подпись типа на бейдже */
    const bw = Math.max(52, tShort.length * 7 + 18), bh = 20, bx = cx - bw / 2, by = y - bh - 6;

    s += `<g onclick="event.stopPropagation();document.getElementById('step-card-${i}')?.scrollIntoView({behavior:'smooth',block:'nearest'})" style="cursor:pointer">
      <rect x="${x}" y="${y}" width="${NW}" height="${NH}" rx="10"
            fill="${NODE_BG}" stroke="${col}" stroke-width="2.5"/>
      <rect x="${bx}" y="${by}" width="${bw}" height="${bh}" rx="10"
            fill="${col}"/>
      <text x="${cx}" y="${by + 14}" text-anchor="middle" font-size="10" font-weight="700"
            fill="#fff" font-family="system-ui,sans-serif" letter-spacing=".05em">${tShort}</text>
      <text x="${cx}" y="${y + 30}" text-anchor="middle" font-size="13" font-weight="600"
            fill="${TEXT}" font-family="system-ui,sans-serif">${escHtml(name)}</text>
      ${table ? `<text x="${cx}" y="${y + 52}" text-anchor="middle" font-size="11"
            fill="${SUB}" font-family="system-ui,sans-serif">→ ${escHtml(table)}</text>` : ''}
    </g>`;
  });

  s += '</svg>';
  wrap.innerHTML = s;
}

/* Собирает карточку одного шага конвейера: заголовок с типом, выбор
   типа/языка/коннектора, блок SQL-входа, редактор кода или форму коннектора
   (makeConnectorConfigHTML), поля SCD2/Match, зависимости (deps) и target_table.
   Порядок блоков зависит от типа (для трансформаций вход→код, для коннекторов —
   конфиг→SQL). Возвращает DOM-элемент .step-card. */
function makeStepCard(step, idx) {
  const div = document.createElement('div');
  div.className = 'step-card' + (step._collapsed ? ' collapsed' : '');
  div.id = `step-card-${idx}`;
  const t = stepType(step);
  const isTransform = ['sql','python','scala'].includes(t);
  const lang = t === 'python' ? 'python' : t === 'scala' ? 'scala' : 'sql';
  const isMatch = t === 'match' || t === 'matchrules';
  const matchMethod = t === 'matchrules' ? 'matchrules' : 'match';
  const isSource = CONNECTOR_TYPES.includes(t);   /* unified «Источник» step */
  const isSink = t.startsWith('sink:');            /* unified «Приёмник» step */
  const sinkType = isSink ? t.slice(5) : '';
  /* Input/transform SQL block — placed BEFORE the code editor for transforms
     (вход → код), and AFTER the connector config for connector steps. */
  const sqlBlock = `
      <div class="form-group" style="margin:0">
        <label>${t.startsWith('sink:') ? 'Что выгружать (SQL)' : t === 'matchrules' ? 'Набор кандидатов (SQL)' : t === 'scd2' ? 'Источник — срез данных (SQL)' : lang === 'sql' ? 'SQL-трансформация' : 'Входные данные (SQL → df)'}</label>
        <textarea class="mono-textarea" rows="4"
                  placeholder="SELECT * FROM {@prev} WHERE ..."
                  oninput="pbUpdateStep(${idx},'transform_sql',this.value)">${escHtml(step.transform_sql || '')}</textarea>
        <div style="display:flex;justify-content:flex-end;margin-top:.4rem">
          <button type="button" class="btn btn-primary btn-sm" style="min-width:140px;justify-content:center"
                  onclick="event.stopPropagation();runStepSQL(${idx})"
                  title="Выполнить только этот SQL (без upstream-шагов)">▶ Выполнить</button>
        </div>
      </div>`;
  div.innerHTML = `
    <div class="step-header">
      <button class="step-collapse" onclick="pbToggleStep(${idx})" title="Свернуть / развернуть шаг" aria-label="Свернуть / развернуть">▾</button>
      <span class="step-num">${idx + 1}</span>
      <input class="step-name-input" type="text" placeholder="Название шага"
             value="${escAttr(step.name)}"
             oninput="pbUpdateStep(${idx},'name',this.value);renderFlowGraph()">
      <button class="step-del" onclick="pbRemoveStep(${idx})" title="Удалить шаг" aria-label="Удалить шаг">×</button>
    </div>
    <div class="step-body">
      <div class="step-row-2">
        <div class="form-group" style="margin:0">
          <label>Тип шага <span class="label-hint">= какое поле заполнено</span></label>
          <select class="step-type-select" onchange="pbChangeStepType(${idx}, this.value)">
            <optgroup label="Загрузка данных">
              <option value="source" ${CONNECTOR_TYPES.includes(t)?'selected':''}>Источник</option>
            </optgroup>
            <optgroup label="Трансформации">
              <option value="transform" ${['sql','python','scala'].includes(t)?'selected':''}>Трансформация</option>
              <option value="scd2"   ${t==='scd2'  ?'selected':''}>Историзация изменений</option>
              <option value="match"  ${(t==='match'||t==='matchrules')?'selected':''}>Сопоставление</option>
            </optgroup>
            <optgroup label="Выгрузка данных">
              <option value="sink" ${t.startsWith('sink:')?'selected':''}>Приёмник</option>
            </optgroup>
          </select>
        </div>
      </div>
      ${isTransform ? `
      <div style="display:flex;gap:.4rem;margin-top:.6rem">
        ${[['sql','SQL'],['python','Python'],['scala','Scala']].map(([L,name]) => `
          <button type="button" class="btn btn-sm${lang===L?' btn-primary':''}"
                  onclick="pbSetTransformLang(${idx},'${L}')">${name}</button>`).join('')}
      </div>` : ''}
      ${isMatch ? `
      <div style="display:flex;gap:.4rem;margin-top:.6rem">
        ${[['match','Нечёткое'],['matchrules','По правилам']].map(([m,name]) => `
          <button type="button" class="btn btn-sm${matchMethod===m?' btn-primary':''}"
                  onclick="pbSetMatchMethod(${idx},'${m}')">${name}</button>`).join('')}
      </div>` : ''}
      ${isTransform ? sqlBlock : ''}
      ${(isSource || isSink) ? makeConnectionPickerHTML(step, idx, isSink ? 'sink' : 'source') : ''}
      ${/* Параметры доступа задаются в подключении, а ЗАПРОС к данным — здесь:
            он свой у каждого конвейера. Ниже та же форма коннектора, из которой
            фильтр оставляет только поля запроса (см. applyConnFieldFilter). */''}
      <div id="step-conn-cfg-${idx}" style="margin-top:0.75rem">${makeConnectorConfigHTML(stepRenderView(step), idx)}</div>
      ${t.startsWith('sink:') ? makeSinkFieldsHTML(step, idx) : ''}
      ${t === 'scd2'  ? makeScd2FieldsHTML(step, idx)  : ''}
      ${t === 'match' ? makeMatchFieldsHTML(step, idx) : ''}
      ${t === 'matchrules' ? makeMatchRulesFieldsHTML(step, idx) : ''}
      ${(isTransform || isSource || t === 'match') ? '' : sqlBlock}
    </div>
  `;
  return div;
}

/* Строит HTML-форму настроек коннектора для шага idx в зависимости от step.connector_type
   (csv/parquet/json_http/postgresql/kafka/oracle/xml/soap/siebel/…) либо редактор кода
   Python/Scala, если тип коннектора не задан. Поля пишут в step.connector_config через
   pbUpdateConnConfig; для источников (не is_sink) добавляются кнопки «Показать данные»
   (pbPreviewSource) и «Подключиться» (pbTestConnection). Ветки для sink скрывают
   source-only поля (путь, заголовок, пагинация). Возвращает строку HTML. */
function makeConnectorConfigHTML(step, idx) {
  /* Шаг может быть задан ОДНОЙ ссылкой на подключение, без connector_type —
   * тогда тип знает только справочник. Без этого форма запроса (SQL, режим
   * чтения, топик) для такого шага не рисовалась вовсе. */
  const type = stepConnType(step);
  const cfg  = safeParse(step.connector_config, {});

  /* Pure SQL transform: no connector / python / scala → nothing to configure
     here (the SQL editor itself is rendered by makeStepCard). */
  if (!type && !step.python_code && !step.python_file && !step.scala_code) return '';

  /* Python step (no connector_type, but has python_code or python_file) */
  if (!type && (step.python_code || step.python_file)) {
    return `
      <div class="form-group" style="margin:0">
        <label>Код Python</label>
        <textarea class="mono-textarea" rows="8"
          oninput="pbStep(${idx}).python_code=this.value"
          style="font-family:var(--mono);font-size:.78rem">${escHtml(step.python_code || '')}</textarea>
        <div style="display:flex;justify-content:flex-end;margin-top:.4rem">
          <button type="button" class="btn btn-primary btn-sm" style="min-width:140px;justify-content:center"
                  onclick="event.stopPropagation();runStepPython(${idx})"
                  title="Выполнить только этот Python (без предыдущих шагов)">▶ Выполнить</button>
        </div>
      </div>`;
  }

  /* Scala step (no connector_type, but has scala_code) */
  if (!type && step.scala_code) {
    return `
      <div class="form-group" style="margin:0">
        <label>Код Scala</label>
        <textarea class="mono-textarea" rows="8"
          oninput="pbStep(${idx}).scala_code=this.value"
          style="font-family:var(--mono);font-size:.78rem">${escHtml(step.scala_code)}</textarea>
        <div style="display:flex;justify-content:flex-end;margin-top:.4rem">
          <button type="button" class="btn btn-primary btn-sm" style="min-width:140px;justify-content:center"
                  onclick="event.stopPropagation();runStepScala(${idx})"
                  title="Выполнить только этот Scala (без предыдущих шагов)">▶ Выполнить</button>
        </div>
      </div>`;
  }

  if (!type) return '';

  if (type === 'csv') {
    const hdr = cfg.header === 'false' ? 'false' : 'true';
    return `
    <div class="conn-group">
      <div class="conn-group-title">CSV-файл</div>
      <div class="step-row-2" style="align-items:end">
        ${step.is_sink ? '' : `
        <div class="form-group" style="margin:0;flex:2">
          <label>Путь к файлу</label>
          <input type="text" value="${escAttr(cfg.path || '')}"
                 oninput="pbUpdateConnConfig(${idx},'path',this.value)" placeholder="/data/input.csv">
        </div>`}
        <div class="form-group" style="margin:0">
          <label>Разделитель</label>
          <select onchange="pbUpdateConnConfig(${idx},'delimiter',this.value)">
            <option value=","  ${(cfg.delimiter||',')===',' ?'selected':''}>Запятая (,)</option>
            <option value=";"  ${cfg.delimiter===';'       ?'selected':''}>Точка с запятой (;)</option>
            <option value="\t" ${cfg.delimiter==='\t'      ?'selected':''}>Табуляция (TSV)</option>
          </select>
        </div>
        ${step.is_sink ? '' : `
        <div class="form-group" style="margin:0">
          <label>Первая строка</label>
          <select onchange="pbUpdateConnConfig(${idx},'header',this.value)">
            <option value="true"  ${hdr==='true' ?'selected':''}>Заголовок</option>
            <option value="false" ${hdr==='false'?'selected':''}>Данные</option>
          </select>
        </div>`}
      </div>
      ${step.is_sink ? '' : `
      <div class="conn-actions" style="display:flex;gap:.5rem;margin-top:.7rem;flex-wrap:wrap">
        <button id="pb-dbtest-${idx}" type="button" class="btn btn-primary btn-sm"
                style="white-space:nowrap;min-width:150px;justify-content:center${step._dbok ? ';background:var(--green);border-color:var(--green);color:#fff' : ''}"
                onclick="event.stopPropagation();pbTestConnection(${idx})">${step._dbok ? '✓ Файл доступен' : 'Подключиться'}</button>
        <button type="button" class="btn btn-sm" style="min-width:150px;justify-content:center"
                onclick="event.stopPropagation();pbPreviewSource(${idx})">▶ Показать данные</button>
      </div>
      <div id="pb-dbpreview-${idx}" style="margin-top:.5rem"></div>`}
    </div>`;
  }

  if (type === 'parquet') return `
    <div class="conn-group">
      <div class="conn-group-title">Parquet-файл</div>
      <div class="form-group" style="margin:0">
        <label>Путь или glob-паттерн</label>
        <input type="text" value="${escAttr(cfg.path || '')}"
               oninput="pbUpdateConnConfig(${idx},'path',this.value)"
               placeholder="/data/exports/*.parquet">
      </div>
      <div class="conn-actions" style="display:flex;gap:.5rem;margin-top:.7rem;flex-wrap:wrap">
        <button id="pb-dbtest-${idx}" type="button" class="btn btn-primary btn-sm"
                style="white-space:nowrap;min-width:150px;justify-content:center${step._dbok ? ';background:var(--green);border-color:var(--green);color:#fff' : ''}"
                onclick="event.stopPropagation();pbTestConnection(${idx})">${step._dbok ? '✓ Файл доступен' : 'Подключиться'}</button>
        <button type="button" class="btn btn-sm" style="min-width:150px;justify-content:center"
                onclick="event.stopPropagation();pbPreviewSource(${idx})">▶ Показать данные</button>
      </div>
      <div id="pb-dbpreview-${idx}" style="margin-top:.5rem"></div>
    </div>`;

  if (type === 'json_http') {
    const method = cfg.method || 'GET';
    const auth = cfg.auth_type || 'none';
    const ptype = cfg.page_type || 'offset';
    const reRender = `pbRerenderStepCfg(${idx})`;
    /* Только активные поля → раскладываем в 2 колонки (grid), без пустых ячеек;
       при нечётном числе последнее поле растягивается на всю ширину. */
    const fields = [];
    fields.push(`<label>Адрес (URL)</label>
      <input type="text" value="${escAttr(cfg.url || '')}"
             oninput="pbUpdateConnConfig(${idx},'url',this.value)" placeholder="https://api.example.com/data">`);
    if (!step.is_sink) fields.push(`<label>Метод</label>
      <select onchange="pbUpdateConnConfig(${idx},'method',this.value);${reRender}">
        <option value="GET"  ${method==='GET' ?'selected':''}>GET</option>
        <option value="POST" ${method==='POST'?'selected':''}>POST</option>
      </select>`);
    fields.push(`<label>Авторизация</label>
      <select onchange="pbUpdateConnConfig(${idx},'auth_type',this.value);${reRender}">
        <option value="none"    ${auth==='none'   ?'selected':''}>Нет</option>
        <option value="bearer"  ${auth==='bearer' ?'selected':''}>Bearer-токен</option>
        <option value="api_key" ${auth==='api_key'?'selected':''}>API-ключ</option>
      </select>`);
    if (auth !== 'none') fields.push(`<label>${auth==='bearer' ? 'Токен' : 'Значение ключа'}</label>
      <input type="text" value="${escAttr(cfg.auth_token || '')}"
             oninput="pbUpdateConnConfig(${idx},'auth_token',this.value)" placeholder="sk-...">`);
    if (auth === 'api_key') fields.push(`<label>Имя заголовка</label>
      <input type="text" value="${escAttr(cfg.auth_header || '')}"
             oninput="pbUpdateConnConfig(${idx},'auth_header',this.value)" placeholder="X-API-Key">`);
    if (!step.is_sink) {   /* источниковые поля: куда писать данные, не нужны приёмнику */
      fields.push(`<label>Путь к массиву в ответе</label>
        <input type="text" value="${escAttr(cfg.data_path || '')}"
               oninput="pbUpdateConnConfig(${idx},'data_path',this.value)" placeholder="data.items">`);
      fields.push(`<label>Параметр пагинации</label>
        <input type="text" value="${escAttr(cfg.page_param || '')}"
               oninput="pbUpdateConnConfig(${idx},'page_param',this.value)" placeholder="пусто = без пагинации">`);
      fields.push(`<label>Тип пагинации</label>
        <select onchange="pbUpdateConnConfig(${idx},'page_type',this.value);${reRender}">
          <option value="offset" ${ptype==='offset'?'selected':''}>По смещению (offset)</option>
          <option value="cursor" ${ptype==='cursor'?'selected':''}>По курсору</option>
        </select>`);
      fields.push(`<label>Размер страницы</label>
        <input type="number" min="1" value="${escAttr(cfg.page_size || 100)}"
               oninput="pbUpdateConnConfig(${idx},'page_size',parseInt(this.value,10)||100)" placeholder="100">`);
      if (ptype !== 'cursor') fields.push(`<label>Поле общего числа</label>
        <input type="text" value="${escAttr(cfg.total_field || '')}"
               oninput="pbUpdateConnConfig(${idx},'total_field',this.value)" placeholder="total">`);
    }
    const grid = fields.map((f, i) =>
      `<div class="form-group" style="margin:0${(i === fields.length - 1 && fields.length % 2 === 1) ? ';grid-column:1/-1' : ''}">${f}</div>`
    ).join('');
    return `
    <div class="conn-group">
      <div class="conn-group-title">Подключение к REST API</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:.6rem 1rem;align-items:start">${grid}</div>
      ${(!step.is_sink && method==='POST') ? `
      <div class="form-group" style="margin:.5rem 0 0">
        <label>Тело запроса (JSON)</label>
        <textarea class="mono-textarea" rows="3" placeholder='{"query":"..."}'
                  oninput="pbUpdateConnConfig(${idx},'post_body',this.value)">${escHtml(cfg.post_body || '')}</textarea>
      </div>` : ''}
      ${step.is_sink ? '' : `
      <div class="conn-actions" style="display:flex;gap:.5rem;margin-top:.7rem;flex-wrap:wrap">
        <button id="pb-dbtest-${idx}" type="button" class="btn btn-primary btn-sm"
                style="white-space:nowrap;min-width:150px;justify-content:center${step._dbok ? ';background:var(--green);border-color:var(--green);color:#fff' : ''}"
                onclick="event.stopPropagation();pbTestConnection(${idx})">${step._dbok ? '✓ Подключено' : 'Подключиться'}</button>
        <button type="button" class="btn btn-sm" style="min-width:150px;justify-content:center"
                onclick="event.stopPropagation();pbPreviewSource(${idx})">▶ Показать данные</button>
      </div>
      <div id="pb-dbpreview-${idx}" style="margin-top:.5rem"></div>`}
    </div>`;
  }

  if (type === 'siebel') {
    /* «Размер страницы» и кнопки предпросмотра — только для источника. */
    const srcExtra = step.is_sink ? '' : `
        <div class="form-group" style="margin:0">
          <label>Размер страницы</label>
          <input type="number" min="1" value="${escAttr(cfg.page_size || 1000)}"
                 oninput="pbUpdateConnConfig(${idx},'page_size',parseInt(this.value,10)||1000)">
        </div>`;
    const actions = step.is_sink ? '' : `
      <div class="conn-actions" style="display:flex;gap:.5rem;margin-top:.7rem;flex-wrap:wrap">
        <button id="pb-dbtest-${idx}" type="button" class="btn btn-primary btn-sm"
                style="white-space:nowrap;min-width:150px;justify-content:center${step._dbok ? ';background:var(--green);border-color:var(--green);color:#fff' : ''}"
                onclick="event.stopPropagation();pbTestConnection(${idx})">${step._dbok ? '✓ Подключено' : 'Подключиться'}</button>
        <button type="button" class="btn btn-sm" style="min-width:150px;justify-content:center"
                onclick="event.stopPropagation();pbPreviewSource(${idx})">▶ Показать данные</button>
      </div>
      <div id="pb-dbpreview-${idx}" style="margin-top:.5rem"></div>`;
    return `
    <div class="conn-group">
      <div class="conn-group-title">${step.is_sink ? 'Приёмник' : 'Источник'} Siebel</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:.6rem 1rem;align-items:start">
        <div class="form-group" style="margin:0;grid-column:1/-1">
          <label>Адрес сервиса Siebel</label>
          <input type="text" value="${escAttr(cfg.url || '')}" oninput="pbUpdateConnConfig(${idx},'url',this.value)">
        </div>
        <div class="form-group" style="margin:0">
          <label>Объект интеграции</label>
          <input type="text" value="${escAttr(cfg.io_name || '')}" oninput="pbUpdateConnConfig(${idx},'io_name',this.value)">
        </div>
        <div class="form-group" style="margin:0">
          <label>Пользователь</label>
          <input type="text" value="${escAttr(cfg.user || '')}" oninput="pbUpdateConnConfig(${idx},'user',this.value)">
        </div>
        <div class="form-group" style="margin:0">
          <label>Пароль</label>
          <input type="password" value="${escAttr(cfg.password || '')}" oninput="pbUpdateConnConfig(${idx},'password',this.value)">
        </div>
        ${srcExtra}
      </div>
      ${actions}
    </div>`;
  }

  if (type === 'xml') {
    const auth   = cfg.auth_type || 'none';
    const method = cfg.method || 'GET';
    const reR = `pbRerenderStepCfg(${idx})`;
    const f = [];
    f.push(`<label>Веб-адрес</label>
      <input type="text" value="${escAttr(cfg.url||'')}" oninput="pbUpdateConnConfig(${idx},'url',this.value)">`);
    f.push(`<label>Путь к файлу</label>
      <input type="text" value="${escAttr(cfg.path||'')}" oninput="pbUpdateConnConfig(${idx},'path',this.value)">`);
    f.push(`<label>Тег одной записи</label>
      <input type="text" value="${escAttr(cfg.row_tag||'')}" oninput="pbUpdateConnConfig(${idx},'row_tag',this.value)">`);
    if (!step.is_sink) {
      f.push(`<label>Метод запроса</label>
        <select onchange="pbUpdateConnConfig(${idx},'method',this.value);${reR}">
          <option value="GET"  ${method==='GET' ?'selected':''}>GET</option>
          <option value="POST" ${method==='POST'?'selected':''}>POST</option>
        </select>`);
      f.push(`<label>Параметр постраничной загрузки</label>
        <input type="text" value="${escAttr(cfg.page_param||'')}" oninput="pbUpdateConnConfig(${idx},'page_param',this.value)">`);
    } else {
      f.push(`<label>Корневой тег документа</label>
        <input type="text" value="${escAttr(cfg.root_tag||'rows')}" oninput="pbUpdateConnConfig(${idx},'root_tag',this.value)">`);
    }
    f.push(`<label>Авторизация</label>
      <select onchange="pbUpdateConnConfig(${idx},'auth_type',this.value);${reR}">
        <option value="none"   ${auth==='none'  ?'selected':''}>Нет</option>
        <option value="basic"  ${auth==='basic' ?'selected':''}>Basic</option>
        <option value="bearer" ${auth==='bearer'?'selected':''}>Bearer-токен</option>
      </select>`);
    if (auth==='basic') {
      f.push(`<label>Пользователь</label><input type="text" value="${escAttr(cfg.user||'')}" oninput="pbUpdateConnConfig(${idx},'user',this.value)">`);
      f.push(`<label>Пароль</label><input type="password" value="${escAttr(cfg.password||'')}" oninput="pbUpdateConnConfig(${idx},'password',this.value)">`);
    } else if (auth==='bearer') {
      f.push(`<label>Токен</label><input type="text" value="${escAttr(cfg.auth_token||'')}" oninput="pbUpdateConnConfig(${idx},'auth_token',this.value)">`);
    }
    const grid = f.map((x,i)=>`<div class="form-group" style="margin:0${(i===f.length-1&&f.length%2===1)?';grid-column:1/-1':''}">${x}</div>`).join('');
    const postBody = (!step.is_sink && method==='POST') ? `
      <div class="form-group" style="margin:.5rem 0 0"><label>Тело запроса</label>
        <textarea class="mono-textarea" rows="3" oninput="pbUpdateConnConfig(${idx},'post_body',this.value)">${escHtml(cfg.post_body||'')}</textarea></div>` : '';
    const actions = step.is_sink ? '' : `
      <div class="conn-actions" style="display:flex;gap:.5rem;margin-top:.7rem;flex-wrap:wrap">
        <button id="pb-dbtest-${idx}" type="button" class="btn btn-primary btn-sm"
                style="white-space:nowrap;min-width:150px;justify-content:center${step._dbok?';background:var(--green);border-color:var(--green);color:#fff':''}"
                onclick="event.stopPropagation();pbTestConnection(${idx})">${step._dbok?'✓ Подключено':'Подключиться'}</button>
        <button type="button" class="btn btn-sm" style="min-width:150px;justify-content:center"
                onclick="event.stopPropagation();pbPreviewSource(${idx})">▶ Показать данные</button>
      </div><div id="pb-dbpreview-${idx}" style="margin-top:.5rem"></div>`;
    return `
    <div class="conn-group">
      <div class="conn-group-title">${step.is_sink?'Приёмник':'Источник'} XML</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:.6rem 1rem;align-items:start">${grid}</div>
      ${postBody}
      ${actions}
    </div>`;
  }

  if (type === 'soap') {
    const auth = cfg.auth_type || 'none';
    const ver  = cfg.soap_version || '1.1';
    const reR = `pbRerenderStepCfg(${idx})`;
    const f = [];
    f.push(`<label>Адрес сервиса</label>
      <input type="text" value="${escAttr(cfg.url||'')}" oninput="pbUpdateConnConfig(${idx},'url',this.value)">`);
    f.push(`<label>Действие SOAP</label>
      <input type="text" value="${escAttr(cfg.soap_action||'')}" oninput="pbUpdateConnConfig(${idx},'soap_action',this.value)">`);
    f.push(`<label>Версия SOAP</label>
      <select onchange="pbUpdateConnConfig(${idx},'soap_version',this.value)">
        <option value="1.1" ${ver==='1.1'?'selected':''}>1.1</option>
        <option value="1.2" ${ver==='1.2'?'selected':''}>1.2</option>
      </select>`);
    if (step.is_sink) {
      f.push(`<label>Имя операции</label>
        <input type="text" value="${escAttr(cfg.operation||'')}" oninput="pbUpdateConnConfig(${idx},'operation',this.value)">`);
      f.push(`<label>Пространство имён</label>
        <input type="text" value="${escAttr(cfg.namespace||'')}" oninput="pbUpdateConnConfig(${idx},'namespace',this.value)">`);
      f.push(`<label>Тег одной строки</label>
        <input type="text" value="${escAttr(cfg.sink_row_tag||'row')}" oninput="pbUpdateConnConfig(${idx},'sink_row_tag',this.value)">`);
    } else {
      f.push(`<label>Тег записи в ответе</label>
        <input type="text" value="${escAttr(cfg.row_tag||'')}" oninput="pbUpdateConnConfig(${idx},'row_tag',this.value)">`);
    }
    f.push(`<label>Авторизация</label>
      <select onchange="pbUpdateConnConfig(${idx},'auth_type',this.value);${reR}">
        <option value="none"   ${auth==='none'  ?'selected':''}>Нет</option>
        <option value="basic"  ${auth==='basic' ?'selected':''}>Basic</option>
        <option value="bearer" ${auth==='bearer'?'selected':''}>Bearer-токен</option>
      </select>`);
    if (auth==='basic') {
      f.push(`<label>Пользователь</label><input type="text" value="${escAttr(cfg.user||'')}" oninput="pbUpdateConnConfig(${idx},'user',this.value)">`);
      f.push(`<label>Пароль</label><input type="password" value="${escAttr(cfg.password||'')}" oninput="pbUpdateConnConfig(${idx},'password',this.value)">`);
    } else if (auth==='bearer') {
      f.push(`<label>Токен</label><input type="text" value="${escAttr(cfg.auth_token||'')}" oninput="pbUpdateConnConfig(${idx},'auth_token',this.value)">`);
    }
    const grid = f.map((x,i)=>`<div class="form-group" style="margin:0${(i===f.length-1&&f.length%2===1)?';grid-column:1/-1':''}">${x}</div>`).join('');
    const tpl = step.is_sink ? '' : `
      <div class="form-group" style="margin:.5rem 0 0"><label>Шаблон SOAP-запроса</label>
        <textarea class="mono-textarea" rows="5"
                  oninput="pbUpdateConnConfig(${idx},'request_template',this.value)">${escHtml(cfg.request_template||'')}</textarea></div>`;
    const actions = step.is_sink ? '' : `
      <div class="conn-actions" style="display:flex;gap:.5rem;margin-top:.7rem;flex-wrap:wrap">
        <button id="pb-dbtest-${idx}" type="button" class="btn btn-primary btn-sm"
                style="white-space:nowrap;min-width:150px;justify-content:center${step._dbok?';background:var(--green);border-color:var(--green);color:#fff':''}"
                onclick="event.stopPropagation();pbTestConnection(${idx})">${step._dbok?'✓ Подключено':'Подключиться'}</button>
        <button type="button" class="btn btn-sm" style="min-width:150px;justify-content:center"
                onclick="event.stopPropagation();pbPreviewSource(${idx})">▶ Показать данные</button>
      </div><div id="pb-dbpreview-${idx}" style="margin-top:.5rem"></div>`;
    return `
    <div class="conn-group">
      <div class="conn-group-title">${step.is_sink?'Приёмник':'Источник'} SOAP</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:.6rem 1rem;align-items:start">${grid}</div>
      ${tpl}
      ${actions}
    </div>`;
  }

  if (type === 'postgresql') return `
    <div class="conn-group">
      <div class="conn-group-title">Подключение к PostgreSQL</div>
      <div style="display:flex;gap:.75rem 1rem;align-items:flex-start;flex-wrap:wrap">
        <div style="flex:1 1 130px;display:flex;flex-direction:column;gap:.6rem">
          <div class="form-group" style="margin:0">
            <label>База данных</label>
            <input type="text" value="${escAttr(cfg.dbname || '')}"
                   oninput="pbUpdateConnConfig(${idx},'dbname',this.value)" placeholder="postgres">
          </div>
          <div class="form-group" style="margin:0">
            <label>Хост</label>
            <input type="text" value="${escAttr(cfg.host || '')}"
                   oninput="pbUpdateConnConfig(${idx},'host',this.value)" placeholder="localhost">
          </div>
          <div class="form-group" style="margin:0;max-width:90px">
            <label>Порт</label>
            <input type="text" value="${escAttr(cfg.port || '5432')}"
                   oninput="pbUpdateConnConfig(${idx},'port',this.value)" placeholder="5432">
          </div>
        </div>
        <div style="flex:1 1 130px;display:flex;flex-direction:column;gap:.6rem;align-self:stretch">
          <div class="form-group" style="margin:0">
            <label>Пользователь</label>
            <input type="text" value="${escAttr(cfg.user || '')}"
                   oninput="pbUpdateConnConfig(${idx},'user',this.value)" placeholder="postgres">
          </div>
          <div class="form-group" style="margin:0;width:50%;align-self:flex-start">
            <label>Пароль</label>
            <input type="password" value="${escAttr(cfg.password || '')}"
                   oninput="pbUpdateConnConfig(${idx},'password',this.value)" placeholder="">
          </div>
        </div>
      </div>
      ${/* Кнопка вынесена из колонки с паролем в собственный ряд: внутри колонки
           она прижималась к её правому краю и висела отдельно от всего остального.
           Ряд такой же, как у прочих типов, — по левому краю формы. */''}
      <div class="conn-actions" style="display:flex;gap:.5rem;margin-top:.7rem;flex-wrap:wrap">
        <button id="pb-dbtest-${idx}" type="button" class="btn btn-primary btn-sm"
                style="white-space:nowrap;min-width:150px;justify-content:center${step._dbok ? ';background:var(--green);border-color:var(--green);color:#fff' : ''}"
                onclick="event.stopPropagation();pbTestConnection(${idx})">${step._dbok ? '✓ Подключено' : 'Подключиться'}</button>
      </div>
      ${step.is_sink ? '' : `
      <div class="step-row-2" style="margin-top:.6rem;align-items:center">
        <div class="form-group" style="margin:0">
          <label>Режим чтения</label>
          <select onchange="pbUpdateConnConfig(${idx},'read_mode',this.value);pbRerenderStepCfg(${idx})">
            <option value="full"   ${(cfg.read_mode||'full')==='full' ?'selected':''}>Все строки — каждый раз заново</option>
            <option value="cursor" ${cfg.read_mode==='cursor'         ?'selected':''}>Только новые строки — по полю-отметке</option>
            <option value="cdc"    ${cfg.read_mode==='cdc'            ?'selected':''}>Изменения из журнала БД (CDC)</option>
          </select>
        </div>
        ${cfg.read_mode==='cursor' ? `
        <div class="form-group" style="margin:0;flex:2">
          <label>Поле-отметка <span class="label-hint">монотонное: id / updated_at</span></label>
          <input type="text" value="${escAttr(cfg.cursor_column || '')}"
                 oninput="pbUpdateConnConfig(${idx},'cursor_column',this.value)" placeholder="updated_at">
        </div>` : ''}
        ${cfg.read_mode==='cdc' ? `
        <div class="form-group" style="margin:0;flex:2">
          <label>Слот репликации</label>
          <input type="text" value="${escAttr(cfg.cdc_slot || '')}"
                 oninput="pbUpdateConnConfig(${idx},'cdc_slot',this.value)" placeholder="dfo_cdc">
        </div>` : ''}
      </div>
      <div class="form-group" style="margin:.6rem 0 0">
        <label>Запрос к источнику</label>
        <textarea class="mono-textarea" rows="3"
                  placeholder="orders   или   SELECT * FROM orders WHERE active = true"
                  oninput="pbUpdateConnConfig(${idx},'table',this.value)">${escHtml(cfg.query || cfg.table || '')}</textarea>
        <div class="conn-actions" style="display:flex;margin-top:.4rem">
          <button type="button" class="btn btn-sm" style="min-width:150px;justify-content:center"
                  onclick="event.stopPropagation();pbPreviewSource(${idx})">▶ Выполнить</button>
        </div>
      </div>
      <div id="pb-dbpreview-${idx}" style="margin-top:.75rem"></div>`}
    </div>`;

  if (type === 'greenplum') return `
    <div class="conn-group">
      <div class="conn-group-title">Подключение к Greenplum</div>
      <div style="display:flex;gap:.75rem 1rem;align-items:flex-start;flex-wrap:wrap">
        <div style="flex:1 1 130px;display:flex;flex-direction:column;gap:.6rem">
          <div class="form-group" style="margin:0">
            <label>База данных</label>
            <input type="text" value="${escAttr(cfg.dbname || '')}"
                   oninput="pbUpdateConnConfig(${idx},'dbname',this.value)" placeholder="postgres">
          </div>
          <div class="form-group" style="margin:0">
            <label>Хост</label>
            <input type="text" value="${escAttr(cfg.host || '')}"
                   oninput="pbUpdateConnConfig(${idx},'host',this.value)" placeholder="localhost">
          </div>
          <div class="form-group" style="margin:0;max-width:90px">
            <label>Порт</label>
            <input type="text" value="${escAttr(cfg.port || '5432')}"
                   oninput="pbUpdateConnConfig(${idx},'port',this.value)" placeholder="5432">
          </div>
        </div>
        <div style="flex:1 1 130px;display:flex;flex-direction:column;gap:.6rem;align-self:stretch">
          <div class="form-group" style="margin:0">
            <label>Пользователь</label>
            <input type="text" value="${escAttr(cfg.user || '')}"
                   oninput="pbUpdateConnConfig(${idx},'user',this.value)" placeholder="postgres">
          </div>
          <div class="form-group" style="margin:0">
            <label>Схема</label>
            <input type="text" value="${escAttr(cfg.schema || '')}"
                   oninput="pbUpdateConnConfig(${idx},'schema',this.value)" placeholder="public">
          </div>
          <div class="form-group" style="margin:0;width:50%;align-self:flex-start">
            <label>Пароль</label>
            <input type="password" value="${escAttr(cfg.password || '')}"
                   oninput="pbUpdateConnConfig(${idx},'password',this.value)" placeholder="">
          </div>
        </div>
      </div>
      ${/* Кнопка вынесена из колонки с паролем в собственный ряд: внутри колонки
           она прижималась к её правому краю и висела отдельно от всего остального.
           Ряд такой же, как у прочих типов, — по левому краю формы. */''}
      <div class="conn-actions" style="display:flex;gap:.5rem;margin-top:.7rem;flex-wrap:wrap">
        <button id="pb-dbtest-${idx}" type="button" class="btn btn-primary btn-sm"
                style="white-space:nowrap;min-width:150px;justify-content:center${step._dbok ? ';background:var(--green);border-color:var(--green);color:#fff' : ''}"
                onclick="event.stopPropagation();pbTestConnection(${idx})">${step._dbok ? '✓ Подключено' : 'Подключиться'}</button>
      </div>
      ${step.is_sink ? '' : `
      <div class="step-row-2" style="margin-top:.6rem;align-items:center">
        <div class="form-group" style="margin:0">
          <label>Режим чтения</label>
          <select onchange="pbUpdateConnConfig(${idx},'read_mode',this.value);if(this.value!=='cursor')pbUpdateConnConfig(${idx},'cursor_column','');pbRerenderStepCfg(${idx})">
            <option value="full"   ${(cfg.read_mode||'full')==='full' ?'selected':''}>Все строки — каждый раз заново</option>
            <option value="cursor" ${cfg.read_mode==='cursor'         ?'selected':''}>Только новые строки — по полю-отметке</option>
          </select>
        </div>
        ${cfg.read_mode==='cursor' ? `
        <div class="form-group" style="margin:0;flex:2">
          <label>Поле-отметка</label>
          <input type="text" value="${escAttr(cfg.cursor_column || '')}"
                 oninput="pbUpdateConnConfig(${idx},'cursor_column',this.value)" placeholder="last_upd_ts">
        </div>` : ''}
      </div>
      <div class="form-group" style="margin:.6rem 0 0">
        <label>Запрос к источнику</label>
        <textarea class="mono-textarea" rows="3"
                  placeholder="orders   или   SELECT * FROM orders WHERE active = true"
                  oninput="pbUpdateConnConfig(${idx},'table',this.value)">${escHtml(cfg.query || cfg.table || '')}</textarea>
        <div class="conn-actions" style="display:flex;margin-top:.4rem">
          <button type="button" class="btn btn-sm" style="min-width:150px;justify-content:center"
                  onclick="event.stopPropagation();pbPreviewSource(${idx})">▶ Выполнить</button>
        </div>
      </div>
      <div id="pb-dbpreview-${idx}" style="margin-top:.75rem"></div>`}
    </div>`;

  if (type === 'oracle') return `
    <div class="conn-group">
      <div class="conn-group-title">Подключение к Oracle</div>
      <div style="display:flex;gap:.75rem 1rem;align-items:flex-start;flex-wrap:wrap">
        <div style="flex:1 1 130px;display:flex;flex-direction:column;gap:.6rem">
          <div class="form-group" style="margin:0">
            <label>Имя сервиса</label>
            <input type="text" value="${escAttr(cfg.service_name || '')}"
                   oninput="pbUpdateConnConfig(${idx},'service_name',this.value)" placeholder="ORCL">
          </div>
          <div class="form-group" style="margin:0">
            <label>Хост</label>
            <input type="text" value="${escAttr(cfg.host || '')}"
                   oninput="pbUpdateConnConfig(${idx},'host',this.value)" placeholder="localhost">
          </div>
          <div class="form-group" style="margin:0;max-width:90px">
            <label>Порт</label>
            <input type="text" value="${escAttr(cfg.port || '1521')}"
                   oninput="pbUpdateConnConfig(${idx},'port',this.value)" placeholder="1521">
          </div>
        </div>
        <div style="flex:1 1 130px;display:flex;flex-direction:column;gap:.6rem;align-self:stretch">
          <div class="form-group" style="margin:0">
            <label>Пользователь</label>
            <input type="text" value="${escAttr(cfg.user || '')}"
                   oninput="pbUpdateConnConfig(${idx},'user',this.value)" placeholder="oracle">
          </div>
          <div class="form-group" style="margin:0">
            <label>Схема</label>
            <input type="text" value="${escAttr(cfg.schema || '')}"
                   oninput="pbUpdateConnConfig(${idx},'schema',this.value)" placeholder="TSMDM_IN">
          </div>
          <div class="form-group" style="margin:0;width:50%;align-self:flex-start">
            <label>Пароль</label>
            <input type="password" value="${escAttr(cfg.password || '')}"
                   oninput="pbUpdateConnConfig(${idx},'password',this.value)" placeholder="">
          </div>
        </div>
      </div>
      ${/* Кнопка вынесена из колонки с паролем в собственный ряд: внутри колонки
           она прижималась к её правому краю и висела отдельно от всего остального.
           Ряд такой же, как у прочих типов, — по левому краю формы. */''}
      <div class="conn-actions" style="display:flex;gap:.5rem;margin-top:.7rem;flex-wrap:wrap">
        <button id="pb-dbtest-${idx}" type="button" class="btn btn-primary btn-sm"
                style="white-space:nowrap;min-width:150px;justify-content:center${step._dbok ? ';background:var(--green);border-color:var(--green);color:#fff' : ''}"
                onclick="event.stopPropagation();pbTestConnection(${idx})">${step._dbok ? '✓ Подключено' : 'Подключиться'}</button>
      </div>
      ${step.is_sink ? '' : `
      <div class="step-row-2" style="margin-top:.6rem;align-items:center">
        <div class="form-group" style="margin:0">
          <label>Режим чтения</label>
          <select onchange="pbUpdateConnConfig(${idx},'read_mode',this.value);if(this.value!=='cursor')pbUpdateConnConfig(${idx},'cursor_column','');pbRerenderStepCfg(${idx})">
            <option value="full"   ${(cfg.read_mode||'full')==='full' ?'selected':''}>Все строки — каждый раз заново</option>
            <option value="cursor" ${cfg.read_mode==='cursor'         ?'selected':''}>Только новые строки — по полю-отметке</option>
          </select>
        </div>
        ${cfg.read_mode==='cursor' ? `
        <div class="form-group" style="margin:0;flex:2">
          <label>Поле-отметка</label>
          <input type="text" value="${escAttr(cfg.cursor_column || '')}"
                 oninput="pbUpdateConnConfig(${idx},'cursor_column',this.value)" placeholder="LAST_UPD">
        </div>` : ''}
      </div>
      <div class="form-group" style="margin:.6rem 0 0">
        <label>Запрос к источнику</label>
        <textarea class="mono-textarea" rows="3"
                  placeholder="ORDERS   или   SELECT * FROM orders WHERE active = 1"
                  oninput="pbUpdateConnConfig(${idx},'table',this.value)">${escHtml(cfg.query || cfg.table || '')}</textarea>
        <div class="conn-actions" style="display:flex;margin-top:.4rem">
          <button type="button" class="btn btn-sm" style="min-width:150px;justify-content:center"
                  onclick="event.stopPropagation();pbPreviewSource(${idx})">▶ Выполнить</button>
        </div>
      </div>
      <div id="pb-dbpreview-${idx}" style="margin-top:.75rem"></div>`}
    </div>`;

  if (type === 'kafka') {
    const fmt = cfg.data_format || 'json';
    const sec = cfg.security_protocol || '';
    const needsSasl = sec.indexOf('SASL') === 0;
    /* SASL выбран, но механизм в конфиге пуст (дропдаун не меняли) → бэкенд
     * получил бы пустой sasl.mechanism, а librdkafka по умолчанию берёт GSSAPI
     * (Kerberos) и падает на keytab. Проставляем дефолт PLAIN сразу при
     * рендере, чтобы отправляемый конфиг всегда был явным. */
    if (needsSasl && !cfg.sasl_mechanism) pbUpdateConnConfig(idx, 'sasl_mechanism', 'PLAIN');
    const mech = cfg.sasl_mechanism || 'PLAIN';
    const isKerberos = mech === 'GSSAPI';   /* Kerberos: keytab/principal вместо логина/пароля */
    const needsTls  = sec === 'SSL' || sec === 'SASL_SSL';
    const off = cfg.offset_reset || 'earliest';
    const iso = cfg.isolation_level || 'read_uncommitted';
    const osm = cfg.offset_start_mode || 'logical';
    return `
    <div class="conn-group">
      <div class="conn-group-title">Подключение к Kafka</div>
      <div class="conn-grid">
        <div class="form-group" style="margin:0">
          <label>Брокеры</label>
          <input type="text" value="${escAttr(cfg.brokers || '')}" placeholder="localhost:9092"
                 oninput="pbUpdateConnConfig(${idx},'brokers',this.value)">
        </div>
        ${step.is_sink ? '' : `
        <div class="form-group" style="margin:0">
          <label>Группа консьюмера</label>
          <input type="text" value="${escAttr(cfg.group_id || '')}" placeholder="dfo_consumer"
                 oninput="pbUpdateConnConfig(${idx},'group_id',this.value)">
        </div>`}
        <div class="form-group" style="margin:0">
          <label>Топик${step.is_sink ? ' (назначение)' : ''}</label>
          <input type="text" value="${escAttr(cfg.topic || '')}" placeholder="orders"
                 oninput="pbUpdateConnConfig(${idx},'topic',this.value)">
        </div>
        <div class="form-group" style="margin:0">
          <label>Защита соединения</label>
          <select onchange="pbSetKafkaSecurity(${idx},this.value)">
            <option value=""               ${sec===''              ?'selected':''}>Без шифрования и авторизации (PLAINTEXT)</option>
            <option value="SASL_SSL"       ${sec==='SASL_SSL'       ?'selected':''}>SASL + TLS</option>
            <option value="SASL_PLAINTEXT" ${sec==='SASL_PLAINTEXT' ?'selected':''}>SASL без TLS</option>
            <option value="SSL"            ${sec==='SSL'            ?'selected':''}>Только TLS</option>
          </select>
        </div>
      </div>
      ${sec ? '' : `
      <div style="font-size:.74rem;color:var(--muted);border-left:2px solid var(--border);padding:.3rem .5rem;margin:.5rem 0 0">
        Выберите «Защиту соединения» — появятся поля аутентификации (логин, пароль,
        механизм SASL) и сертификатов (truststore, keystore). Сейчас подключение
        настроено без шифрования и авторизации.
      </div>`}

      <div class="conn-grid">
        <div class="form-group" style="margin:0">
          <label>Формат сообщений</label>
          <select onchange="pbUpdateConnConfig(${idx},'data_format',this.value);pbRerenderStepCfg(${idx})">
            <option value="json" ${fmt==='json'?'selected':''}>JSON</option>
            <option value="csv"  ${fmt==='csv' ?'selected':''}>CSV</option>
            <option value="avro" ${fmt==='avro'?'selected':''}>Avro (реестр схем)</option>
          </select>
        </div>
        ${step.is_sink ? '' : `
        <div class="form-group" style="margin:0">
          <label>С какого места читать</label>
          <select onchange="pbUpdateConnConfig(${idx},'offset_reset',this.value)">
            <option value="earliest" ${off==='earliest'?'selected':''}>С начала топика</option>
            <option value="latest"   ${off==='latest'  ?'selected':''}>Только новые сообщения</option>
          </select>
        </div>
        <div class="form-group" style="margin:0">
          <label>Режим чтения (isolation)</label>
          <select onchange="pbUpdateConnConfig(${idx},'isolation_level',this.value)">
            <option value="read_uncommitted" ${iso==='read_uncommitted'?'selected':''}>read_uncommitted (совместимо со всеми брокерами)</option>
            <option value="read_committed"   ${iso==='read_committed'  ?'selected':''}>read_committed (только транзакционные источники, Kafka 0.11+)</option>
          </select>
        </div>
        <div class="form-group" style="margin:0">
          <label>Стартовая позиция (совместимость)</label>
          ${(() => {
            const isNumeric = /^\d+$/.test(osm);
            const sel = isNumeric ? 'numeric' : osm;
            return `
          <select onchange="pbSetKafkaOffsetStartMode(${idx},this.value)">
            <option value="logical"  ${sel==='logical' ?'selected':''}>logical (обычные брокеры — резолв через ListOffsets)</option>
            <option value="absolute" ${sel==='absolute'?'selected':''}>absolute (брокер/прокси без ListOffsets — чтение с offset 0, автопереход на latest при OFFSET_OUT_OF_RANGE)</option>
            <option value="numeric"  ${sel==='numeric' ?'selected':''}>точный offset вручную (ни earliest, ни latest не резолвятся — offset берётся у владельца брокера)</option>
          </select>
          ${isNumeric ? `
          <input type="number" min="0" step="1" value="${escAttr(osm)}" style="margin-top:.35rem"
                 placeholder="например 15230"
                 oninput="pbUpdateConnConfig(${idx},'offset_start_mode',this.value)">` : ''}`;
          })()}
        </div>`}
      </div>
      ${needsSasl ? `
      <div style="font-size:.7rem;font-weight:600;letter-spacing:.04em;text-transform:uppercase;color:var(--muted);margin:.9rem 0 .1rem">Аутентификация</div>
      <div class="conn-grid">
        <div class="form-group" style="margin:0">
          <label>Механизм SASL</label>
          <select onchange="pbUpdateConnConfig(${idx},'sasl_mechanism',this.value);pbRerenderStepCfg(${idx})">
            <option value="PLAIN"         ${mech==='PLAIN'         ?'selected':''}>PLAIN</option>
            <option value="SCRAM-SHA-256" ${mech==='SCRAM-SHA-256' ?'selected':''}>SCRAM-SHA-256</option>
            <option value="SCRAM-SHA-512" ${mech==='SCRAM-SHA-512' ?'selected':''}>SCRAM-SHA-512</option>
            <option value="GSSAPI"        ${mech==='GSSAPI'        ?'selected':''}>GSSAPI (Kerberos)</option>
          </select>
        </div>
        ${isKerberos ? '' : `
        <div class="form-group" style="margin:0">
          <label>Пользователь</label>
          <input type="text" value="${escAttr(cfg.sasl_username || '')}" placeholder="имя пользователя (Confluent Cloud — API-ключ)"
                 oninput="pbUpdateConnConfig(${idx},'sasl_username',this.value)">
        </div>
        <div class="form-group" style="margin:0">
          <label>Пароль</label>
          <input type="password" value="${escAttr(cfg.sasl_password || '')}" placeholder="" autocomplete="off"
                 oninput="pbUpdateConnConfig(${idx},'sasl_password',this.value)">
        </div>`}
        ${!isKerberos ? '' : `
        <div class="form-group" style="margin:0">
          <label>Kerberos — имя сервиса</label>
          <input type="text" value="${escAttr(cfg.sasl_kerberos_service_name || '')}" placeholder="kafka"
                 oninput="pbUpdateConnConfig(${idx},'sasl_kerberos_service_name',this.value)">
        </div>
        <div class="form-group" style="margin:0">
          <label>Kerberos — принципал</label>
          <input type="text" value="${escAttr(cfg.sasl_kerberos_principal || '')}" placeholder="user@REALM"
                 oninput="pbUpdateConnConfig(${idx},'sasl_kerberos_principal',this.value)">
        </div>
        <div class="form-group" style="margin:0">
          <label>Kerberos — keytab (путь на сервере)</label>
          <input type="text" value="${escAttr(cfg.sasl_kerberos_keytab || '')}" placeholder="/etc/security/dfo.keytab"
                 oninput="pbUpdateConnConfig(${idx},'sasl_kerberos_keytab',this.value)">
        </div>
        <div style="grid-column:1/-1;font-size:.72rem;color:var(--muted)">
          Значения KAFKA_KERBEROS_KEYTAB / _PRINCIPAL / _SERVICE_NAME из окружения
          гейтвея, если заданы, перекрывают указанное здесь.
        </div>`}
      </div>` : ''}
      ${needsTls ? `
      <div style="font-size:.7rem;font-weight:600;letter-spacing:.04em;text-transform:uppercase;color:var(--muted);margin:.9rem 0 .1rem">Сертификаты и доверенные корни</div>
      <div class="conn-grid">
        <div class="form-group" style="margin:0">
          <label>CA‑сертификат (путь на сервере)</label>
          <input type="text" value="${escAttr(cfg.ssl_ca_location || '')}" placeholder="/opt/dataflow-os/certs/ca.pem"
                 oninput="pbUpdateConnConfig(${idx},'ssl_ca_location',this.value)">
        </div>
      </div>
      <div class="conn-grid">
        <div class="form-group" style="margin:0">
          <label>Truststore PKCS#12 (вместо CA‑файла)</label>
          <input type="text" value="${escAttr(cfg.ssl_truststore_location || '')}" placeholder="truststore.p12 / .pfx"
                 oninput="pbUpdateConnConfig(${idx},'ssl_truststore_location',this.value)">
        </div>
        <div class="form-group" style="margin:0">
          <label>Пароль truststore</label>
          <input type="password" value="${escAttr(cfg.ssl_truststore_password || '')}" placeholder="" autocomplete="off"
                 oninput="pbUpdateConnConfig(${idx},'ssl_truststore_password',this.value)">
        </div>
      </div>
      <div class="conn-grid">
        <div class="form-group" style="margin:0">
          <label>Клиентский сертификат (mTLS, необяз.)</label>
          <input type="text" value="${escAttr(cfg.ssl_certificate_location || '')}" placeholder="client.pem / client.p12"
                 oninput="pbUpdateConnConfig(${idx},'ssl_certificate_location',this.value)">
        </div>
        <div class="form-group" style="margin:0">
          <label>Клиентский ключ (для PEM/DER)</label>
          <input type="text" value="${escAttr(cfg.ssl_key_location || '')}" placeholder="client.key — не нужен для PKCS#12"
                 oninput="pbUpdateConnConfig(${idx},'ssl_key_location',this.value)">
        </div>
        <div class="form-group" style="margin:0">
          <label>Пароль ключа / keystore</label>
          <input type="password" value="${escAttr(cfg.ssl_key_password || '')}" placeholder="" autocomplete="off"
                 oninput="pbUpdateConnConfig(${idx},'ssl_key_password',this.value)">
        </div>
      </div>
      <div class="conn-grid">
        <div class="form-group" style="margin:0">
          <label>PKCS#12 keystore (необяз., альтернатива cert+key)</label>
          <input type="text" value="${escAttr(cfg.ssl_keystore_location || '')}" placeholder="client.p12 / client.pfx"
                 oninput="pbUpdateConnConfig(${idx},'ssl_keystore_location',this.value)">
        </div>
      </div>` : ''}
      ${fmt==='avro' ? `
      <div style="font-size:.7rem;font-weight:600;letter-spacing:.04em;text-transform:uppercase;color:var(--muted);margin:.9rem 0 .1rem">Реестр схем (Avro)</div>
      <div class="conn-grid">
        <div class="form-group" style="margin:0">
          <label>Адрес Schema Registry</label>
          <input type="text" value="${escAttr(cfg.schema_registry_url || '')}" placeholder="https://registry:8081"
                 oninput="pbUpdateConnConfig(${idx},'schema_registry_url',this.value)">
        </div>
        <div class="form-group" style="margin:0">
          <label>Авторизация реестра (user:pass)</label>
          <input type="password" value="${escAttr(cfg.schema_registry_auth || '')}" placeholder="user:pass" autocomplete="off"
                 oninput="pbUpdateConnConfig(${idx},'schema_registry_auth',this.value)">
        </div>
      </div>
      <div class="conn-grid">
        <div class="form-group" style="margin:0">
          <label>CA реестра (для private CA, необяз.)</label>
          <input type="text" value="${escAttr(cfg.schema_registry_ca_location || '')}" placeholder="/opt/dataflow-os/certs/registry-ca.pem"
                 oninput="pbUpdateConnConfig(${idx},'schema_registry_ca_location',this.value)">
        </div>
      </div>` : ''}
      ${step.is_sink ? '' : `
      ${/* Редкие служебные параметры кластера — свёрнуты, чтобы не оттеснять
           главное: адрес брокеров и защиту соединения. */''}
      <details style="margin-top:.6rem">
        <summary style="cursor:pointer;font-size:.74rem;color:var(--muted);user-select:none">Дополнительно: сеть и диагностика</summary>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:.6rem 1rem;margin-top:.5rem">
          <div class="form-group" style="margin:0">
            <label>Семейство адресов</label>
            <select onchange="pbUpdateConnConfig(${idx},'broker_address_family',this.value)">
              <option value="v4"  ${(cfg.broker_address_family||'v4')==='v4' ?'selected':''}>IPv4 (по умолчанию)</option>
              <option value="v6"  ${cfg.broker_address_family==='v6' ?'selected':''}>IPv6</option>
              <option value="any" ${cfg.broker_address_family==='any'?'selected':''}>Любое</option>
            </select>
          </div>
          <div class="form-group" style="margin:0">
            <label>Отладка librdkafka</label>
            <input type="text" value="${escAttr(cfg.librdkafka_debug || '')}" placeholder="broker,security — пусто выключает"
                   oninput="pbUpdateConnConfig(${idx},'librdkafka_debug',this.value)">
          </div>
        </div>
      </details>
      ${/* Действия — по левому краю, как и остальное содержимое формы: одинокий
           блок у правого края выбивался из общего выравнивания. Основное
           действие блока доступа — «Подключиться», предпросмотр вторичен. */''}
      <div class="conn-actions" style="display:flex;gap:.5rem;margin-top:.7rem;flex-wrap:wrap">
        <button id="pb-dbtest-${idx}" type="button" class="btn btn-primary btn-sm"
                style="white-space:nowrap;min-width:150px;justify-content:center${step._dbok ? ';background:var(--green);border-color:var(--green);color:#fff' : ''}"
                onclick="event.stopPropagation();pbTestConnection(${idx})">${step._dbok ? '✓ Подключено' : 'Подключиться'}</button>
        <button type="button" class="btn btn-sm" style="min-width:150px;justify-content:center"
                onclick="event.stopPropagation();pbPreviewKafka(${idx})">▶ Показать сообщения</button>
      </div>
      <div id="pb-dbpreview-${idx}" style="margin-top:.5rem"></div>`}
      ${step.is_sink ? `
      <div class="conn-actions" style="display:flex;gap:.5rem;margin-top:.7rem;flex-wrap:wrap">
        <button id="pb-dbtest-${idx}" type="button" class="btn btn-primary btn-sm"
                style="white-space:nowrap;min-width:150px;justify-content:center${step._dbok ? ';background:var(--green);border-color:var(--green);color:#fff' : ''}"
                onclick="event.stopPropagation();pbTestConnection(${idx})">${step._dbok ? '✓ Подключено' : 'Подключиться'}</button>
      </div>` : ''}
    </div>`;
  }

  /* fallback: raw JSON */
  return `
    <div class="form-group" style="margin:0">
      <label>Конфигурация коннектора (JSON)</label>
      <textarea class="mono-textarea" rows="2" placeholder="{}"
                oninput="pbStep(${idx}).connector_config=this.value">${escHtml(step.connector_config || '')}</textarea>
    </div>`;
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
  const webhookOn  = 'all';   /* notifications fire on any status change */

  if (!name) { showToast('Необходимо указать название конвейера', 'error'); return null; }

  const steps = pb.steps.map((s, i) => ({
    ...stepForApi(s),               /* carries scd2_ and any new fields through */
    id:               s.id || `step_${i + 1}`,
    name:             s.name || `Шаг ${i + 1}`,
    connector_type:   s.connector_type   || '',
    connector_config: s.connector_config || '',
    connection_id:    s.connection_id    || '',
    transform_sql:    s.transform_sql    || '',
    target_table:     s.target_table     || '',
    python_code:      s.python_code      || '',
    python_timeout_sec: s.python_timeout_sec || 300,
    scala_code:       s.scala_code       || '',
    scala_timeout_sec: s.scala_timeout_sec || 600,
    max_retries:      maxRetries,      /* one retry policy for the whole pipeline (set in «Запуск») */
    retry_delay_sec:  retryDelay,
    deps:             s.deps             || [],
    ndeps:            (s.deps || []).length,
  }));
  const triggers = (pb.triggers || []).filter(t => t && t.type).map(t => ({...t}));
  const body = { name, cron, enabled, steps, triggers,
                 max_retries: maxRetries,
                 retry_delay_sec: retryDelay,
                 webhook_url: webhookUrl,
                 webhook_on: webhookOn };
  if (pb.editId) body.id = pb.editId;

  /* Редактирование = удалить старую версию и создать заново (нет PUT-эндпоинта);
     новый id подхватываем ниже и синхронизируем URL. */
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
    clearBuilderDraft();   /* saved → no stale draft to restore */
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

/* Paginated per-entity lists inside the metrics view. */
let _mPipesAll = [], _mPipesPage = 0, _mPipesPerPage = 10;
let _mMvAll    = [], _mMvPage    = 0, _mMvPerPage    = 10;

/* Общий генератор HTML-пагинатора (стрелки + «N–M из T» + выбор размера страницы).
   goFn/sizeFn — ИМЕНА глобальных функций-обработчиков (строки для onclick/onchange),
   разные у каждого списка (метрики/аудит/сохранённые результаты). */
function metricsPagerHTML(total, start, end, pages, page, all, perPage, goFn, sizeFn) {
  const navBtns = pages > 1 ? `
      <button class="btn btn-sm" ${page === 0 ? 'disabled' : ''} onclick="${goFn}(${page - 1})">← Назад</button>
      <span class="pager-info">${start + 1}–${end} из ${total} · стр. ${page + 1}/${pages}</span>
      <button class="btn btn-sm" ${page >= pages - 1 ? 'disabled' : ''} onclick="${goFn}(${page + 1})">Вперёд →</button>`
    : `<span class="pager-info">всего: ${total}</span>`;
  const opts = [10, 25, 50].map(n => `<option value="${n}" ${perPage === n ? 'selected' : ''}>${n}</option>`).join('')
             + `<option value="all" ${all ? 'selected' : ''}>Все</option>`;
  return `<div class="pager">${navBtns}<label class="pager-size">На странице:
            <select onchange="${sizeFn}(this.value)">${opts}</select></label></div>`;
}

function metricPipeCardHTML(p) {
  const st    = p.status || 0;
  const last  = p.last_run ? new Date(p.last_run * 1000).toLocaleString('ru') : 'не запускался';
  const sched = p.cron ? `расписание: ${escHtml(p.cron)}` : 'вручную';
  return `<div class="pipeline-item pipeline-item-clickable" title="Метрики конвейера"
               onclick="showPipelineRuns('${escAttr(p.id)}','${escAttr(p.name || p.id)}')">
    <div style="flex:1;min-width:0">
      <div style="display:flex;align-items:center;gap:.5rem;flex-wrap:wrap">
        <span class="pipeline-name">${escHtml(p.name || p.id)}</span>
        <span class="badge ${STATUS_BADGE[st] || ''}">${STATUS_LABELS[st] || ''}</span>
        ${!p.enabled ? '<span class="badge badge-warn">отключён</span>' : ''}
      </div>
      <div class="pipeline-meta">последний запуск: ${last} &nbsp;·&nbsp; ${sched}</div>
    </div>
    <div class="pipeline-actions"><span class="pipeline-meta">метрики →</span></div>
  </div>`;
}

function metricMvCardHTML(v) {
  const badge = v.last_error ? 'badge-err' : (v.is_stale ? 'badge-warn' : 'badge-ok');
  const lbl   = v.last_error ? 'ошибка' : (v.is_stale ? 'устарело' : 'актуально');
  const when  = v.last_refreshed_at ? new Date(v.last_refreshed_at * 1000).toLocaleString('ru') : 'не обновлялась';
  return `<div class="pipeline-item pipeline-item-clickable" title="Метрики витрины"
               onclick="showMatviewMetrics('${escAttr(v.name)}')">
    <div style="flex:1;min-width:0">
      <div style="display:flex;align-items:center;gap:.5rem;flex-wrap:wrap">
        <span class="pipeline-name">${escHtml(v.name)}</span>
        <span class="badge ${badge}">${lbl}</span>
      </div>
      <div class="pipeline-meta">строк: ${fmtNum(v.row_count || 0)} &nbsp;·&nbsp; обновление: ${v.refresh_duration_ms ?? 0} мс &nbsp;·&nbsp; обновлена: ${when}</div>
    </div>
    <div class="pipeline-actions"><span class="pipeline-meta">метрики →</span></div>
  </div>`;
}

/* Переключение вкладок в «Метриках»: Конвейеры / Витрины. */
function metricsTab(name) {
  const pipes = name === 'pipelines';
  document.getElementById('metrics-tab-pipelines').style.display = pipes ? '' : 'none';
  document.getElementById('metrics-tab-matviews').style.display  = pipes ? 'none' : '';
  document.getElementById('mtab-pipelines').classList.toggle('active', pipes);
  document.getElementById('mtab-matviews').classList.toggle('active', !pipes);
  /* Показанный список только теперь получил высоту — пересчитать под 10 без скролла. */
  sizeListRows(pipes ? 'metrics-pipelines-list' : 'metrics-matviews-list');
}

function renderMetricsPipes() {
  const list = document.getElementById('metrics-pipelines-list');
  if (!list) return;
  const total = _mPipesAll.length;
  if (!total) { list.innerHTML = '<div class="empty-state">Нет конвейеров</div>'; return; }
  const perPage = _mPipesPerPage, all = perPage === Infinity;
  const pages = all ? 1 : Math.max(1, Math.ceil(total / perPage));
  if (_mPipesPage > pages - 1) _mPipesPage = pages - 1;
  if (_mPipesPage < 0) _mPipesPage = 0;
  const start = all ? 0 : _mPipesPage * perPage;
  const end   = all ? total : Math.min(start + perPage, total);
  const slice = all ? _mPipesAll : _mPipesAll.slice(start, end);
  list.innerHTML = slice.map(metricPipeCardHTML).join('') +
    metricsPagerHTML(total, start, end, pages, _mPipesPage, all, perPage, 'mPipesGoPage', 'mPipesSetPerPage');
  sizeListRows('metrics-pipelines-list');
}
function mPipesGoPage(p)     { _mPipesPage = p; renderMetricsPipes(); }
function mPipesSetPerPage(v) { _mPipesPerPage = (v === 'all') ? Infinity : parseInt(v, 10); _mPipesPage = 0; renderMetricsPipes(); }

function renderMetricsMv() {
  const list = document.getElementById('metrics-matviews-list');
  if (!list) return;
  const total = _mMvAll.length;
  if (!total) { list.innerHTML = '<div class="empty-state">Нет витрин</div>'; return; }
  const perPage = _mMvPerPage, all = perPage === Infinity;
  const pages = all ? 1 : Math.max(1, Math.ceil(total / perPage));
  if (_mMvPage > pages - 1) _mMvPage = pages - 1;
  if (_mMvPage < 0) _mMvPage = 0;
  const start = all ? 0 : _mMvPage * perPage;
  const end   = all ? total : Math.min(start + perPage, total);
  const slice = all ? _mMvAll : _mMvAll.slice(start, end);
  list.innerHTML = slice.map(metricMvCardHTML).join('') +
    metricsPagerHTML(total, start, end, pages, _mMvPage, all, perPage, 'mMvGoPage', 'mMvSetPerPage');
  sizeListRows('metrics-matviews-list');
}
function mMvGoPage(p)     { _mMvPage = p; renderMetricsMv(); }
function mMvSetPerPage(v) { _mMvPerPage = (v === 'all') ? Infinity : parseInt(v, 10); _mMvPage = 0; renderMetricsMv(); }

/* Загружает и рисует раздел «Метрики»: параллельно тянет /api/metrics + tables +
   pipelines + matviews, строит карточки показателей (аптайм, задержки, ошибки 4xx/5xx),
   сводки и постраничные списки конвейеров/витрин. Вызывается по таймеру автообновления
   (restartMetricsTimer) — пагинация списков между обновлениями сохраняется. */
async function loadMetrics() {
  try {
    const [raw, tables, pipelines, matviews] = await Promise.all([
      apiFetch('/api/metrics').catch(() => null),
      apiFetch('/api/tables').catch(() => []),
      apiFetch('/api/pipelines').catch(() => []),
      apiFetch('/api/matviews').catch(() => []),
    ]);
    if (!raw) return; /* logged out or network error */
    const m = raw.metrics ?? raw; /* /api/metrics returns a flat object */

    /* ── Stat cards ── */
    const grid = document.getElementById('metrics-detail');
    grid.innerHTML = '';
    const num = v => fmtNum(v ?? 0);
    const ms  = v => (v ?? 0).toFixed(1);
    const e5xx = m.http_errors_5xx ?? 0, e4xx = m.http_errors_4xx ?? 0;
    const storageRows = tables.reduce((s, t) => s + (t.rows || 0), 0);   /* объём данных */
    /* [label, value, warn?, tooltip?] */
    const cards = [
      ['Аптайм',                 fmtUptime(m.uptime ?? 0), false, 'Время работы сервера с момента запуска'],
      ['Таблиц',                 num(tables.length), false, 'Сколько таблиц сейчас в движке (источники + результаты конвейеров + витрины)'],
      ['Строк в хранилище',      num(storageRows), false, 'Суммарно строк во всех таблицах движка — объём данных платформы'],
      ['Строк/мин',              num(Math.round(m.avg_ingest_rows_1min ?? 0)), false, 'Скорость загрузки строк из источников за последнюю минуту'],
      ['Запросов',               num(m.total_queries), false, 'Всего SQL-запросов выполнено с момента запуска'],
      ['Задержка запроса, мс',   ms(m.avg_query_latency_ms), false, 'Средняя длительность SQL-запроса (последняя минута)'],
      ['Запусков конвейеров',    num(m.total_pipelines_run), false, 'Всего запусков конвейеров с момента запуска'],
      ['Задержка конвейера, мс', ms(m.avg_pipeline_latency_ms), false, 'Средняя длительность прогона конвейера (последняя минута)'],
      ['HTTP-латенси, мс',       ms(m.avg_http_latency_ms), false, 'Среднее время ответа API (последняя минута)'],
      ['Ошибки сервера (5xx)',   num(e5xx), e5xx > 0, 'Внутренние ошибки сервера (HTTP 5xx) — сбои/баги. В норме 0'],
      ['Ошибки клиента (4xx)',   num(e4xx), false, 'Отклонённые запросы клиента (HTTP 4xx) — напр. 401 (нет доступа), 404 (не найдено). Обычно не проблема'],
    ];
    cards.forEach(([label, val, warn, tip]) => {
      const card = document.createElement('div');
      card.className = 'stat-card';
      if (tip) card.title = tip;
      card.innerHTML = `<div class="stat-val"${warn ? ' style="color:var(--red)"' : ''}>${val}</div>` +
                       `<div class="stat-label">${label}</div>`;
      grid.appendChild(card);
    });

    /* ── Pipelines — list of plashki (как раздел «Конвейеры»), клик → метрики ── */
    const pEnabled   = pipelines.filter(p => p.enabled).length;
    const pScheduled = pipelines.filter(p => p.cron).length;
    const pSuccess   = pipelines.filter(p => (p.status || 0) === 2).length;
    const pFailed    = pipelines.filter(p => (p.status || 0) === 3).length;
    document.getElementById('metrics-pipelines-sum').textContent = pipelines.length
      ? `всего ${pipelines.length} · включено ${pEnabled} · по расписанию ${pScheduled} · запусков ${m.total_pipelines_run ?? 0} · успех ${pSuccess} / ошибка ${pFailed}`
      : '';
    _mPipesAll = pipelines;
    renderMetricsPipes();   /* пагинация сохраняет текущую страницу между авто-обновлениями */

    /* ── Datamarts — list of plashki, клик → метрики ── */
    _metricsMv = matviews;
    const mvStale  = matviews.filter(v => v.is_stale && !v.last_error).length;
    const mvError  = matviews.filter(v => v.last_error).length;
    const mvActual = matviews.filter(v => !v.is_stale && !v.last_error).length;
    const mvRows   = matviews.reduce((s, v) => s + (v.row_count || 0), 0);
    document.getElementById('metrics-matviews-sum').textContent = matviews.length
      ? `всего ${matviews.length} · актуальных ${mvActual} · устаревших ${mvStale} · с ошибкой ${mvError} · строк ${fmtNum(mvRows)}`
      : '';
    _mMvAll = matviews;
    renderMetricsMv();

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

/* Last datamarts list loaded by the metrics view — for the per-datamart drill-down. */
let _metricsMv = [];

/* Per-datamart metrics modal (reuses the generic runs-modal): aggregate summary
   computed from the collected refresh history + a row per refresh. */
async function showMatviewMetrics(name) {
  const mv   = _metricsMv.find(v => v.name === name);
  const body = document.getElementById('runs-modal-body');
  document.getElementById('runs-modal-title').textContent = `Метрики витрины — ${name}`;
  document.getElementById('runs-modal').classList.remove('hidden');
  body.innerHTML = '<div style="color:var(--muted)">Загрузка…</div>';

  let runs = [];
  try { runs = await apiFetch(`/api/matviews/${encodeURIComponent(name)}/runs`); } catch (e) { /* пусто */ }
  if (!Array.isArray(runs)) runs = [];

  const okN  = runs.filter(r => r.status === 0).length;
  const durs = runs.map(r => r.duration_ms || 0);
  const avgD = durs.length ? durs.reduce((a, b) => a + b, 0) / durs.length : 0;
  const maxD = durs.length ? Math.max(...durs) : 0;
  const lastRows = runs.length ? runs[0].row_count : (mv ? mv.row_count : 0);
  const when = (mv && mv.last_refreshed_at) ? new Date(mv.last_refreshed_at * 1000).toLocaleString('ru')
             : (runs.length && runs[0].finished ? new Date(runs[0].finished * 1000).toLocaleString('ru') : 'не обновлялась');
  const status = (mv && mv.last_error) ? '<span class="badge badge-err">ошибка</span>'
    : (mv && mv.is_stale ? '<span class="badge badge-warn">устарело</span>' : '<span class="badge badge-ok">актуально</span>');

  const summary = `<div style="margin-bottom:.85rem">
    <div class="mstat-row"><span class="mstat-k">Статус</span><span class="mstat-v">${status}</span></div>
    <div class="mstat-row"><span class="mstat-k">Обновлений</span><span class="mstat-v">${runs.length}</span></div>
    <div class="mstat-row"><span class="mstat-k">Успешных</span><span class="mstat-v ok">${okN}</span></div>
    <div class="mstat-row"><span class="mstat-k">С ошибкой</span><span class="mstat-v ${runs.length - okN ? 'err' : ''}">${runs.length - okN}</span></div>
    <div class="mstat-row"><span class="mstat-k">Строк (последнее)</span><span class="mstat-v">${fmtNum(lastRows || 0)}</span></div>
    <div class="mstat-row"><span class="mstat-k">Средняя длительность</span><span class="mstat-v">${avgD.toFixed(1)} мс</span></div>
    <div class="mstat-row"><span class="mstat-k">Макс. длительность</span><span class="mstat-v">${maxD} мс</span></div>
    <div class="mstat-row"><span class="mstat-k">Обновлена</span><span class="mstat-v">${when}</span></div>
  </div>`;

  let table;
  if (runs.length) {
    table = '<table class="runs-table"><thead><tr><th>Начало</th><th>Статус</th><th>Строк</th><th>Длит., мс</th><th>Ошибка</th></tr></thead><tbody>'
      + runs.map(r => {
          const st = r.status === 0 ? '<span class="badge badge-ok">успех</span>' : '<span class="badge badge-err">ошибка</span>';
          return `<tr>
            <td>${r.started ? new Date(r.started * 1000).toLocaleString('ru') : '—'}</td>
            <td>${st}</td>
            <td>${fmtNum(r.row_count || 0)}</td>
            <td>${r.duration_ms ?? 0}</td>
            <td style="${r.error ? 'color:var(--red)' : ''}">${r.error ? `
              <div>${escHtml(humanizeError(r.error))}</div>
              <div style="margin-top:.25rem;font-family:monospace;font-size:.78rem;white-space:pre-wrap;word-break:break-word;opacity:.85">${escHtml(r.error)}</div>
            ` : ''}</td>
          </tr>`;
        }).join('')
      + '</tbody></table>';
  } else {
    table = '<div style="color:var(--muted);margin-top:.5rem">Обновлений ещё не было</div>';
  }

  const sqlBlock = mv ? `<details style="margin-top:.75rem"><summary style="cursor:pointer;color:var(--muted);font-size:.8rem">SQL определения</summary>
       <pre style="white-space:pre-wrap;font-size:.78rem;font-family:var(--mono);background:var(--surface2);border:1px solid var(--border);padding:.5rem;border-radius:6px;margin-top:.4rem;overflow:auto;max-height:200px">${escHtml(mv.definition_sql || '')}</pre>
     </details>` : '';

  body.innerHTML = summary + table + sqlBlock;
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
  /* при возврате к списку пересчитать min-height, чтобы пагинатор встал внизу */
  if (screen === 'list') sizeListRows('an-saved-list');
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

/* Выполняет SQL из редактора Аналитики через /api/tables/query, нормализует ответ
   в массив объектов {колонка: значение} (сервер может вернуть строки массивом),
   кладёт в analyticsState.currentRows/Cols и обновляет таблицу, графики
   (первый запуск → авто-графики) и селекторы статистики. */
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
/* Эвристикой подбирает оси и строит стартовый набор графиков: X — первая
   нечисловая колонка (не *_id), Y — «похожая на метрику» число; если есть колонка
   с датой → добавляет area по времени, иначе scatter по двум числовым. */
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

/* Тянущийся правый край: ширина карточки дискретна (1 → 2 → full колонки),
   переключается по порогам сдвига мыши (delta), а не плавно. */
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
      <button class="btn btn-sm btn-danger" onclick="removeAnalyticsChart('${id}')" title="Удалить">✕</button>
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
/* Преобразует строки в точки графика: scatter — как есть (до 2000 точек),
   histogram — по бинам, остальные — группировка по xCol с агрегатом agg
   (sum/avg/count/min/max) по yCol. line/area сортируются по X, прочие — по убыванию
   значения и обрезаются до topN, а «хвост» сворачивается в «Остальные». */
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
/* Карта «id контейнера-поля → список методов, для которых оно показывается».
   onStatMethodChange по выбранному методу прячет/показывает нужные поля. */
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


/* Диспетчер вкладки «Статистика»: по выбранному методу (st-method) считает результат
   поверх analyticsState.currentRows и рисует его в #stats-output. Каждая ветка ниже —
   отдельный метод (описательная, перцентили, частоты, корреляция, регрессия, гипотезы,
   ANOVA и т.д.). Побочно кладёт результат в lastStatsReport для сохранения/экспорта. */
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

/* ── Статистические примитивы (чистые функции над массивами чисел) ──────────
 * Используются методами вкладки «Статистика» (runStatMethod). */

/* Колонка таблицы → массив только конечных чисел (нечисловые/пустые отброшены). */
function numericCol(rows, col) {
  if (!col) return [];
  return rows.map(r => Number(r[col])).filter(Number.isFinite);
}

/* p-й перцентиль отсортированного массива с линейной интерполяцией (p в 0..100). */
function pctOf(sorted, p) {
  if (!sorted.length) return 0;
  const idx = (p / 100) * (sorted.length - 1);
  const lo = Math.floor(idx), hi = Math.ceil(idx);
  return sorted[lo] + (idx - lo) * ((sorted[hi] ?? sorted[lo]) - sorted[lo]);
}

/* Полная описательная статистика: среднее, медиана, мода, дисперсия/σ (по выборке),
 * квартили/IQR, асимметрия и эксцесс. Возвращает null для пустого массива. */
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

/* Коэффициент корреляции Пирсона по парам (a[i], b[i]); 0 при < 2 пар. */
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

/* Выборочная ковариация двух рядов (делитель n-1). */
function covarianceCalc(a, b) {
  const n = Math.min(a.length, b.length);
  if (n < 2) return 0;
  const mx = a.slice(0,n).reduce((s,v)=>s+v,0)/n;
  const my = b.slice(0,n).reduce((s,v)=>s+v,0)/n;
  return a.slice(0,n).reduce((s,v,i) => s + (v-mx)*(b[i]-my), 0) / (n-1);
}

/* Простая линейная регрессия y = a + b·x методом наименьших квадратов;
 * возвращает {a: пересечение, b: наклон, r2: коэф. детерминации, n}. */
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

/* Скользящее среднее с окном win (по предыдущим win значениям включительно). */
function movingAvg(arr, win) {
  return arr.map((_, i) => {
    const sl = arr.slice(Math.max(0, i - win + 1), i + 1);
    return sl.reduce((s, v) => s + v, 0) / sl.length;
  });
}

/* Экспоненциальное сглаживание с коэффициентом al (0..1). */
function expSmooth(arr, al) {
  if (!arr.length) return [];
  const r = [arr[0]];
  for (let i = 1; i < arr.length; i++) r.push(al * arr[i] + (1 - al) * r[i - 1]);
  return r;
}

/* Однофакторный дисперсионный анализ (one-way ANOVA): группирует valCol по
 * groupCol, считает SS/MS между и внутри групп, F-статистику и p-value. */
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

/* Берёт первые две группы (по groupCol) с >1 значением — вход для welchT. */
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

/* t-критерий Уэлча для двух выборок с неравными дисперсиями; p — двусторонний
 * (нормальное приближение через normalCdf). */
function welchT(a, b) {
  const da = descExt(a) || {mean:0,sd:0,n:0}, db = descExt(b) || {mean:0,sd:0,n:0};
  const se = Math.sqrt((da.sd ** 2 / (da.n||1)) + (db.sd ** 2 / (db.n||1)));
  const t = se ? (da.mean - db.mean) / se : 0;
  const p = 2 * (1 - normalCdf(Math.abs(t)));
  return { t, p };
}

/* Функция распределения стандартного нормального закона N(0,1). */
function normalCdf(x) {
  return 0.5 * (1 + erf(x / Math.SQRT2));
}

/* Функция ошибок erf(x) — приближение Абрамовица–Стиган (max. ошибка ~1.5e-7). */
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
      el.innerHTML = '';
      return;
    }
    const roleLabel = { admin: 'admin', analyst: 'analyst', viewer: 'viewer' };
    el.innerHTML = keys.map(k => {
      const keyId = escAttr(k.key || '');
      const created = k.created_at ? new Date(k.created_at * 1000).toLocaleDateString('ru') : '—';
      return `
        <div class="pipeline-item" id="akrow-${keyId}">
          <div style="flex:1;min-width:0">
            <div class="pipeline-name" style="font-family:var(--mono);font-size:.85rem">${escHtml(k.key || '—')}</div>
            <div class="pipeline-meta">
              user: <b>${escHtml(k.user_id || '—')}</b> · роль: <b>${escHtml(k.role || '—')}</b> · создан: ${escHtml(created)}
            </div>
          </div>
          <div class="pipeline-actions">
            <button class="btn btn-sm btn-danger" title="Удалить ключ" onclick="deleteApiKey('${keyId}')">✕</button>
          </div>
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
}

/* ── Saved Results ── */
let _anSavedAll = [], _anSavedPage = 0, _anSavedPerPage = 10;

async function loadSavedResults() {
  try {
    const list = await apiGet('/api/analytics/results');
    const el = document.getElementById('an-saved-list');
    if (!el) return;
    _anSavedAll = Array.isArray(list) ? list : [];
    if (!_anSavedAll.length) {
      el.innerHTML = '<div class="empty-state">Нет сохранённых результатов.<br>Нажмите «+ Добавить аналитику» чтобы создать первый анализ.</div>';
      return;
    }
    _anSavedPage = 0;
    renderAnSavedPage();
  } catch(e) { console.error('loadSavedResults', e); }
}

function anSavedCardHTML(r) {
  const cols = Array.isArray(r.columns_json) ? r.columns_json
    : (r.columns_json ? JSON.parse(r.columns_json) : []);
  const pills = cols.map(c => `<span class="step-badge">${escHtml(c)}</span>`).join('');
  return `<div class="pipeline-item pipeline-item-clickable" id="sr_${r.id}" onclick="openSavedResult(${r.id})">
    <div style="flex:1;min-width:0">
      <div style="display:flex;align-items:center;gap:.5rem;flex-wrap:wrap">
        <span class="pipeline-name">${escHtml(r.name)}</span>
      </div>
      <div class="pipeline-meta">${r.row_count} строк · ${new Date(r.created_at*1000).toLocaleDateString('ru')}</div>
      ${pills ? `<div class="step-list">${pills}</div>` : ''}
    </div>
    <div class="pipeline-actions">
      <button class="btn btn-sm btn-danger" title="Удалить" onclick="event.stopPropagation();deleteSavedResult(${r.id})">✕</button>
    </div>
  </div>`;
}

/* Пагинация сохранённой аналитики — как у конвейеров/витрин: 10 на странице по умолчанию. */
function renderAnSavedPage() {
  const el = document.getElementById('an-saved-list');
  if (!el) return;
  const total   = _anSavedAll.length;
  const perPage = _anSavedPerPage;
  const all     = perPage === Infinity;
  const pages   = all ? 1 : Math.max(1, Math.ceil(total / perPage));
  if (_anSavedPage > pages - 1) _anSavedPage = pages - 1;
  if (_anSavedPage < 0) _anSavedPage = 0;
  const start = all ? 0 : _anSavedPage * perPage;
  const end   = all ? total : Math.min(start + perPage, total);
  const slice = all ? _anSavedAll : _anSavedAll.slice(start, end);
  el.innerHTML = slice.map(anSavedCardHTML).join('') +
    metricsPagerHTML(total, start, end, pages, _anSavedPage, all, perPage, 'anSavedGoPage', 'anSavedSetPerPage');
  sizeListRows('an-saved-list');
}
function anSavedGoPage(p)     { _anSavedPage = p; renderAnSavedPage(); }
function anSavedSetPerPage(v) { _anSavedPerPage = (v === 'all') ? Infinity : parseInt(v, 10); _anSavedPage = 0; renderAnSavedPage(); }

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

/* Сохраняет результат в /api/analytics/results. Тип берётся из modal._saveType:
   'stats' → сериализует lastStatsReport в таблицу (_statsReportToTable),
   иначе — текущие строки запроса. Строки сохраняются массивами по порядку columns. */
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
/* ── HTTP-обёртки над fetch ──────────────────────────────────────────────
 * Все добавляют JWT (если есть), а на 401 разлогинивают и возвращают undefined
 * вместо выброса — поэтому вызывающий код проверяет результат на falsy. */
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

/* PUT с JSON-телом. apiFetch(path,'PUT',body) не годится: шлёт тело сырым и не
 * ставит Content-Type. Нужен для идемпотентного обновления подключений. */
async function apiPut(path, body) {
  const headers = { 'Content-Type': 'application/json' };
  if (jwtToken) headers['Authorization'] = `Bearer ${jwtToken}`;
  const resp = await fetch(API + path, {
    method: 'PUT',
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

/* Логин: POST логин/пароль на /api/auth/token, сохраняет полученный JWT в
   localStorage (dfo_jwt) и currentUser, прячет экран входа и запускает initApp. */
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
    currentUser = parseJwt(jwtToken);
    localStorage.setItem('dfo_jwt', jwtToken);
    isLoggedIn = true;
    hideLogin();
    initApp();
  } catch (e) {
    alert('Login failed: ' + e.message);
  }
}

function logout() {
  jwtToken = null;
  currentUser = null;
  localStorage.removeItem('dfo_jwt');
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
  applyRoleVisibility();   /* скрыть «Администрирование» не-admin пользователям */

  const { view, id } = parseHash();

  if (view === 'builder' && id) {
    /* '#builder/<id>' — fetch the pipeline and reopen the builder on it. */
    restoreBuilderFromHash(id);
  } else {
    let stored; try { stored = localStorage.getItem('dfo_view'); } catch (_) {}
    const v = VALID_VIEWS.has(view) ? view : (VALID_VIEWS.has(stored) ? stored : 'pipelines');
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
const MV_REFRESH = ['Вручную', 'При изменении источника', 'По расписанию'];
let _mvList = [];   /* last loaded datamarts — used to prefill the edit form */
let _mvPerPage = 10;   /* page size (Infinity = «Все») */
let _mvPage = 0;

/* ══ Справочник подключений к внешним системам ═══════════════════════════════
 * Подключение — именованная пара «тип системы + параметры доступа». Шаг
 * конвейера ссылается на него через connection_id, поэтому пароль хранится в
 * одном месте и его смена подхватывается всеми конвейерами.
 *
 * Здесь описаны ТОЛЬКО параметры доступа. Пошаговые ключи (query, table,
 * cursor_column, topic, group_id, offset_*, data_path…) остаются в карточке
 * шага в конструкторе — их читает гейтвей из конфига шага.
 *
 * Секреты сервер отдаёт как «********»; если поле не трогали, оно уходит
 * обратно маской и пароль остаётся прежним. */


const CONN_TYPE_LABELS = {
  postgresql: 'PostgreSQL', greenplum: 'Greenplum', oracle: 'Oracle',
  kafka: 'Kafka', csv: 'CSV-файл', parquet: 'Parquet-файл',
  json_http: 'JSON по HTTP', xml: 'XML', soap: 'SOAP', siebel: 'Siebel',
};




let _connList = [];          /* последний загруженный справочник (для экрана) */
let connCache  = [];         /* тот же справочник для СИНХРОННОГО чтения из конструктора */
let _connEditId = null;      /* id редактируемого подключения, null = создание */
let _connAfterSave = null;   /* колбэк: подставить новое подключение в шаг конструктора */

/* Кэш обязателен синхронный: makeStepCard/renderBuilderSteps не умеют await. */
async function loadConnectionsCache() {
  try { connCache = (await apiGet('/api/connections')) || []; }
  catch (_) { connCache = []; }
  return connCache;
}

async function loadConnectionsView() {
  const el = document.getElementById('connections-list');
  if (!el) return;
  el.innerHTML = '<div style="color:var(--muted);padding:.5rem">Загрузка…</div>';
  try {
    /* Список конвейеров нужен строке подключения: она показывает, кто его
     * использует. Если раздел «Конвейеры» ещё не открывали, подтянем сами. */
    if (!Array.isArray(_pipelinesAll) || !_pipelinesAll.length) {
      try { _pipelinesAll = (await apiGet('/api/pipelines')) || []; } catch (_) {}
    }
    const list = await apiGet('/api/connections');
    _connList = list || [];        /* apiGet после 401 отдаёт undefined, а не бросает */
    connCache = _connList;
    renderConnectionsList();
  } catch (err) {
    el.innerHTML = `<div class="settings-loading" style="color:var(--red)">${escHtml(String(err))}</div>`;
  }
}

function renderConnectionsList() {
  const el = document.getElementById('connections-list');
  if (!el) return;
  if (!_connList.length) {
    el.innerHTML = '<div class="empty-state">Подключений пока нет. Добавьте первое — и его можно будет выбирать в шагах конвейера.</div>';
    return;
  }
  el.innerHTML = '';
  _connList.forEach(c => el.appendChild(makeConnectionRow(c)));
  sizeListRows('connections-list');
}

function makeConnectionRow(c) {
  const div = document.createElement('div');
  /* Кликабельная строка — как у конвейеров: клик по плашке открывает редактор,
   * клики по кнопкам не перехватываем. */
  div.className = 'pipeline-item pipeline-item-clickable';
  div.title = 'Открыть подключение';
  div.onclick = (e) => {
    if (e.target.closest('button')) return;
    openConnectionEditor(c.id);
  };
  const typeLabel = CONN_TYPE_LABELS[c.type] || c.type;
  const cfg = c.config || {};
  /* Короткая сводка «куда смотрит» — по одному ключу на семейство типов. */
  const target = cfg.host ? `${cfg.host}${cfg.port ? ':' + cfg.port : ''}${cfg.dbname ? '/' + cfg.dbname : ''}${cfg.service_name ? '/' + cfg.service_name : ''}`
               : (cfg.brokers || cfg.url || cfg.siebel_url || cfg.path || '');
  const used = countStepsUsingConnection(c.id);
  /* Состояние ПОСТОЯННОЙ сессии из пула: она живёт между запусками конвейеров,
   * поэтому показываем её здесь, а не только в момент прогона. */
  const st = c.status || 'idle';
  const stBadge = st === 'ok'
    ? `<span class="badge badge-ok" title="Постоянная сессия установлена${c.last_ok ? ', проверена ' + new Date(c.last_ok * 1000).toLocaleString('ru-RU') : ''}">● подключено${c.sessions > 1 ? ' ×' + c.sessions : ''}</span>`
    : (st === 'error'
      ? `<span class="badge badge-err" title="${escAttr(c.last_error || 'сессия недоступна')}">● нет связи</span>`
      : `<span class="badge" title="Сессия ещё не поднята — фоновый обход поднимет её в течение минуты">○ подключается…</span>`);
  /* Конвейеры, которые опираются на это подключение, — плашками, как шаги в
   * строке конвейера (.step-list / .step-badge). */
  const users = (Array.isArray(_pipelinesAll) ? _pipelinesAll : [])
    .filter(p => (p.steps || []).some(s => s.connection_id === c.id))
    .map(p => `<span class="step-badge">${escHtml(p.name || p.id)}</span>`)
    .join('');

  /* Структура один-в-один со строкой конвейера (makePipelineRow): обёртка
   * flex:1, первая строка «название + плашки», под ней meta, затем плашки
   * связанных сущностей, справа — действия. */
  div.innerHTML = `
    <div style="flex:1;min-width:0">
      <div style="display:flex;align-items:center;gap:.5rem;flex-wrap:wrap">
        <span class="pipeline-name">${escHtml(c.name)}</span>
        ${stBadge}
        <span class="badge badge-run">${escHtml(typeLabel)}</span>

      </div>
      <div class="pipeline-meta">
        адрес: <code>${escHtml(target || '—')}</code>
        &nbsp;·&nbsp; проверено: ${c.last_ok ? new Date(c.last_ok * 1000).toLocaleString('ru') : '—'}
        &nbsp;·&nbsp; используется шагов: ${used}
      </div>
      <div class="step-list">${users}</div>
    </div>
    <div class="pipeline-actions">
      <button class="btn btn-sm btn-primary" onclick="openConnectionEditor('${escAttr(c.id)}')">✎ Изменить</button>
      <button class="btn btn-sm btn-danger" onclick="deleteConnection('${escAttr(c.id)}')">✕</button>
    </div>`;
  return div;
}

/* Сколько шагов ссылается на подключение — чтобы предупредить перед удалением. */
function countStepsUsingConnection(id) {
  if (!id || !Array.isArray(_pipelinesAll)) return 0;
  let n = 0;
  _pipelinesAll.forEach(p => (p.steps || []).forEach(s => { if (s.connection_id === id) n++; }));
  return n;
}


function openConnectionEditor(id, dirHint, afterSave) {
  _connEditId    = id || null;
  _connAfterSave = afterSave || null;

  /* Экран подключений может быть не открыт — редактор вызывают и из конструктора. */
  if (!document.getElementById('view-connections').classList.contains('active'))
    switchView('connections');

  const rec = id ? (connCache.find(c => c.id === id) || null) : null;

  /* Виртуальный «шаг» редактора: на нём работает та же форма коннектора, что и
   * в конструкторе. connection_id нужен пробам — сервер подставит по нему
   * настоящие секреты вместо масок. */
  _connFormStep = {
    id: '__conn__',
    connector_type:   rec ? rec.type : 'postgresql',
    connector_config: JSON.stringify(rec ? (rec.config || {}) : {}),
    connection_id:    id || '',
    is_sink:          false,
    transform_sql:    '',
    target_table:     '',
  };

  document.getElementById('conn-form-title').textContent = rec ? 'Изменение подключения' : 'Новое подключение';
  document.getElementById('conn-name').value = rec ? rec.name : '';
  document.getElementById('conn-status').textContent = '';
  document.getElementById('conn-editor').style.display = '';
  renderConnEditorForm();
  document.getElementById('conn-name').focus();
}

/* Кнопки типов — те же наборы, что в карточке шага, и та же форма под ними. */
function renderConnEditorForm() {
  const cur  = _connFormStep.connector_type;
  /* Один набор типов: подключение не делится на источник и приёмник. */
  const opts = SOURCE_OPTIONS;
  document.getElementById('conn-type-buttons').innerHTML = opts.map(([v, name]) =>
    `<button type="button" class="btn btn-sm${cur === v ? ' btn-primary' : ''}"
             onclick="connSetType('${v}')">${escHtml(name)}</button>`).join('');
  renderConnEditorBody();
}

/* Тело формы подключения. ЕДИНСТВЕННАЯ точка сборки: её же зовёт
 * pbRerenderStepCfg при частичной перерисовке (смена протокола, формата,
 * режима чтения, стартовой позиции). Раньше перерисовка шла другим путём и
 * теряла и поля варианта приёмника, и секцию значений по умолчанию — поля
 * «то есть, то нет» в зависимости от переключения. */
function renderConnEditorBody() {
  const box = document.getElementById('step-conn-cfg--1');
  if (!box || !_connFormStep) return;
  box.innerHTML = makeConnectorConfigHTML(_connFormStep, CONN_FORM_IDX);
  /* Подключение не знает про роль, а форма коннектора кое-где ветвится по
   * is_sink: часть полей доступа рисуется ТОЛЬКО в варианте приёмника
   * (у SOAP — «Пространство имён»). Показываем объединение обоих вариантов,
   * иначе такое поле не появилось бы нигде: в шаге его скроет фильтр доступа,
   * а сюда оно не попало бы вовсе. */
  addAccessFieldsFromSinkVariant(box, _connFormStep);
  /* Поля запроса (топик, группа, offset, формат) не прячем, а выносим вниз
   * отдельным блоком: здесь они задают ЗНАЧЕНИЯ ПО УМОЛЧАНИЮ для всех шагов на
   * этом подключении, а шаг конвейера их переопределяет. Слияние на сервере
   * ровно такое же: конфиг подключения — база, конфиг шага — сверху. */
  groupStepDefaults(box, _connFormStep.connector_type);
}

/* Переносит поля запроса в отдельную секцию «Значения по умолчанию для шагов». */
function groupStepDefaults(box, type) {
  const req = CONN_REQUEST_KEYS[type];
  if (!req || !req.length) return;
  const shared = CONN_SHARED_KEYS[type] || [];
  const keyOf = (el) => {
    const n = el.querySelector('[oninput],[onchange],[onclick]');
    if (!n) return null;
    const h = (n.getAttribute('oninput') || '') + ';' + (n.getAttribute('onchange') || '');
    const m = h.match(/pbUpdateConnConfig\(\s*-?\d+\s*,\s*'([a-zA-Z_0-9]+)'/);
    if (m) return m[1];
    if (h.indexOf('pbSetKafkaOffsetStartMode(') >= 0) return 'offset_start_mode';
    return null;
  };
  const move = [];
  box.querySelectorAll('.form-group').forEach(g => {
    const k = keyOf(g);
    /* общий ключ (формат сообщений) уже показан среди параметров доступа */
    if (k && req.indexOf(k) >= 0 && shared.indexOf(k) < 0) move.push(g);
  });
  if (!move.length) return;

  const sec = document.createElement('div');
  sec.className = 'conn-group';
  sec.style.marginTop = '.75rem';
  const title = document.createElement('div');
  title.className = 'conn-group-title';
  title.textContent = 'Значения по умолчанию для шагов';
  const hint = document.createElement('div');
  hint.style.cssText = 'font-size:.72rem;color:var(--muted);margin:-.2rem 0 .4rem';
  hint.textContent = 'Подставляются в шаги конвейеров на этом подключении. ' +
                     'В самом шаге любое из них можно задать по-своему — значение шага главнее.';
  const row = document.createElement('div');
  row.className = 'conn-grid';          /* та же сетка, что и у полей доступа */
  move.forEach(g => row.appendChild(g));
  sec.appendChild(title); sec.appendChild(hint); sec.appendChild(row);

  /* Предпросмотр читает данные по параметрам запроса (топик, таблица, запрос) —
   * значит, его место под этими полями, а не в блоке доступа: иначе кнопка
   * стоит выше того, от чего зависит. «Подключиться» остаётся наверху: оно
   * проверяет только доступ. У БД кнопка «Выполнить» лежит внутри поля
   * «Запрос к источнику» и переезжает вместе с ним — её не трогаем. */
  const prevBtn = [...box.querySelectorAll('.conn-actions button[onclick*="pbPreview"]')]
                    .find(b => !b.closest('.form-group'));
  if (prevBtn) {
    const acts = document.createElement('div');
    acts.className = 'conn-actions';
    acts.style.cssText = 'display:flex;gap:.5rem;margin-top:.7rem;flex-wrap:wrap';
    acts.appendChild(prevBtn);
    sec.appendChild(acts);
  }
  /* Вывод предпросмотра — всегда в самый низ, под кнопку, которая его наполняет. */
  const out = box.querySelector('[id^="pb-steppreview-"],[id^="pb-dbpreview-"]');
  if (out) sec.appendChild(out);

  box.appendChild(sec);
}

/* Добавляет в форму подключения поля доступа, которые форма рисует только для
 * приёмника. Дубликаты не создаём: ключ, уже присутствующий в разметке,
 * пропускаем. */
function addAccessFieldsFromSinkVariant(box, formStep) {
  const tmp = document.createElement('div');
  tmp.innerHTML = makeConnectorConfigHTML(
    Object.assign({}, formStep, { is_sink: true }), CONN_FORM_IDX);

  const keyOf = (el) => {
    const n = el.querySelector('[oninput],[onchange],[onclick]');
    if (!n) return null;
    const h = (n.getAttribute('oninput') || '') + ';' + (n.getAttribute('onchange') || '');
    const m = h.match(/pbUpdateConnConfig\(\s*-?\d+\s*,\s*'([a-zA-Z_0-9]+)'/);
    return m ? m[1] : null;
  };
  const have = new Set();
  box.querySelectorAll('.form-group').forEach(g => { const k = keyOf(g); if (k) have.add(k); });

  const extra = [];
  tmp.querySelectorAll('.form-group').forEach(g => {
    const k = keyOf(g);
    if (k && !have.has(k)) { have.add(k); extra.push(g); }
  });
  if (!extra.length) return;
  const row = document.createElement('div');
  row.className = 'conn-grid';
  extra.forEach(g => row.appendChild(g));
  box.appendChild(row);
}

function connSetType(type) {
  if (_connFormStep.connector_type === type) return;
  _connFormStep.connector_type   = type;
  _connFormStep.connector_config = '{}';   /* у другого типа другие ключи */
  renderConnEditorForm();
}

function closeConnectionEditor() {
  document.getElementById('conn-editor').style.display = 'none';
  _connEditId = null;
  _connAfterSave = null;
}

/* Тело запроса. Конфиг уже собран самой формой в _connFormStep.connector_config
 * (через pbUpdateConnConfig), поэтому здесь его достаточно разобрать. Секрет,
 * оставленный маской, уходит как есть — сервер поймёт это как «не менять». */
function collectConnectionForm() {
  return {
    name: document.getElementById('conn-name').value.trim(),
    type: _connFormStep.connector_type,
    config: safeParse(_connFormStep.connector_config, {}),
  };
}

async function saveConnection() {
  const body = collectConnectionForm();
  const st = document.getElementById('conn-status');
  if (!body.name) { showToast('Укажите название подключения', 'warn'); return; }
  st.textContent = 'Сохранение…';
  try {
    const saved = _connEditId
      ? await apiPut('/api/connections/' + encodeURIComponent(_connEditId), body)
      : await apiPost('/api/connections', body);
    st.textContent = '';
    showToast('Подключение сохранено', 'ok');
    const cb = _connAfterSave;
    closeConnectionEditor();
    await loadConnectionsView();
    if (cb && saved && saved.id) cb(saved.id);
  } catch (err) {
    st.textContent = '';
    showToast(`Не удалось сохранить: ${escHtml(String(err))}`, 'error');
  }
}

async function deleteConnection(id) {
  const rec = connCache.find(c => c.id === id);
  const used = countStepsUsingConnection(id);
  const warn = used
    ? `\n\nНа него ссылаются шаги конвейеров: ${used}. Эти шаги перестанут запускаться, пока вы не выберете им другое подключение.`
    : '';
  if (!confirm(`Удалить подключение «${rec ? rec.name : id}»?${warn}`)) return;
  try {
    await apiDelete('/api/connections/' + encodeURIComponent(id));
    showToast('Подключение удалено', 'ok');
    await loadConnectionsView();
  } catch (err) {
    showToast(`Не удалось удалить: ${escHtml(String(err))}`, 'error');
  }
}

async function loadMatviews() {
  const el = document.getElementById('matviews-list');
  el.innerHTML = '<div style="color:var(--muted);padding:.5rem">Загрузка…</div>';
  try {
    const list = await apiFetch('/api/matviews');
    _mvList = list || [];
    if (!_mvList.length) {
      el.innerHTML = '<div class="empty-state">Витрин пока нет.</div>';
      return;
    }
    _mvPage = 0;
    renderMatviewsPage();
  } catch (err) {
    el.innerHTML = `<div class="settings-loading" style="color:var(--red)">${escHtml(String(err))}</div>`;
  }
}

/* Render the current page of datamarts + pager (page nav + page size). */
function renderMatviewsPage() {
  const el = document.getElementById('matviews-list');
  if (!el) return;
  const total   = _mvList.length;
  const perPage = _mvPerPage;
  const all     = perPage === Infinity;
  const pages   = all ? 1 : Math.max(1, Math.ceil(total / perPage));
  if (_mvPage > pages - 1) _mvPage = pages - 1;
  if (_mvPage < 0) _mvPage = 0;
  const start = all ? 0 : _mvPage * perPage;
  const end   = all ? total : Math.min(start + perPage, total);
  const slice = all ? _mvList : _mvList.slice(start, end);
  el.innerHTML = '';
  slice.forEach(mv => el.appendChild(makeMatviewRow(mv)));
  if (total > 0) {
    const nav = document.createElement('div');
    nav.className = 'pager';
    const navBtns = pages > 1 ? `
      <button class="btn btn-sm" ${_mvPage === 0 ? 'disabled' : ''} onclick="mvGoPage(${_mvPage - 1})">← Назад</button>
      <span class="pager-info">${start + 1}–${end} из ${total} · стр. ${_mvPage + 1}/${pages}</span>
      <button class="btn btn-sm" ${_mvPage >= pages - 1 ? 'disabled' : ''} onclick="mvGoPage(${_mvPage + 1})">Вперёд →</button>`
      : `<span class="pager-info">всего: ${total}</span>`;
    const opts = [10, 25, 50].map(n => `<option value="${n}" ${perPage === n ? 'selected' : ''}>${n}</option>`).join('')
               + `<option value="all" ${all ? 'selected' : ''}>Все</option>`;
    nav.innerHTML = navBtns +
      `<label class="pager-size">На странице:
         <select onchange="mvSetPerPage(this.value)">${opts}</select>
       </label>`;
    el.appendChild(nav);
  }
  sizeListRows('matviews-list');
}

function mvGoPage(p) {
  _mvPage = p;
  renderMatviewsPage();
  document.getElementById('matviews-list').scrollIntoView({ block: 'start', behavior: 'smooth' });
}

function mvSetPerPage(v) {
  _mvPerPage = (v === 'all') ? Infinity : parseInt(v, 10);
  _mvPage = 0;
  renderMatviewsPage();
}

/* Datamart card — same visual chrome as a pipeline row (makePipelineRow). */
function makeMatviewRow(mv) {
  const div = document.createElement('div');
  div.className = 'pipeline-item pipeline-item-clickable';
  div.title = 'Открыть данные витрины';
  div.onclick = (e) => {
    /* ignore clicks on the action buttons and on the «SQL определения» disclosure */
    if (e.target.closest('button') || e.target.closest('details')) return;
    queryMatview(mv.name);
  };
  const dur  = (mv.refresh_duration_ms != null && mv.refresh_duration_ms >= 0) ? `${mv.refresh_duration_ms} мс` : '—';
  const when = mv.last_refreshed_at ? new Date(mv.last_refreshed_at * 1000).toLocaleString('ru') : 'ещё не обновлялась';
  const badge = mv.last_error
    ? '<span class="badge badge-err">ошибка</span>'
    : (mv.is_stale ? '<span class="badge badge-warn">устарело</span>'
                   : '<span class="badge badge-ok">актуально</span>');
  div.innerHTML = `
    <div style="flex:1;min-width:0">
      <div style="display:flex;align-items:center;gap:.5rem;flex-wrap:wrap">
        <span class="pipeline-name">${escHtml(mv.name)}</span>
        ${badge}
      </div>
      <div class="pipeline-meta">
        строк: ${fmtNum(mv.row_count || 0)}
        &nbsp;·&nbsp; обновлена: ${when}
        &nbsp;·&nbsp; ${dur}
      </div>
      ${mv.last_error ? `<div class="pipeline-meta" style="color:var(--red)">⚠ ${escHtml(mv.last_error)}</div>` : ''}
      <details style="margin-top:.5rem">
        <summary style="cursor:pointer;font-size:.72rem;color:var(--muted)">SQL определения</summary>
        <pre style="margin:.35rem 0 0;white-space:pre-wrap;font-size:.75rem;font-family:var(--mono);background:var(--surface2);border:1px solid var(--border);padding:.5rem;border-radius:6px;overflow:auto;max-height:220px">${escHtml(mv.definition_sql || '')}</pre>
      </details>
    </div>
    <div class="pipeline-actions">
      <button class="btn btn-sm btn-primary" onclick="refreshMatview('${escAttr(mv.name)}')">↻ Обновить</button>
      <button class="btn btn-sm" onclick="editMatview('${escAttr(mv.name)}')">Изменить</button>
      <button class="btn btn-sm btn-danger" onclick="dropMatview('${escAttr(mv.name)}')">✕</button>
    </div>`;
  return div;
}

/* Refresh every datamart sequentially. */
async function refreshAllMatviews() {
  if (!_mvList.length) return;
  showToast(`Обновляю ${_mvList.length} витрин…`, 'ok');
  for (const mv of _mvList) {
    try { await apiPost(`/api/matviews/${encodeURIComponent(mv.name)}/refresh`, {}); } catch (e) {}
  }
  showToast('Витрины обновлены', 'ok');
  loadMatviews();
}

/* Open the form pre-filled with an existing datamart for editing (UPSERT by name). */
function editMatview(name) {
  const mv = _mvList.find(m => m.name === name);
  if (!mv) return;
  openCreateMatview();
  document.getElementById('mv-form-title').textContent = 'Изменить витрину';
  document.getElementById('mv-name').value    = mv.name;
  document.getElementById('mv-name').readOnly  = true;   /* name is the identity — don't rename here */
  document.getElementById('mv-sql').value      = mv.definition_sql || '';
}

function openCreateMatview() {
  document.getElementById('matview-create-form').style.display = '';
  document.getElementById('mv-form-title').textContent = 'Новая витрина';
  const nm = document.getElementById('mv-name');
  nm.readOnly = false; nm.value = '';
  document.getElementById('mv-sql').value = '';
  document.getElementById('mv-status').textContent = '';
  loadMvTablesHint();
  nm.focus();
}

/* Show the engine tables a datamart can be built from; click a chip → insert its
 * name into the SQL editor at the cursor. */
async function loadMvTablesHint() {
  const el = document.getElementById('mv-tables-hint');
  if (!el) return;
  const cw = document.getElementById('mv-cols-wrap'); if (cw) cw.style.display = 'none';
  el.innerHTML = '<span style="font-size:.75rem;color:var(--muted)">Загрузка…</span>';
  try {
    let tables = await apiFetch('/api/tables');
    tables = (tables || []).filter(t => !(t.name || '').startsWith('__'));
    if (!tables.length) {
      el.innerHTML = '<span style="font-size:.75rem;color:var(--muted)">Таблиц пока нет — наполните данные конвейерами.</span>';
      return;
    }
    el.innerHTML = tables.map(t => {
      const rows = t.rows ?? t.row_count ?? t.nrows;
      const cnt  = (rows != null) ? ` <span style="color:var(--muted)">· ${fmtNum(rows)}</span>` : '';
      return `<button type="button" class="step-badge" style="cursor:pointer;font-family:inherit;line-height:1.4"
                 onclick="mvPickTable('${escAttr(t.name)}')" title="Показать колонки и вставить имя">${escHtml(t.name)}${cnt}</button>`;
    }).join('');
  } catch (e) {
    el.innerHTML = `<span style="font-size:.75rem;color:var(--red)">${escHtml(String(e))}</span>`;
  }
}

/* Click a table chip: insert its name into the SQL and reveal its columns. */
function mvPickTable(name) {
  mvInsertSql(name);
  loadMvColumns(name);
}

/* Fetch a table's columns and show them as clickable chips (click → insert name). */
async function loadMvColumns(table) {
  const wrap = document.getElementById('mv-cols-wrap');
  const el   = document.getElementById('mv-cols-hint');
  const lbl  = document.getElementById('mv-cols-table');
  if (!wrap || !el) return;
  wrap.style.display = '';
  lbl.textContent = table;
  el.innerHTML = '<span style="font-size:.75rem;color:var(--muted)">Загрузка…</span>';
  try {
    const sch = await apiFetch(`/api/tables/${encodeURIComponent(table)}/schema`);
    const cols = (sch && sch.columns) || [];
    if (!cols.length) { el.innerHTML = '<span style="font-size:.75rem;color:var(--muted)">Нет колонок</span>'; return; }
    el.innerHTML = cols.map(c =>
      `<button type="button" class="step-badge" style="cursor:pointer;font-family:inherit;line-height:1.4"
          onclick="mvInsertSql('${escAttr(c.name)}')" title="${escAttr(c.type || '')}">${escHtml(c.name)}<span style="color:var(--muted)"> · ${escHtml(c.type || '')}</span></button>`
    ).join('');
  } catch (e) {
    el.innerHTML = `<span style="font-size:.75rem;color:var(--red)">${escHtml(String(e))}</span>`;
  }
}

/* Insert text into the SQL editor at the caret. */
function mvInsertSql(text) {
  const ta = document.getElementById('mv-sql');
  if (!ta) return;
  const a = ta.selectionStart ?? ta.value.length;
  const b = ta.selectionEnd   ?? a;
  ta.value = ta.value.slice(0, a) + text + ta.value.slice(b);
  ta.focus();
  const pos = a + text.length;
  ta.setSelectionRange(pos, pos);
}

function closeCreateMatview() {
  document.getElementById('matview-create-form').style.display = 'none';
  document.getElementById('mv-status').textContent = '';
}

async function createMatview() {
  const name = document.getElementById('mv-name').value.trim();
  const sql  = document.getElementById('mv-sql').value.trim();
  const status = document.getElementById('mv-status');
  if (!name || !sql) { status.textContent = 'Укажите название и SQL'; return; }
  const sources = [];          /* derived from SQL on the backend; no manual field */
  const refresh_mode = 0;      /* manual refresh only */
  const refresh_cron = '';
  try {
    await apiPost('/api/matviews', { name, definition_sql: sql, source_tables: sources, refresh_mode, refresh_cron });
    status.textContent = '';
    closeCreateMatview();
    loadMatviews();
    showToast(`Витрина ${name} сохранена и материализована`, 'ok');
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
/* event_type (lib/auth/audit.h) → человекопонятное русское действие */
const AUDIT_TYPES  = ['', 'SQL-запрос', 'Загрузка данных', 'Запуск конвейера', 'Вход в систему',
                      'Неудачный вход', 'Изменение схемы', 'Изменение прав доступа'];

/* allowed_actions — битовая маска (lib/auth/rbac.h). Декодируем в читаемый список. */
const RBAC_ACTION_BITS = [
  [1, 'Чтение'], [2, 'Запись'], [4, 'Создание'], [8, 'Удаление'],
  [16, 'Запуск конв.'], [32, 'Правка конв.'], [64, 'Админ'], [128, 'Метрики'],
];
function rbacActionsLabel(mask) {
  mask = mask | 0;
  if (mask === 0) return '—';
  if ((mask & 255) === 255) return 'Все права';
  return RBAC_ACTION_BITS.filter(([b]) => mask & b).map(([, l]) => l).join(', ');
}

/* Переключает под-вкладку раздела «Безопасность» (Пользователи/RBAC/Аудит/API-ключи),
   запоминает выбор в localStorage (dfo_sec_tab) и лениво грузит данные активной вкладки. */
function switchSecTab(tab) {
  ['users','rbac','audit','apikeys'].forEach(t => {
    document.getElementById(`sec-tab-${t}`).classList.toggle('active', t === tab);
    document.getElementById(`sec-pane-${t}`).style.display = t === tab ? '' : 'none';
  });
  try { localStorage.setItem('dfo_sec_tab', tab); } catch (_) {}
  if (tab === 'users')        loadUsers();
  else if (tab === 'audit')   loadAuditLog();
  else if (tab === 'apikeys') loadApiKeys();
  else                        loadRbacPolicies();
}

/* ── Пользователи (учётки для входа) ── */
async function loadUsers() {
  const el = document.getElementById('settings-users-list');
  if (!el) return;
  try {
    const users = await apiFetch('/api/auth/users');
    if (!Array.isArray(users) || !users.length) {
      el.innerHTML = '';
      return;
    }
    el.innerHTML = users.map(u => {
      const created = u.created_at ? new Date(u.created_at * 1000).toLocaleString('ru') : '—';
      return `
        <div class="pipeline-item" id="urow-${escAttr(u.username)}">
          <div style="flex:1;min-width:0">
            <div class="pipeline-name">${escHtml(u.username || '—')}</div>
            <div class="pipeline-meta">роль: ${escHtml(u.role || '—')} &nbsp;·&nbsp; создан: ${created}</div>
          </div>
          <div class="pipeline-actions">
            <button class="btn btn-sm btn-danger" title="Удалить пользователя" onclick="deleteUser('${escAttr(u.username)}')">✕</button>
          </div>
        </div>`;
    }).join('');
  } catch (err) {
    el.innerHTML = `<div style="color:var(--red);padding:.5rem">Ошибка: ${escHtml(String(err))}</div>`;
  }
}

/* Открыть/закрыть форму создания пользователя (как «+ Витрина»). */
function openCreateUser() {
  const f = document.getElementById('user-create-form');
  if (!f) return;
  const n = document.getElementById('new-user-name');
  const p = document.getElementById('new-user-pass');
  const r = document.getElementById('new-user-role');
  if (n) n.value = '';
  if (p) p.value = '';
  if (r) r.value = 'analyst';
  f.style.display = '';
  if (n) n.focus();
}
function closeCreateUser() {
  const f = document.getElementById('user-create-form');
  if (f) f.style.display = 'none';
}

async function createUser() {
  const nameEl = document.getElementById('new-user-name');
  const passEl = document.getElementById('new-user-pass');
  const roleEl = document.getElementById('new-user-role');
  const username = (nameEl?.value || '').trim();
  const password = passEl?.value || '';
  const role     = roleEl?.value || 'analyst';
  if (!username) { showToast('Введите логин', 'warn'); return; }
  if (!password) { showToast('Введите пароль', 'warn'); return; }
  try {
    await apiPost('/api/auth/users', { username, password, role });
    showToast(`Пользователь «${username}» заведён (роль: ${role})`, 'ok');
    if (nameEl) nameEl.value = '';
    if (passEl) passEl.value = '';
    closeCreateUser();
    loadUsers();
  } catch (err) {
    showToast(`Ошибка: ${err}`, 'error');
  }
}

async function deleteUser(username) {
  if (!confirm(`Удалить пользователя «${username}»?`)) return;
  try {
    await apiDelete(`/api/auth/users/${encodeURIComponent(username)}`);
    showToast('Пользователь удалён', 'ok');
    loadUsers();
  } catch (err) {
    showToast(`Ошибка: ${err}`, 'error');
  }
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
        <td>${rbacActionsLabel(p.allowed_actions)}</td>
        <td>${p.row_filter ? `<code>${escHtml(p.row_filter)}</code>` : '—'}</td>
        <td><button class="btn btn-sm btn-danger" title="Удалить политику" onclick="deleteRbacPolicy(${p.id})">✕</button></td>
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

/* ── Audit ──
   loadAuditLog: однократная загрузка окна событий с сервера в кэш _auditAll.
   renderAuditFiltered: фильтрация кэша по всем полям + постраничный вывод (без сети). */
let _auditAll = [], _auditPage = 0, _auditPerPage = 25;

async function loadAuditLog() {
  const el = document.getElementById('audit-log-list');
  el.innerHTML = '<div class="settings-loading">Загрузка…</div>';
  try {
    const all = await apiFetch('/api/audit?limit=1000');
    _auditAll = Array.isArray(all) ? all : [];
    _auditPage = 0;
    renderAuditFiltered();
  } catch (err) {
    el.innerHTML = `<div class="settings-loading" style="color:var(--red)">${escHtml(String(err))}</div>`;
    document.getElementById('audit-status').textContent = '';
  }
}

/* смена любого фильтра → на первую страницу */
function auditFilterChanged() { _auditPage = 0; renderAuditFiltered(); }
function auditGoPage(p)       { _auditPage = p; renderAuditFiltered(); }
function auditSetPerPage(v)   { _auditPerPage = (v === 'all') ? Infinity : parseInt(v, 10); _auditPage = 0; renderAuditFiltered(); }

function renderAuditFiltered() {
  const el     = document.getElementById('audit-log-list');
  const status = document.getElementById('audit-status');
  const fAction = document.getElementById('audit-filter-action').value;
  const fUser   = document.getElementById('audit-filter-user').value.trim().toLowerCase();
  const fRole   = document.getElementById('audit-filter-role').value;
  const fIp     = document.getElementById('audit-filter-ip').value.trim().toLowerCase();
  const fRes    = document.getElementById('audit-filter-resource').value.trim().toLowerCase();
  const fCode   = document.getElementById('audit-filter-code').value;
  const isOk = c => c === 0 || (c >= 200 && c < 400);
  const list = _auditAll.filter(e =>
    (!fAction     || e.event_type === +fAction) &&
    (!fUser       || (e.user_id   || '').toLowerCase().includes(fUser)) &&
    (fRole === '' || e.role === +fRole) &&
    (!fIp         || (e.client_ip || '').toLowerCase().includes(fIp)) &&
    (!fRes        || (e.resource  || '').toLowerCase().includes(fRes)) &&
    (!fCode       || (fCode === 'ok' ? isOk(e.result_code) : !isOk(e.result_code)))
  );
  status.textContent = `${list.length} из ${_auditAll.length}`;
  if (!list.length) {
    el.innerHTML = '<div class="settings-loading">Событий нет (по фильтрам)</div>';
    return;
  }
  /* пагинация вывода */
  const total = list.length, perPage = _auditPerPage, all = perPage === Infinity;
  const pages = all ? 1 : Math.max(1, Math.ceil(total / perPage));
  if (_auditPage > pages - 1) _auditPage = pages - 1;
  if (_auditPage < 0) _auditPage = 0;
  const start = all ? 0 : _auditPage * perPage;
  const end   = all ? total : Math.min(start + perPage, total);
  const slice = all ? list : list.slice(start, end);

  el.innerHTML = `<table class="result-table" style="width:100%;font-size:.8rem">
    <thead><tr><th>Время</th><th>Действие</th><th>Пользователь</th><th>Роль</th><th>IP</th><th>Ресурс</th><th>Код</th><th>Длит.</th></tr></thead>
    <tbody>${slice.map(e => {
      const okCode = isOk(e.result_code);
      const role   = (e.role >= 0 && RBAC_ROLES[e.role]) ? RBAC_ROLES[e.role] : '—';
      return `<tr>
      <td style="white-space:nowrap">${new Date(e.ts * 1000).toLocaleString('ru')}</td>
      <td><span class="badge ${e.event_type === 5 ? 'badge-danger' : 'badge-ok'}">${AUDIT_TYPES[e.event_type] ?? e.event_type}</span></td>
      <td>${escHtml(e.user_id || '—')}</td>
      <td>${role}</td>
      <td>${escHtml(e.client_ip || '—')}</td>
      <td>${escHtml(e.resource || '—')}</td>
      <td style="white-space:nowrap">${okCode
          ? '<span style="color:var(--green)">✓ ' + e.result_code + '</span>'
          : '<span style="color:var(--red)">✗ ' + e.result_code + '</span>'}</td>
      <td style="white-space:nowrap">${e.duration_ms ?? 0} мс</td>
    </tr>`;
    }).join('')}</tbody></table>` +
    metricsPagerHTML(total, start, end, pages, _auditPage, all, perPage, 'auditGoPage', 'auditSetPerPage');
  sizeListRows('audit-log-list', { cards: false });
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
  let stored; try { stored = localStorage.getItem('dfo_view'); } catch (_) {}
  let v = VALID_VIEWS.has(view) ? view : (VALID_VIEWS.has(stored) ? stored : 'pipelines');
  if (v === 'builder') v = 'pipelines';
  history.replaceState({ view: v }, '', '#' + v);
  switchView(v, { pushState: false });
})();
