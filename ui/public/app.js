const metricFormatter = new Intl.NumberFormat('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 1 });
let topicsCache = [];
let brokersCache = [];
let selectedTopicName = null;
let consoleStarted = false;
let metricsSource = null;
let statusInterval = null;
let brokersExpanded = false;

const loginScreen = document.getElementById('login-screen');
const appScreen = document.getElementById('app');
const loginForm = document.getElementById('login-form');
const loginMessage = document.getElementById('login-message');
const loginError = document.getElementById('login-error');
const loginSubmit = document.getElementById('login-submit');
const logoutButton = document.getElementById('logout-button');

async function fetchStatus() {
  const summary = document.getElementById('cluster-summary');
  try {
    const resp = await fetch('/ui/api/status');
    if (resp.status === 401 || resp.status === 503) {
      showLogin('Session expired. Please sign in again.');
      return;
    }
    if (!resp.ok) throw new Error('status ' + resp.status);
    const data = await resp.json();
    summary.textContent = `${data.cluster} · version ${data.version} · brokers ${data.brokers.ready}/${data.brokers.desired}`;

    renderHealth(data);

    topicsCache = data.topics || [];
    brokersCache = data.brokers?.nodes || [];
    renderTopics(topicsCache);
  } catch (err) {
    summary.textContent = `Failed to load status: ${err.message}`;
  }
}

function renderHealth(data) {
  const container = document.getElementById('health-cards');
  container.innerHTML = '';
  const cards = [
    { title: 'Brokers', value: `${data.brokers.ready}/${data.brokers.desired}` },
    { title: 'S3 State', value: (data.s3.state || 'unknown').toUpperCase() },
    { title: 'S3 Pressure', value: (data.s3.backpressure || 'unknown').toUpperCase() },
    { title: 'etcd', value: (data.etcd.state || 'unknown').toUpperCase() },
    { title: 'Alerts', value: `${(data.alerts || []).length} active` },
  ];
  cards.forEach(card => {
    const div = document.createElement('div');
    div.className = 'card';
    div.innerHTML = `<h3>${card.title}</h3><strong>${card.value}</strong>`;
    container.appendChild(div);
  });

  renderBrokers(data.brokers.nodes || []);

  const alerts = document.getElementById('alerts');
  alerts.innerHTML = '';
  if (!data.alerts || data.alerts.length === 0) {
    const ok = document.createElement('p');
    ok.textContent = 'No active alerts';
    alerts.appendChild(ok);
  } else {
    data.alerts.forEach(alert => {
      const div = document.createElement('div');
      div.className = `alert ${alert.level}`;
      div.textContent = alert.message;
      alerts.appendChild(div);
    });
  }
}

function brokerStateRank(state) {
  const normalized = normalizeBrokerState(state);
  switch (normalized) {
    case 'unavailable':
      return 3;
    case 'degraded':
      return 2;
    case 'healthy':
      return 1;
    default:
      return 0;
  }
}

function normalizeBrokerState(state) {
  if (!state) return 'unknown';
  const value = String(state).trim().toLowerCase();
  if (value === 'ready' || value === 'up' || value === 'ok') return 'healthy';
  return value;
}

function renderBrokerSummary(nodes) {
  const summary = document.getElementById('broker-summary');
  if (!summary) return;
  if (!nodes.length) {
    summary.textContent = 'No broker data';
    const wrap = document.getElementById('broker-table-wrap');
    if (wrap) {
      wrap.style.display = 'none';
    }
    return;
  }
  let cpuMax = 0;
  let memMax = 0;
  let cpuSum = 0;
  let memSum = 0;
  let cpuCount = 0;
  let memCount = 0;
  let healthy = 0;
  let degraded = 0;
  let unavailable = 0;
  nodes.forEach(node => {
    switch (normalizeBrokerState(node.state)) {
      case 'healthy':
        healthy++;
        break;
      case 'degraded':
        degraded++;
        break;
      case 'unavailable':
        unavailable++;
        break;
      default:
        break;
    }
    if (typeof node.cpu === 'number' && node.cpu > 0) {
      cpuSum += node.cpu;
      cpuCount++;
      if (node.cpu > cpuMax) cpuMax = node.cpu;
    }
    if (typeof node.memory === 'number' && node.memory > 0) {
      memSum += node.memory;
      memCount++;
      if (node.memory > memMax) memMax = node.memory;
    }
  });
  const cpuAvg = cpuCount ? Math.round((cpuSum / cpuCount) * 10) / 10 : null;
  const memAvg = memCount ? Math.round((memSum / memCount) * 10) / 10 : null;
  const cpuSummary = cpuCount ? `${cpuAvg}% avg · ${cpuMax}% max` : 'n/a';
  const memSummary = memCount ? `${memAvg} MB avg · ${memMax} MB max` : 'n/a';
  summary.innerHTML = `
    <div><span>State</span><strong>${healthy} healthy · ${degraded} degraded · ${unavailable} unavailable</strong></div>
    <div><span>CPU</span><strong>${cpuSummary}</strong></div>
    <div><span>Mem</span><strong>${memSummary}</strong></div>
  `;
}

function renderBrokers(nodes) {
  renderBrokerSummary(nodes);
  const table = document.getElementById('broker-table');
  const body = table ? table.querySelector('tbody') : null;
  const wrap = document.getElementById('broker-table-wrap');
  if (wrap) {
    wrap.style.display = brokersExpanded ? 'block' : 'none';
  }
  if (!body) return;
  body.innerHTML = '';
  if (!nodes.length) {
    body.innerHTML = '<tr><td colspan="5">No broker data</td></tr>';
    return;
  }
  const sortSelect = document.getElementById('broker-sort');
  const sortKey = sortSelect ? sortSelect.value : 'state';
  const sorted = [...nodes].sort((a, b) => {
    if (sortKey === 'cpu') return (b.cpu ?? 0) - (a.cpu ?? 0);
    if (sortKey === 'mem') return (b.memory ?? 0) - (a.memory ?? 0);
    if (sortKey === 'name') return String(a.name).localeCompare(String(b.name));
    return brokerStateRank(b.state) - brokerStateRank(a.state);
  });
  sorted.forEach(node => {
    const row = document.createElement('tr');
    const cpu = typeof node.cpu === 'number' && node.cpu > 0 ? `${node.cpu}%` : 'n/a';
    const mem = typeof node.memory === 'number' && node.memory > 0 ? `${node.memory} MB` : 'n/a';
    const backpressure = node.backpressure ? node.backpressure.toUpperCase() : 'UNKNOWN';
    const state = normalizeBrokerState(node.state);
    row.innerHTML = `
      <td><span class="status-dot ${state}"></span></td>
      <td>${node.name}</td>
      <td><span class="metric-pill">${cpu}</span></td>
      <td><span class="metric-pill">${mem}</span></td>
      <td>${backpressure}</td>
    `;
    body.appendChild(row);
  });
  const toggle = document.getElementById('broker-toggle');
  if (toggle) {
    toggle.textContent = brokersExpanded ? 'Hide brokers' : `Show brokers (${nodes.length})`;
  }
}

function startMetricsStream() {
  const produceEl = document.getElementById('produce-rps');
  const fetchEl = document.getElementById('fetch-rps');
  const latencyEl = document.getElementById('live-s3-latency');
  const errorRateEl = document.getElementById('s3-error-rate');
  const adminReqEl = document.getElementById('admin-requests');
  const adminErrEl = document.getElementById('admin-errors');
  const adminLatencyEl = document.getElementById('admin-latency');
  const snapshotAgeEl = document.getElementById('etcd-snapshot-age');
  const snapshotStaleEl = document.getElementById('etcd-snapshot-stale');
  const snapshotAccessEl = document.getElementById('etcd-snapshot-access');
  const snapshotLastSuccessEl = document.getElementById('etcd-snapshot-last-success');
  const snapshotLastScheduleEl = document.getElementById('etcd-snapshot-last-schedule');
  const status = document.getElementById('metrics-status');
  if (metricsSource) {
    metricsSource.close();
  }
  metricsSource = new EventSource('/ui/api/metrics');
  metricsSource.onmessage = event => {
    try {
      const payload = JSON.parse(event.data);
      const metrics = payload.metrics || {};
      setMetricValue(produceEl, metrics.produce_rps);
      setMetricValue(fetchEl, metrics.fetch_rps);
      setMetricValue(latencyEl, metrics.s3_latency_ms);
      setMetricValue(errorRateEl, metrics.s3_error_rate);
      setMetricValue(adminReqEl, metrics.admin_requests_total);
      setMetricValue(adminErrEl, metrics.admin_errors_total);
      setMetricValue(adminLatencyEl, metrics.admin_latency_ms_avg);
      setMetricValue(snapshotAgeEl, metrics.etcd_snapshot_age_s);
      setMetricValue(snapshotStaleEl, metrics.etcd_snapshot_stale);
      setMetricValue(snapshotAccessEl, metrics.etcd_snapshot_access_ok);
      setMetricValue(snapshotLastSuccessEl, metrics.etcd_snapshot_last_success_ts, 'timestamp');
      setMetricValue(snapshotLastScheduleEl, metrics.etcd_snapshot_last_schedule_ts, 'timestamp');
      setEtcdFallbacks(metrics);
      if (payload.timestamp) {
        const ts = new Date(payload.timestamp);
        status.textContent = `Last update ${ts.toLocaleTimeString()}`;
      } else {
        status.textContent = 'Live metrics';
      }
    } catch (err) {
      status.textContent = 'Metrics parse error';
    }
  };
  metricsSource.onerror = () => {
    status.textContent = 'Stream disconnected';
    metricsSource.close();
  };
}

function setEtcdFallbacks(metrics) {
  const ageEl = document.getElementById('etcd-snapshot-age');
  const staleEl = document.getElementById('etcd-snapshot-stale');
  const accessEl = document.getElementById('etcd-snapshot-access');
  const lastSuccessEl = document.getElementById('etcd-snapshot-last-success');
  const lastScheduleEl = document.getElementById('etcd-snapshot-last-schedule');
  if (!ageEl || !staleEl || !accessEl || !lastSuccessEl || !lastScheduleEl) return;
  if (typeof metrics.etcd_snapshot_age_s !== 'number') {
    ageEl.textContent = 'n/a';
    staleEl.textContent = 'n/a';
    accessEl.textContent = 'n/a';
    lastSuccessEl.textContent = 'n/a';
    lastScheduleEl.textContent = 'n/a';
  }
}

function setMetricValue(el, value, mode) {
  if (!el) return;
  if (typeof value === 'number' && !Number.isNaN(value)) {
    if (el.id === 's3-error-rate') {
      el.textContent = `${metricFormatter.format(value * 100)}%`;
      return;
    }
    if (mode === 'timestamp') {
      if (value <= 0) {
        el.textContent = 'n/a';
        return;
      }
      el.textContent = new Date(value * 1000).toLocaleString();
      return;
    }
    if (el.id === 'etcd-snapshot-stale') {
      el.textContent = value >= 1 ? 'YES' : 'NO';
      return;
    }
    if (el.id === 'etcd-snapshot-access') {
      el.textContent = value >= 1 ? 'OK' : 'FAIL';
      return;
    }
    el.textContent = metricFormatter.format(value);
  }
}

function renderTopics(topics) {
  const tableBody = document.querySelector('#topic-table tbody');
  tableBody.innerHTML = '';
  if (!topics.length) {
    tableBody.innerHTML = '<tr><td colspan="3">No topics</td></tr>';
    renderTopicDetail(null);
    return;
  }
  if (!selectedTopicName || !topics.some(t => t.name === selectedTopicName)) {
    selectedTopicName = topics[0].name;
  }
  topics.forEach(topic => {
    const row = document.createElement('tr');
    row.dataset.topic = topic.name;
    row.innerHTML = `<td>${topic.name}</td><td>${topic.partitions}</td><td>${topic.state}</td>`;
    if (topic.name === selectedTopicName) {
      row.classList.add('selected');
    }
    row.addEventListener('click', () => {
      selectedTopicName = topic.name;
      updateTopicSelection();
      renderTopicDetail(topic);
    });
    tableBody.appendChild(row);
  });
  const selected = topics.find(t => t.name === selectedTopicName) || null;
  renderTopicDetail(selected);
}

function updateTopicSelection() {
  document.querySelectorAll('#topic-table tbody tr').forEach(row => {
    if (row.dataset.topic === selectedTopicName) {
      row.classList.add('selected');
    } else {
      row.classList.remove('selected');
    }
  });
}

function renderTopicDetail(topic) {
  const panel = document.getElementById('topic-detail');
  if (!panel) return;
  if (!topic) {
    panel.innerHTML = '<p>Select a topic to inspect partition layout.</p>';
    return;
  }
  const partitions = topic.partitions_details || [];
  const header = `
    <div class="topic-header">
      <h3>${topic.name}</h3>
      <p>${topic.partitions} partitions · State ${topic.state.toUpperCase()}</p>
    </div>
  `;
  if (!partitions.length) {
    panel.innerHTML = header + '<p>No partition metadata reported.</p>';
    return;
  }
  const rows = partitions.map(part => `
    <tr>
      <td>${part.id}</td>
      <td>${part.leader}</td>
      <td>${part.replicas}</td>
      <td>${part.isr}</td>
    </tr>
  `).join('');
  panel.innerHTML = `
    ${header}
    <table class="detail-table">
      <thead>
        <tr><th>Partition</th><th>Leader</th><th>Replicas</th><th>ISR</th></tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function showLogin(message) {
  consoleStarted = false;
  clearLoginAlerts();
  loginScreen.classList.add('screen-active');
  appScreen.classList.remove('screen-active');
  if (metricsSource) {
    metricsSource.close();
    metricsSource = null;
  }
  if (statusInterval) {
    clearInterval(statusInterval);
    statusInterval = null;
  }
  if (message) {
    loginMessage.textContent = message;
    loginMessage.classList.add('show');
  }
}

function showLoginError(message) {
  if (!message) return;
  loginError.textContent = message;
  loginError.classList.add('show');
}

function clearLoginAlerts() {
  loginError.textContent = '';
  loginError.classList.remove('show');
  loginMessage.classList.remove('show');
}

function showApp() {
  loginScreen.classList.remove('screen-active');
  appScreen.classList.add('screen-active');
}

async function fetchAuthConfig() {
  try {
    const resp = await fetch('/ui/api/auth/config');
    if (!resp.ok) {
      return { enabled: false, message: 'Unable to load auth configuration.' };
    }
    return await resp.json();
  } catch (err) {
    return { enabled: false, message: 'Unable to reach auth service.' };
  }
}

async function fetchSession() {
  try {
    const resp = await fetch('/ui/api/auth/session');
    if (!resp.ok) return { authenticated: false };
    return await resp.json();
  } catch (err) {
    return { authenticated: false };
  }
}

async function startConsole() {
  if (consoleStarted) return;
  consoleStarted = true;
  fetchStatus();
  startMetricsStream();
  statusInterval = setInterval(fetchStatus, 10000);
}

async function initAuth() {
  const config = await fetchAuthConfig();
  if (!config.enabled) {
    showLogin(config.message || 'UI access is disabled until credentials are set.');
    if (loginSubmit) {
      loginSubmit.disabled = true;
    }
    if (loginForm) {
      loginForm.querySelectorAll('input').forEach(input => {
        input.disabled = true;
      });
    }
    return;
  }
  const session = await fetchSession();
  if (session.authenticated) {
    showApp();
    startConsole();
    return;
  }
  showLogin();
}

const brokerSort = document.getElementById('broker-sort');
if (brokerSort) {
  brokerSort.addEventListener('change', () => {
    if (brokersExpanded) {
      renderBrokers(brokersCache);
    }
  });
}

const brokerToggle = document.getElementById('broker-toggle');
if (brokerToggle) {
  brokerToggle.addEventListener('click', () => {
    if (!brokersExpanded) {
      brokersExpanded = true;
    } else {
      brokersExpanded = false;
    }
    renderBrokers(brokersCache);
  });
}

if (loginForm) {
  loginForm.addEventListener('submit', async event => {
    event.preventDefault();
    clearLoginAlerts();
    if (!loginSubmit) return;
    loginSubmit.disabled = true;
    loginSubmit.textContent = 'Signing in…';
    const username = document.getElementById('login-username').value.trim();
    const password = document.getElementById('login-password').value;
    try {
      const resp = await fetch('/ui/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password }),
      });
      if (!resp.ok) {
        if (resp.status === 503) {
          showLogin('UI access is disabled until credentials are set.');
        } else {
          showLoginError('Invalid username or password.');
        }
        return;
      }
      const data = await resp.json();
      if (data.authenticated) {
        showApp();
        startConsole();
      } else {
        showLoginError(data.message || 'Login failed.');
      }
    } catch (err) {
      showLoginError('Login failed. Please try again.');
    } finally {
      loginSubmit.disabled = false;
      loginSubmit.textContent = 'Enter Console';
    }
  });
}

if (logoutButton) {
  logoutButton.addEventListener('click', async () => {
    try {
      await fetch('/ui/api/auth/logout', { method: 'POST' });
    } catch (err) {
      // Ignore logout network errors and force local state.
    }
    showLogin('Signed out. Please sign in again.');
  });
}

initAuth();

document.querySelectorAll('.tab-button').forEach(button => {
  button.addEventListener('click', () => {
    const target = button.getAttribute('data-tab');
    document.querySelectorAll('.tab-button').forEach(btn => {
      btn.classList.toggle('active', btn === button);
    });
    document.querySelectorAll('.tab-panel').forEach(panel => {
      panel.classList.toggle('active', panel.id === `tab-${target}`);
    });
  });
});
