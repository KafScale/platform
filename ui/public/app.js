const metricFormatter = new Intl.NumberFormat('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 1 });
let topicsCache = [];
let selectedTopicName = null;
let consoleStarted = false;
let metricsSource = null;
let statusInterval = null;

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

function renderBrokers(nodes) {
  const grid = document.getElementById('broker-grid');
  grid.innerHTML = '';
  if (!nodes.length) {
    grid.textContent = 'No broker data';
    return;
  }
  nodes.forEach(node => {
    const card = document.createElement('div');
    card.className = 'broker-card';
    card.innerHTML = `
      <div><span class="status-dot ${node.state}"></span><strong>${node.name}</strong></div>
      <div>State: ${node.state.toUpperCase()}</div>
      <div>Partitions: ${node.partitions}</div>
      <div>CPU: ${node.cpu}% · Mem: ${node.memory}%</div>
      <div>Backpressure: ${node.backpressure.toUpperCase()}</div>
    `;
    grid.appendChild(card);
  });
}

function startMetricsStream() {
  const produceEl = document.getElementById('produce-rps');
  const fetchEl = document.getElementById('fetch-rps');
  const latencyEl = document.getElementById('live-s3-latency');
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

function setMetricValue(el, value) {
  if (!el) return;
  if (typeof value === 'number' && !Number.isNaN(value)) {
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
