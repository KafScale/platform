async function fetchStatus() {
  const summary = document.getElementById('cluster-summary');
  const tableBody = document.querySelector('#topic-table tbody');
  try {
    const resp = await fetch('/ui/api/status');
    if (!resp.ok) throw new Error('status ' + resp.status);
    const data = await resp.json();
    summary.textContent = `${data.cluster} · version ${data.version} · brokers ${data.brokers.ready}/${data.brokers.desired}`;

    renderHealth(data);

    tableBody.innerHTML = '';
    data.topics.forEach(topic => {
      const row = document.createElement('tr');
      row.innerHTML = `<td>${topic.name}</td><td>${topic.partitions}</td><td>${topic.state}</td><td><button data-topic="${topic.name}">Delete</button></td>`;
      row.querySelector('button').addEventListener('click', () => deleteTopic(topic.name));
      tableBody.appendChild(row);
    });
  } catch (err) {
    summary.textContent = `Failed to load status: ${err.message}`;
  }
}

function renderHealth(data) {
  const container = document.getElementById('health-cards');
  container.innerHTML = '';
  const cards = [
    { title: 'Brokers', value: `${data.brokers.ready}/${data.brokers.desired}`, state: data.brokers.ready === data.brokers.desired ? 'healthy' : 'degraded' },
    { title: 'S3', value: data.s3.state.toUpperCase(), state: data.s3.state },
    { title: 'S3 Latency (ms)', value: data.s3.latency_ms, state: data.s3.state },
    { title: 'S3 Pressure', value: data.s3.backpressure.toUpperCase(), state: data.s3.backpressure },
    { title: 'etcd', value: data.etcd.state.toUpperCase(), state: data.etcd.state === 'connected' ? 'healthy' : 'degraded' },
  ];
  cards.forEach(card => {
    const div = document.createElement('div');
    div.className = `card ${card.state}`;
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

async function createTopic() {
  const name = document.getElementById('topic-name').value.trim();
  const partitions = parseInt(document.getElementById('topic-partitions').value, 10) || 1;
  if (!name) return;
  await fetch('/ui/api/status/topics', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, partitions })
  });
  document.getElementById('topic-name').value = '';
  fetchStatus();
}

async function deleteTopic(name) {
  await fetch(`/ui/api/status/topics/${encodeURIComponent(name)}`, { method: 'DELETE' });
  fetchStatus();
}

function startMetricsStream() {
  const target = document.getElementById('metric-stream');
  const source = new EventSource('/ui/api/metrics');
  source.onmessage = event => {
    target.textContent = event.data;
  };
  source.onerror = () => {
    target.textContent = 'Stream disconnected';
    source.close();
  };
}

fetchStatus();
document.getElementById('create-topic').addEventListener('click', createTopic);
startMetricsStream();
setInterval(fetchStatus, 10000);
