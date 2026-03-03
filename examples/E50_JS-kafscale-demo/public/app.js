/**
 * Agent Kanban Board - Frontend Application
 * Manages task board state, drag-and-drop, and WebSocket communication
 */

class AgentKanban {
  constructor() {
    this.ws = null
    this.tasks = new Map()
    this.agentHistory = []
    this.reconnectAttempts = 0
    this.maxReconnectAttempts = 5

    this.init()
  }

  init() {
    this.connectWebSocket()
    this.setupEventListeners()
    this.setupDragAndDrop()
  }

  // WebSocket Connection
  connectWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const wsUrl = `${protocol}//${window.location.host}`

    console.log('Connecting to WebSocket:', wsUrl)
    this.ws = new WebSocket(wsUrl)

    this.ws.onopen = () => {
      console.log('✓ Connected to server')
      this.reconnectAttempts = 0
      this.updateAgentStatus(true)
      this.showToast('Connected to server', 'success')
    }

    this.ws.onmessage = (event) => {
      const message = JSON.parse(event.data)
      this.handleServerMessage(message)
    }

    this.ws.onclose = () => {
      console.log('Disconnected from server')
      this.updateAgentStatus(false)
      this.attemptReconnect()
    }

    this.ws.onerror = (error) => {
      console.error('WebSocket error:', error)
      this.showToast('Connection error', 'error')
    }
  }

  attemptReconnect() {
    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++
      const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 10000)
      console.log(`Reconnecting in ${delay}ms... (attempt ${this.reconnectAttempts})`)
      setTimeout(() => this.connectWebSocket(), delay)
    } else {
      this.showToast('Connection lost. Please refresh the page.', 'error')
    }
  }

  handleServerMessage(message) {
    const knownTypes = ['connected', 'task-created', 'task-response', 'agent-activity', 'error']
    const type = knownTypes.includes(message.type) ? message.type : 'unknown'
    console.log('Received:', type)

    switch (type) {
      case 'connected':
        console.log('Server connected')
        break

      case 'task-created':
        this.addTask(message.data, 'backlog')
        this.showToast('Task created', 'success')
        break

      case 'task-response':
        this.updateTaskWithResponse(message.data)
        break

      case 'agent-activity':
        this.updateAgentActivity(message.data)
        break

      case 'error':
        this.showToast(message.message, 'error')
        break
    }
  }

  // Task Management
  addTask(taskData, lane) {
    const task = {
      ...taskData,
      lane,
      status: lane === 'backlog' ? 'pending' : lane,
      result: null
    }

    this.tasks.set(task.correlationId, task)
    this.renderTask(task)
    this.updateCounts()
  }

  updateTaskWithResponse(response) {
    const task = this.tasks.get(response.correlationId)
    if (!task) return

    task.result = response.result
    task.error = response.error
    task.finishedAt = response.finishedAt
    task.status = 'completed'

    // Move to completed lane
    this.moveTask(task.correlationId, 'completed')

    // Add to agent history
    this.addToHistory(task)

    this.renderTask(task)
    this.updateCounts()
  }

  moveTask(correlationId, toLane) {
    const task = this.tasks.get(correlationId)
    if (!task) return

    task.lane = toLane
    task.status = toLane === 'backlog' ? 'pending' : toLane

    // Remove from old lane
    document.querySelectorAll('.card').forEach(card => {
      if (card.dataset.id === correlationId) {
        card.remove()
      }
    })

    // Add to new lane
    this.renderTask(task)
    this.updateCounts()

    // If moved to processing, send to Kafka (already done via drag)
    if (toLane === 'processing' && task.status === 'pending') {
      task.status = 'processing'
      this.showAgentProcessing(task)
    }
  }

  renderTask(task) {
    const card = this.createTaskCard(task)
    const container = document.getElementById(`${task.lane}-cards`)

    // Remove existing card if present
    const existing = container.querySelector(`[data-id="${task.correlationId}"]`)
    if (existing) existing.remove()

    container.appendChild(card)
  }

  createTaskCard(task) {
    const card = document.createElement('div')
    card.className = 'card'
    card.draggable = true
    card.dataset.id = task.correlationId

    const validStatuses = ['pending', 'processing', 'completed', 'backlog']
    const statusClass = validStatuses.includes(task.status) ? task.status : 'pending'
    const statusText = statusClass.charAt(0).toUpperCase() + statusClass.slice(1)

    const header = document.createElement('div')
    header.className = 'card-header'
    const title = document.createElement('div')
    title.className = 'card-title'
    title.textContent = task.task
    header.appendChild(title)

    const body = document.createElement('div')
    body.className = 'card-body'
    const specLabel = document.createElement('strong')
    specLabel.textContent = 'Spec: '
    body.appendChild(specLabel)
    body.appendChild(document.createTextNode(task.spec || 'None'))
    body.appendChild(document.createElement('br'))
    const ctxLabel = document.createElement('strong')
    ctxLabel.textContent = 'Context: '
    body.appendChild(ctxLabel)
    body.appendChild(document.createTextNode(task.context || 'None'))

    const meta = document.createElement('div')
    meta.className = 'card-meta'
    const statusSpan = document.createElement('span')
    statusSpan.className = `card-status ${statusClass}`
    statusSpan.textContent = statusText
    const idSpan = document.createElement('span')
    idSpan.className = 'card-id'
    idSpan.textContent = task.correlationId.slice(0, 8)
    meta.appendChild(statusSpan)
    meta.appendChild(idSpan)

    card.appendChild(header)
    card.appendChild(body)
    card.appendChild(meta)

    if (task.result) {
      const result = document.createElement('div')
      result.className = 'card-result'
      result.textContent = task.result.substring(0, 200) + '...'
      card.appendChild(result)
    }

    return card
  }

  // Agent Activity Display
  showAgentProcessing(task) {
    const currentTaskEl = document.getElementById('current-task')
    currentTaskEl.className = 'current-task active'
    currentTaskEl.innerHTML = `
      <div><strong>Task:</strong> ${this.escapeHtml(task.task)}</div>
      <div><strong>Spec:</strong> ${this.escapeHtml(task.spec || 'None')}</div>
      <div><strong>Context:</strong> ${this.escapeHtml(task.context || 'None')}</div>
    `

    const promptEl = document.getElementById('agent-prompt')
    promptEl.innerHTML = `<code>SYSTEM: You are a helpful technical writing agent.

CONTEXT:
${task.context}

TASK:
${task.task}

SPEC:
${task.spec}</code>`

    const llmStatus = document.getElementById('llm-status')
    llmStatus.className = 'llm-status processing'
    llmStatus.innerHTML = `
      <div class="status-indicator"></div>
      <span>Processing with LLM...</span>
    `
  }

  addToHistory(task) {
    this.agentHistory.unshift(task)
    if (this.agentHistory.length > 10) this.agentHistory.pop()

    const historyEl = document.getElementById('task-history')
    historyEl.innerHTML = this.agentHistory.map(t => `
      <div class="history-item">
        <div class="time">${new Date(t.finishedAt).toLocaleTimeString()}</div>
        <div class="task-name">${this.escapeHtml(t.task)}</div>
        <div>${t.error ? '❌ Error' : '✓ Completed'}</div>
      </div>
    `).join('')

    // Clear current task display
    document.getElementById('current-task').className = 'current-task empty'
    document.getElementById('current-task').innerHTML = '<p>Waiting for tasks...</p>'

    const llmStatus = document.getElementById('llm-status')
    llmStatus.className = 'llm-status idle'
    llmStatus.innerHTML = `
      <div class="status-indicator"></div>
      <span>Idle</span>
    `
  }

  updateAgentActivity(_activity) {
    // Handle real-time agent activity updates
    console.log('Agent activity received')
  }

  // UI Updates
  updateAgentStatus(online) {
    const statusEl = document.getElementById('agent-status')
    if (online) {
      statusEl.innerHTML = '<span class="status-dot online"></span><span>Agent Online</span>'
    } else {
      statusEl.innerHTML = '<span class="status-dot offline"></span><span>Agent Offline</span>'
    }
  }

  updateCounts() {
    const counts = {
      backlog: 0,
      processing: 0,
      completed: 0,
      total: 0
    }

    const validLanes = new Set(['backlog', 'processing', 'completed'])
    this.tasks.forEach(task => {
      if (validLanes.has(task.lane)) {
        counts[task.lane]++
      }
      counts.total++
    })

    document.getElementById('task-count').textContent = counts.total
    document.getElementById('completed-count').textContent = counts.completed

    document.querySelectorAll('.lane').forEach(lane => {
      const laneName = lane.dataset.lane
      const count = counts[laneName] || 0
      lane.querySelector('.count').textContent = count
    })
  }

  // Drag and Drop
  setupDragAndDrop() {
    document.addEventListener('dragstart', (e) => {
      if (e.target.classList.contains('card')) {
        e.target.classList.add('dragging')
        e.dataTransfer.effectAllowed = 'move'
        e.dataTransfer.setData('text/plain', e.target.dataset.id)
      }
    })

    document.addEventListener('dragend', (e) => {
      if (e.target.classList.contains('card')) {
        e.target.classList.remove('dragging')
      }
    })

    document.querySelectorAll('.cards').forEach(container => {
      container.addEventListener('dragover', (e) => {
        e.preventDefault()
        e.dataTransfer.dropEffect = 'move'
      })

      container.addEventListener('drop', (e) => {
        e.preventDefault()
        const taskId = e.dataTransfer.getData('text/plain')
        const toLane = e.target.closest('.lane').dataset.lane

        this.moveTask(taskId, toLane)
      })
    })
  }

  // Event Listeners
  setupEventListeners() {
    // New Task Button
    document.getElementById('new-task-btn').addEventListener('click', () => {
      this.showTaskModal()
    })

    // Close Modal
    document.getElementById('close-modal').addEventListener('click', () => {
      this.hideTaskModal()
    })

    document.getElementById('cancel-btn').addEventListener('click', () => {
      this.hideTaskModal()
    })

    // Task Form Submit
    document.getElementById('task-form').addEventListener('submit', (e) => {
      e.preventDefault()
      this.createTask()
    })

    // Click outside modal to close
    document.getElementById('task-modal').addEventListener('click', (e) => {
      if (e.target.id === 'task-modal') {
        this.hideTaskModal()
      }
    })
  }

  showTaskModal() {
    document.getElementById('task-modal').classList.add('active')
    document.getElementById('task-input').focus()
  }

  hideTaskModal() {
    document.getElementById('task-modal').classList.remove('active')
    document.getElementById('task-form').reset()
  }

  createTask() {
    const task = document.getElementById('task-input').value.trim()
    const spec = document.getElementById('spec-input').value.trim()
    const context = document.getElementById('context-input').value.trim()

    if (!task) {
      this.showToast('Task description is required', 'error')
      return
    }

    const taskData = {
      correlationId: this.generateId(),
      task,
      spec: spec || 'No specific requirements',
      context: context || 'General context',
      createdAt: new Date().toISOString()
    }

    // Send to server
    this.ws.send(JSON.stringify({
      type: 'create-task',
      data: taskData
    }))

    this.hideTaskModal()
  }

  // Utilities
  generateId() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
      const r = Math.random() * 16 | 0
      const v = c === 'x' ? r : (r & 0x3 | 0x8)
      return v.toString(16)
    })
  }

  escapeHtml(text) {
    const div = document.createElement('div')
    div.textContent = text
    return div.innerHTML
  }

  showToast(message, type = 'success') {
    const toast = document.getElementById('connection-toast')
    const messageEl = document.getElementById('toast-message')

    messageEl.textContent = message
    toast.className = `toast ${type}`

    setTimeout(() => {
      toast.classList.add('hidden')
    }, 3000)
  }
}

// Initialize the application
new AgentKanban()
