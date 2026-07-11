/**
 * Web Server with WebSocket Bridge
 * Serves the Kanban UI and bridges WebSocket <-> Kafka
 */

import express from 'express'
import { WebSocketServer } from 'ws'
import { createServer } from 'http'
import path from 'path'
import { fileURLToPath } from 'url'
import { createProducer, createConsumer, topics } from './kafka.js'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

const app = express()
const server = createServer(app)
const wss = new WebSocketServer({ server })

const PORT = process.env.PORT || 3000

// Serve static files
app.use(express.static(path.join(__dirname, '../public')))

// Kafka clients
let producer
let responseConsumer

// Track connected clients
const clients = new Set()

// Initialize Kafka connections
async function initKafka() {
  try {
    producer = createProducer()
    await producer.connect()
    console.log('✓ Producer connected to Kafka')

    responseConsumer = createConsumer('web-ui-responses')
    await responseConsumer.connect()
    await responseConsumer.subscribe({ topic: topics.responses, fromBeginning: false })
    console.log('✓ Consumer subscribed to responses topic')

    // Consume responses and broadcast to all connected clients
    responseConsumer.run({
      eachMessage: async ({ message }) => {
        const response = JSON.parse(message.value.toString())
        broadcast({
          type: 'task-response',
          data: response
        })
      }
    })

  } catch (error) {
    console.error('❌ Failed to initialize Kafka:', error.message)
    process.exit(1)
  }
}

// WebSocket connection handler
wss.on('connection', (ws) => {
  console.log('🔌 Client connected')
  clients.add(ws)

  // Send connection success
  ws.send(JSON.stringify({
    type: 'connected',
    message: 'Connected to Agent Kanban'
  }))

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data)

      switch (message.type) {
        case 'create-task':
          await handleCreateTask(message.data, ws)
          break

        case 'ping':
          ws.send(JSON.stringify({ type: 'pong' }))
          break

        default:
          console.warn('Unknown message type:', message.type)
      }
    } catch (error) {
      console.error('Error handling message:', error)
      ws.send(JSON.stringify({
        type: 'error',
        message: error.message
      }))
    }
  })

  ws.on('close', () => {
    console.log('🔌 Client disconnected')
    clients.delete(ws)
  })

  ws.on('error', (error) => {
    console.error('WebSocket error:', error)
    clients.delete(ws)
  })
})

// Handle task creation
async function handleCreateTask(taskData, ws) {
  try {
    const task = {
      correlationId: taskData.correlationId,
      task: taskData.task,
      spec: taskData.spec,
      context: taskData.context,
      createdAt: new Date().toISOString()
    }

    await producer.send({
      topic: topics.requests,
      messages: [{
        key: task.correlationId,
        value: JSON.stringify(task)
      }]
    })

    console.log('✓ Task sent to Kafka:', task.correlationId)

    // Confirm to sender
    ws.send(JSON.stringify({
      type: 'task-created',
      data: task
    }))

    // Broadcast to all clients
    broadcast({
      type: 'task-created',
      data: task
    })

  } catch (error) {
    console.error('Error creating task:', error)
    ws.send(JSON.stringify({
      type: 'error',
      message: 'Failed to create task: ' + error.message
    }))
  }
}

// Broadcast message to all connected clients
function broadcast(message) {
  const data = JSON.stringify(message)
  clients.forEach(client => {
    if (client.readyState === 1) { // OPEN
      client.send(data)
    }
  })
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Shutting down web server...')

  clients.forEach(client => client.close())

  if (producer) await producer.disconnect()
  if (responseConsumer) await responseConsumer.disconnect()

  server.close(() => {
    console.log('✓ Server closed')
    process.exit(0)
  })
})

// Start server
async function start() {
  await initKafka()

  server.listen(PORT, () => {
    console.log(`
🚀 Web Server Started
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  URL: http://localhost:${PORT}

  Open this URL in your browser to access
  the Agent Kanban Board interface.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
`)
  })
}

start()
