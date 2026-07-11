/**
 * Human Panel (CLI)
 * Interactive interface for submitting tasks to agents
 * Produces messages to agent.requests topic
 */

import readline from 'readline'
import { createProducer, topics } from './kafka.js'
import { createAgentRequest } from './types.js'
import crypto from 'crypto'

console.log('👤 Human Panel - Task Submission')
console.log('=================================\n')

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
})

const producer = createProducer()

try {
  await producer.connect()
  console.log('✓ Connected to Kafka\n')
} catch (error) {
  console.error('❌ Failed to connect to Kafka:', error.message)
  console.error('Make sure Kafka/KafScale is running on localhost:9092')
  process.exit(1)
}

// Helper to prompt for input
const question = (prompt) => {
  return new Promise((resolve) => {
    rl.question(prompt, resolve)
  })
}

async function submitTask() {
  console.log('Enter task details (press Ctrl+C to exit):\n')

  const task = await question('Task: ')
  if (!task.trim()) {
    console.log('❌ Task cannot be empty')
    rl.close()
    await producer.disconnect()
    return
  }

  const spec = await question('Spec: ')
  const context = await question('Context: ')

  const correlationId = crypto.randomUUID()

  const msg = createAgentRequest({
    task,
    spec: spec || 'No specific requirements',
    context: context || 'General context',
    correlationId
  })

  console.log('\n📤 Submitting task...')

  try {
    await producer.send({
      topic: topics.requests,
      messages: [{
        key: correlationId,
        value: JSON.stringify(msg)
      }]
    })

    console.log('✓ Task submitted successfully!')
    console.log(`   Topic: ${topics.requests}`)
    console.log(`   Correlation ID: ${correlationId}`)
    console.log('\n💡 Monitor responses with: make run-consumer')
    console.log('   or: node scripts/consume-response.js\n')

  } catch (error) {
    console.error('❌ Failed to send task:', error.message)
  }

  rl.close()
  await producer.disconnect()
}

// Handle Ctrl+C gracefully
process.on('SIGINT', async () => {
  console.log('\n\n👋 Goodbye!')
  rl.close()
  await producer.disconnect()
  process.exit(0)
})

// Run the panel
submitTask()
