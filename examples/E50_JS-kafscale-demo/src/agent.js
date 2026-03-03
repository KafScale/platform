/**
 * Agent Service
 * Consumes tasks from agent.requests, processes them, and produces responses
 * This demonstrates a stateless, queue-driven agent architecture
 */

import { createConsumer, createProducer, topics, agentConfig } from './kafka.js'
import { callLLM } from './llm.js'

console.log('🤖 Starting Agent Service...')

const consumer = createConsumer('agent-service')
const producer = createProducer()

// Connect to Kafka
try {
  await consumer.connect()
  await producer.connect()
  console.log('✓ Connected to Kafka')
} catch (error) {
  console.error('❌ Failed to connect to Kafka:', error.message)
  process.exit(1)
}

// Subscribe to requests topic
await consumer.subscribe({ topic: topics.requests, fromBeginning: false })
console.log(`✓ Subscribed to: ${topics.requests}`)
console.log(`✓ Publishing to: ${topics.responses}`)
console.log('\n🎧 Agent started, waiting for tasks...\n')

// Process messages
await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    try {
      const req = JSON.parse(message.value.toString())

      console.log('📥 Received task:')
      console.log(`   Correlation ID: ${req.correlationId}`)
      console.log(`   Task: ${req.task}`)
      console.log(`   Spec: ${req.spec}`)
      console.log(`   Context: ${req.context}`)

      // Build prompt according to agent configuration
      const prompt = `
SYSTEM:
${agentConfig.systemPrompt}

CONTEXT:
${req.context}

TASK:
${req.task}

SPEC:
${req.spec}
`

      console.log('\n🔄 Processing with LLM...')

      // Call LLM (stubbed for demo)
      const result = await callLLM(prompt)

      console.log('✓ LLM processing complete')

      // Produce response
      await producer.send({
        topic: topics.responses,
        messages: [{
          key: req.correlationId,
          value: JSON.stringify({
            correlationId: req.correlationId,
            result,
            finishedAt: new Date().toISOString()
          })
        }]
      })

      console.log(`📤 Response sent to: ${topics.responses}`)
      console.log(`   Correlation ID: ${req.correlationId}\n`)
      console.log('🎧 Waiting for next task...\n')

    } catch (error) {
      console.error('❌ Error processing message:', error.message)

      // Optionally send error response
      try {
        const errorReq = JSON.parse(message.value.toString())
        await producer.send({
          topic: topics.responses,
          messages: [{
            key: errorReq.correlationId,
            value: JSON.stringify({
              correlationId: errorReq.correlationId,
              error: error.message,
              finishedAt: new Date().toISOString()
            })
          }]
        })
      } catch (sendError) {
        console.error('Failed to send error response:', sendError.message)
      }
    }
  }
})

// Handle shutdown gracefully
process.on('SIGINT', async () => {
  console.log('\n\n🛑 Shutting down agent service...')
  await consumer.disconnect()
  await producer.disconnect()
  console.log('✓ Disconnected from Kafka')
  process.exit(0)
})
