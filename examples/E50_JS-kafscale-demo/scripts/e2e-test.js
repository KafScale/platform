/**
 * End-to-End Test
 * Tests the complete agent workflow: produce task → agent processes → consume response
 *
 * IMPORTANT: This test uses KafkaJS (not Java client)
 * - Uses the same kafka.js configuration as the rest of the demo
 * - Includes all KafScale-compatible settings (metadata refresh, retries, etc.)
 * - This proves KafkaJS works with KafScale when configured correctly
 */

import { createProducer, createConsumer, topics } from '../src/kafka.js'
import { createAgentRequest } from '../src/types.js'
import crypto from 'crypto'

const TEST_TIMEOUT = 60000 // 60 seconds
const correlationId = crypto.randomUUID()
let startTime

console.log('🧪 E2E Test: Agent Workflow')
console.log('=' .repeat(50))
console.log()

async function runTest() {
  const producer = createProducer()
  const consumer = createConsumer('e2e-test-consumer')

  let testPassed = false
  let responseReceived = false

  try {
    // Connect producer and consumer
    console.log('📡 Connecting to Kafka...')
    await producer.connect()
    await consumer.connect()
    console.log('✓ Connected\n')

    // Subscribe to responses
    console.log(`📥 Subscribing to: ${topics.responses}`)
    await consumer.subscribe({ topic: topics.responses, fromBeginning: false })
    console.log('✓ Subscribed\n')

    // Set up consumer to listen for response
    const responsePromise = new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error(`Timeout: No response received after ${TEST_TIMEOUT}ms`))
      }, TEST_TIMEOUT)

      consumer.run({
        eachMessage: async ({ message }) => {
          const response = JSON.parse(message.value.toString())

          if (response.correlationId === correlationId) {
            clearTimeout(timeout)
            responseReceived = true

            const elapsed = Date.now() - startTime

            console.log('✓ Response received!')
            console.log(`   Correlation ID: ${response.correlationId}`)
            console.log(`   Time elapsed: ${elapsed}ms`)
            console.log(`   Has result: ${!!response.result}`)
            console.log(`   Has error: ${!!response.error}`)

            if (response.error) {
              console.log(`\n❌ Agent returned error: ${response.error}`)
              reject(new Error('Agent processing failed'))
            } else if (!response.result) {
              console.log('\n❌ Response missing result field')
              reject(new Error('Invalid response format'))
            } else {
              console.log('\n📄 Result preview:')
              console.log('─'.repeat(50))
              console.log(response.result.substring(0, 200))
              console.log('─'.repeat(50))
              resolve(response)
            }
          }
        }
      })
    })

    // Give consumer time to be ready
    await new Promise(resolve => setTimeout(resolve, 2000))

    // Send test task
    console.log('📤 Sending test task...')
    const task = createAgentRequest({
      task: 'Explain what Kafka topics are',
      spec: 'Brief explanation, 2-3 sentences',
      context: 'For beginners learning Kafka',
      correlationId
    })

    console.log(`   Correlation ID: ${correlationId}`)
    console.log(`   Topic: ${topics.requests}`)

    startTime = Date.now()

    await producer.send({
      topic: topics.requests,
      messages: [{
        key: correlationId,
        value: JSON.stringify(task)
      }]
    })

    console.log('✓ Task sent\n')
    console.log('⏳ Waiting for agent to process...\n')

    // Wait for response
    const response = await responsePromise

    // Test passed!
    testPassed = true

    console.log('\n' + '='.repeat(50))
    console.log('✅ E2E TEST PASSED')
    console.log('='.repeat(50))
    console.log()
    console.log('Summary:')
    console.log('  ✓ Producer connected')
    console.log('  ✓ Consumer subscribed')
    console.log('  ✓ Task sent to agent')
    console.log('  ✓ Agent processed task')
    console.log('  ✓ Response received')
    console.log('  ✓ Response format valid')
    console.log()

  } catch (error) {
    console.error('\n' + '='.repeat(50))
    console.error('❌ E2E TEST FAILED')
    console.error('='.repeat(50))
    console.error()
    console.error('Error:', error.message)

    if (!responseReceived) {
      console.error()
      console.error('Troubleshooting:')
      console.error('  1. Is the agent running? (make run-agent)')
      console.error('  2. Are topics created? (make setup)')
      console.error('  3. Is KafScale running correctly?')
      console.error('  4. Check agent logs for errors')
    }

    console.error()
    process.exit(1)

  } finally {
    // Cleanup
    await producer.disconnect()
    await consumer.disconnect()
  }

  if (!testPassed) {
    process.exit(1)
  }
}

// Run the test
runTest().catch(error => {
  console.error('Fatal error:', error)
  process.exit(1)
})
