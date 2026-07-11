/**
 * Response Consumer
 * Monitors agent.responses topic and displays results
 * This represents the human panel receiving agent outputs
 */

import { createConsumer, topics } from '../src/kafka.js'

console.log('👁️  Response Consumer')
console.log('===================\n')

const consumer = createConsumer('panel-response')

try {
  await consumer.connect()
  console.log('✓ Connected to Kafka')
} catch (error) {
  console.error('❌ Failed to connect to Kafka:', error.message)
  console.error('Make sure Kafka/KafScale is running on localhost:9092')
  process.exit(1)
}

await consumer.subscribe({ topic: topics.responses, fromBeginning: false })
console.log(`✓ Subscribed to: ${topics.responses}`)
console.log('\n🎧 Waiting for agent responses...\n')

await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    try {
      const response = JSON.parse(message.value.toString())

      console.log('═══════════════════════════════════════════════════════')
      console.log('📨 AGENT RESPONSE RECEIVED')
      console.log('═══════════════════════════════════════════════════════')
      console.log(`Correlation ID: ${response.correlationId}`)
      console.log(`Finished At:    ${response.finishedAt}`)

      if (response.error) {
        console.log('\n❌ ERROR:')
        console.log(response.error)
      } else {
        console.log('\n📝 RESULT:')
        console.log(response.result)
      }

      console.log('═══════════════════════════════════════════════════════\n')

    } catch (error) {
      console.error('❌ Error parsing response:', error.message)
      console.error('Raw message:', message.value.toString())
    }
  }
})

// Handle shutdown gracefully
process.on('SIGINT', async () => {
  console.log('\n\n🛑 Shutting down response consumer...')
  await consumer.disconnect()
  console.log('✓ Disconnected from Kafka')
  process.exit(0)
})
