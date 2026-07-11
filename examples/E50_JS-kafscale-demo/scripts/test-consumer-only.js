/**
 * Consumer-Only Test
 * Tests if KafkaJS can consume messages from KafScale
 * First send a message with: echo '{"test":"message"}' | kcat -b 127.0.0.1:39092 -t agent_requests -P
 */

import { createConsumer, topics } from '../src/kafka.js'

console.log('📥 KafkaJS Consumer Test')
console.log('=' .repeat(50))
console.log()

async function testConsumer() {
  const consumer = createConsumer('test-consumer-' + Date.now())

  try {
    console.log('📡 Connecting consumer...')
    await consumer.connect()
    console.log('✓ Consumer connected\n')

    console.log(`📥 Subscribing to: ${topics.requests}`)
    await consumer.subscribe({ topic: topics.requests, fromBeginning: true })
    console.log('✓ Subscribed\n')

    console.log('🎧 Waiting for messages...')
    console.log('   (Press Ctrl+C to stop)\n')
    console.log('📋 To send a test message, run in another terminal:')
    console.log(`   echo '{"test":"message","timestamp":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}' | kcat -b 127.0.0.1:39092 -t ${topics.requests} -P`)
    console.log('   or:')
    console.log(`   echo '{"test":"hello"}' | kafka-console-producer --bootstrap-server 127.0.0.1:39092 --topic ${topics.requests}`)
    console.log()

    let messageCount = 0

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        messageCount++

        console.log('─'.repeat(50))
        console.log(`📨 Message #${messageCount} received`)
        console.log('─'.repeat(50))
        console.log(`Topic: ${topic}`)
        console.log(`Partition: ${partition}`)
        console.log(`Offset: ${message.offset}`)
        console.log(`Key: ${message.key?.toString() || 'null'}`)
        console.log(`Value: ${message.value.toString()}`)
        console.log(`Timestamp: ${new Date(Number(message.timestamp)).toISOString()}`)
        console.log()
      }
    })

  } catch (error) {
    console.error('❌ Consumer test failed')
    console.error('Error:', error.message)
    console.error()
    if (error.message.includes('ECONNREFUSED')) {
      console.error('Troubleshooting:')
      console.error('  - Is KafScale running on 127.0.0.1:39092?')
      console.error('  - Check with: lsof -i :39092')
    }
    process.exit(1)

  } finally {
    await consumer.disconnect()
    console.log('✓ Consumer disconnected')
  }
}

// Handle Ctrl+C gracefully
process.on('SIGINT', async () => {
  console.log('\n\n👋 Shutting down...')
  process.exit(0)
})

testConsumer()
