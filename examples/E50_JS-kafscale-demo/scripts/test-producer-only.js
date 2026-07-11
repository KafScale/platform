/**
 * Producer-Only Test
 * Tests if KafkaJS can produce messages to KafScale
 * Use with: kcat -b 127.0.0.1:39092 -t agent_requests -C
 */

import { createProducer, topics } from '../src/kafka.js'
import { createAgentRequest } from '../src/types.js'
import crypto from 'crypto'

console.log('📤 KafkaJS Producer Test')
console.log('=' .repeat(50))
console.log()

async function testProducer() {
  const producer = createProducer()

  try {
    console.log('📡 Connecting producer...')
    await producer.connect()
    console.log('✓ Producer connected\n')

    const correlationId = crypto.randomUUID()
    const message = createAgentRequest({
      task: 'Test task from KafkaJS producer',
      spec: 'This is a test message',
      context: 'Testing producer functionality',
      correlationId
    })

    console.log('📤 Sending message...')
    console.log(`   Topic: ${topics.requests}`)
    console.log(`   Correlation ID: ${correlationId}`)
    console.log(`   Message: ${JSON.stringify(message).substring(0, 100)}...\n`)

    const result = await producer.send({
      topic: topics.requests,
      messages: [{
        key: correlationId,
        value: JSON.stringify(message)
      }]
    })

    console.log('✅ Message sent successfully!')
    console.log(`   Partition: ${result[0].partition}`)
    console.log(`   Offset: ${result[0].baseOffset}`)
    console.log()
    console.log('📋 To consume this message, run:')
    console.log(`   kcat -b 127.0.0.1:39092 -t ${topics.requests} -C`)
    console.log('   or:')
    console.log(`   kafka-console-consumer --bootstrap-server 127.0.0.1:39092 --topic ${topics.requests} --from-beginning`)
    console.log()

  } catch (error) {
    console.error('❌ Producer test failed')
    console.error('Error:', error.message)
    console.error()
    if (error.message.includes('ECONNREFUSED')) {
      console.error('Troubleshooting:')
      console.error('  - Is KafScale running on 127.0.0.1:39092?')
      console.error('  - Check with: lsof -i :39092')
    }
    process.exit(1)

  } finally {
    await producer.disconnect()
    console.log('✓ Producer disconnected')
  }
}

testProducer()
