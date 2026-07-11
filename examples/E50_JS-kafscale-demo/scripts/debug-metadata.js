/**
 * Debug Script - Check Kafka Metadata
 * Helps diagnose broker ID and metadata issues
 */

import { kafka } from '../src/kafka.js'

console.log('🔍 Debugging Kafka Metadata\n')

async function checkMetadata() {
  const admin = kafka.admin()

  try {
    console.log('Connecting to admin client...')
    await admin.connect()
    console.log('✓ Connected\n')

    console.log('Fetching cluster metadata...')
    const cluster = await admin.describeCluster()

    console.log('Cluster Info:')
    console.log('─'.repeat(50))
    console.log(`Cluster ID: ${cluster.clusterId}`)
    console.log(`Controller: ${cluster.controller}`)
    console.log(`\nBrokers (${cluster.brokers.length}):`)
    cluster.brokers.forEach(broker => {
      console.log(`  - Broker ID: ${broker.nodeId}`)
      console.log(`    Host: ${broker.host}:${broker.port}`)
      console.log(`    Rack: ${broker.rack || 'null'}`)
    })

    console.log('\n' + '─'.repeat(50))

    const topics = await admin.listTopics()
    console.log(`\nTopics (${topics.length}):`)
    topics.filter(t => t.startsWith('agent')).forEach(t => console.log(`  - ${t}`))

  } catch (error) {
    console.error('\n❌ Error:', error.message)
    console.error('\nFull error:', error)
  } finally {
    await admin.disconnect()
    console.log('\n✓ Disconnected')
  }
}

checkMetadata()
