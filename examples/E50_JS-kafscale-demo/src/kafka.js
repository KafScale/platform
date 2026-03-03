/**
 * Kafka Helper Module
 * Provides shared Kafka client, producer, and consumer factory functions
 * Loads configuration from config/agent-config.json
 *
 * KafScale Compatibility:
 * - Configured for single logical broker architecture
 * - Frequent metadata refresh to handle broker abstraction
 * - Conservative retry and timeout settings
 */

import { Kafka } from 'kafkajs'
import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

// Load configuration
const configPath = path.join(__dirname, '../config/agent-config.json')
const config = JSON.parse(fs.readFileSync(configPath, 'utf-8'))

// Silence KafkaJS partitioner warning
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1'

// Create Kafka client with KafScale-compatible settings
export const kafka = new Kafka({
  ...config.kafka,
  // Force frequent metadata refresh for KafScale's dynamic broker model
  metadataMaxAge: 30000, // 30 seconds

  // Retry configuration for transient broker routing issues
  retry: {
    retries: 10,
    initialRetryTime: 300,
    maxRetryTime: 30000,
    multiplier: 2,
    factor: 0.2
  },

  // Connection timeout
  connectionTimeout: 10000,
  requestTimeout: 30000,

  // Allow auto topic creation (useful for demo)
  allowAutoTopicCreation: true,

  // Log level (can be overridden)
  logLevel: process.env.KAFKA_LOG_LEVEL || 1 // ERROR=1, WARN=2, INFO=4, DEBUG=5
})

// Factory functions with KafScale-compatible settings
export const createProducer = () => kafka.producer({
  // Conservative settings for single broker architecture
  allowAutoTopicCreation: true,
  transactionTimeout: 30000
})

export const createConsumer = (groupId) => kafka.consumer({
  groupId,
  // Single partition concurrency works best with KafScale
  sessionTimeout: 30000,
  rebalanceTimeout: 60000,
  heartbeatInterval: 3000,

  // Force from beginning for demo reliability
  retry: {
    retries: 10
  }
})

// Export topic configuration
export const topics = config.topics

// Export full config for agent settings
export const agentConfig = config.agent
