/**
 * Agent Message Types
 * Defines the structure for messages passed between human panel and agents
 */

export const createAgentRequest = ({
  task,
  spec,
  context,
  correlationId
}) => ({
  correlationId,
  task,
  spec,
  context,
  createdAt: new Date().toISOString()
})

export const createAgentResponse = ({
  correlationId,
  result,
  error = null
}) => ({
  correlationId,
  result,
  error,
  finishedAt: new Date().toISOString()
})
