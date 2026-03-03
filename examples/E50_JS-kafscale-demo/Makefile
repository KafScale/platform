# Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
# This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# E50 JS Agent KafScale Demo - Makefile
# This demo shows Kafka-based agent orchestration with JavaScript

.PHONY: help install setup topics clean run-agent run-panel run-consumer run-web test-flow test-e2e test-producer test-consumer kcat-consume kcat-produce demo stop-all

# Default target: show help
help:
	@echo "E50 JavaScript Agent KafScale Demo"
	@echo "==================================="
	@echo ""
	@echo "Available targets:"
	@echo "  make install       - Install Node.js dependencies"
	@echo "  make setup         - Create required Kafka topics"
	@echo "  make topics        - List all agent topics"
	@echo "  make run-web       - Start web UI with Kanban board (recommended)"
	@echo "  make run-agent     - Start the agent service (terminal 1)"
	@echo "  make run-consumer  - Start response consumer (terminal 2)"
	@echo "  make run-panel     - Run interactive task panel (terminal 3)"
	@echo "  make test-flow     - Send a test task through the system"
	@echo "  make test-e2e      - Run end-to-end automated test"
	@echo "  make test-producer - Test KafkaJS producer (use with kcat consumer)"
	@echo "  make test-consumer - Test KafkaJS consumer (use with kcat producer)"
	@echo "  make kcat-consume  - Start kcat consumer for testing"
	@echo "  make kcat-produce  - Start kcat producer for testing"
	@echo "  make demo          - Run complete demo flow (requires Kafka running)"
	@echo "  make clean         - Delete all topics and reset state"
	@echo "  make stop-all      - Stop all running services"
	@echo ""
	@echo "Quick Start (Web UI - Recommended):"
	@echo "  1. make install    # Install dependencies"
	@echo "  2. make setup      # Create topics"
	@echo "  3. Terminal 1: make run-agent"
	@echo "  4. Terminal 2: make run-web"
	@echo "  5. Open browser: http://localhost:3000"
	@echo ""
	@echo "Quick Start (CLI):"
	@echo "  1. make install    # Install dependencies"
	@echo "  2. make setup      # Create topics"
	@echo "  3. Terminal 1: make run-agent"
	@echo "  4. Terminal 2: make run-consumer"
	@echo "  5. Terminal 3: make run-panel"
	@echo ""
	@echo "Prerequisites:"
	@echo "  - Kafka or KafScale running on localhost:9092"
	@echo "  - Node.js 18+ installed"

# Install Node.js dependencies
install:
	@echo "Installing Node.js dependencies..."
	npm install

# Create Kafka topics required for the demo
setup: topics-create
	@echo "✓ Demo setup complete"
	@echo ""
	@echo "Next steps (Web UI):"
	@echo "  Terminal 1: make run-agent"
	@echo "  Terminal 2: make run-web"
	@echo "  Browser: http://localhost:3000"
	@echo ""
	@echo "Next steps (CLI):"
	@echo "  Terminal 1: make run-agent"
	@echo "  Terminal 2: make run-consumer"
	@echo "  Terminal 3: make run-panel"

# Create all required topics
topics-create:
	@echo "Creating Kafka topics..."
	@kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--topic agent_requests \
		--partitions 3 \
		--replication-factor 1 \
		--if-not-exists || true
	@kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--topic agent_internal \
		--partitions 3 \
		--replication-factor 1 \
		--if-not-exists || true
	@kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--topic agent_responses \
		--partitions 3 \
		--replication-factor 1 \
		--if-not-exists || true
	@echo "✓ Topics created"

# List all agent topics
topics:
	@echo "Agent topics:"
	@kafka-topics --list \
		--bootstrap-server localhost:9092 | grep "^agent_"

# Run the agent service (consume requests, produce responses)
run-agent:
	@echo "Starting agent service..."
	@echo "Listening on: agent_requests"
	@echo "Publishing to: agent_responses"
	node src/agent.js

# Run the response consumer (human panel view)
run-consumer:
	@echo "Starting response consumer..."
	@echo "Listening on: agent_responses"
	node scripts/consume-response.js

# Run the interactive task submission panel
run-panel:
	@echo "Interactive Task Panel"
	@echo "====================="
	node src/panel.js

# Run the web UI server (Kanban board)
run-web:
	@echo "Starting Web UI Server..."
	@echo "Open browser: http://localhost:3000"
	npm run web

# Send a test task through the system
test-flow:
	@echo "Sending test task..."
	@echo '{"task":"Explain Kafka topics","spec":"Brief, 2 sentences","context":"For beginners","correlationId":"test-'$$(date +%s)'","createdAt":"'$$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}' | \
	kafka-console-producer \
		--bootstrap-server localhost:9092 \
		--topic agent_requests
	@echo "✓ Test task sent to agent_requests"
	@echo "Check the consumer output for the response"

# Run end-to-end automated test
test-e2e:
	@echo "🧪 Running End-to-End Test"
	@echo "=========================="
	@echo ""
	@echo "Prerequisites:"
	@echo "  - KafScale is running"
	@echo "  - Topics are created (make setup)"
	@echo "  - Agent is running (make run-agent in another terminal)"
	@echo ""
	@echo "Starting test..."
	@echo ""
	@node scripts/e2e-test.js

# Run complete demo flow (for automated testing)
demo: check-kafka
	@echo "Running automated demo flow..."
	@echo ""
	@echo "Step 1: Creating topics..."
	@make topics-create
	@echo ""
	@echo "Step 2: Sending test task..."
	@make test-flow
	@echo ""
	@echo "Demo setup complete!"
	@echo "Start agent and consumer in separate terminals to see the flow"

# Check if Kafka is running
check-kafka:
	@echo "Checking Kafka connectivity..."
	@kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1 || \
		(echo "❌ Cannot connect to Kafka at localhost:9092" && \
		 echo "Please start Kafka or KafScale first" && exit 1)
	@echo "✓ Kafka is running"

# Clean up: delete all topics and reset
clean:
	@echo "Cleaning up demo environment..."
	@kafka-topics --delete \
		--bootstrap-server localhost:9092 \
		--topic agent_requests 2>/dev/null || true
	@kafka-topics --delete \
		--bootstrap-server localhost:9092 \
		--topic agent_internal 2>/dev/null || true
	@kafka-topics --delete \
		--bootstrap-server localhost:9092 \
		--topic agent_responses 2>/dev/null || true
	@echo "✓ Topics deleted"
	@echo "Run 'make setup' to recreate"

# Stop all running services (pkill approach)
stop-all:
	@echo "Stopping agent services..."
	@pkill -f "node src/agent.js" || echo "No agent running"
	@pkill -f "node scripts/consume-response.js" || echo "No consumer running"
	@echo "✓ Services stopped"

# Development targets
dev-check:
	@echo "Checking demo configuration..."
	@test -f config/agent-config.json || (echo "❌ Missing config/agent-config.json" && exit 1)
	@test -f src/kafka.js || (echo "❌ Missing src/kafka.js" && exit 1)
	@test -f src/agent.js || (echo "❌ Missing src/agent.js" && exit 1)
	@test -f src/panel.js || (echo "❌ Missing src/panel.js" && exit 1)
	@echo "✓ All required files present"

# Show topic contents (for debugging)
peek-requests:
	@echo "Recent messages in agent_requests:"
	@kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic agent_requests \
		--from-beginning \
		--max-messages 10 \
		--timeout-ms 5000 2>/dev/null || echo "No messages yet"

peek-responses:
	@echo "Recent messages in agent_responses:"
	@kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic agent_responses \
		--from-beginning \
		--max-messages 10 \
		--timeout-ms 5000 2>/dev/null || echo "No messages yet"

# Show demo architecture
arch:
	@echo "Demo Architecture"
	@echo "================="
	@echo ""
	@echo "┌──────────────┐"
	@echo "│ Human Panel  │  (make run-panel)"
	@echo "│ (CLI / UI)   │"
	@echo "└──────┬───────┘"
	@echo "       │ produce"
	@echo "       ▼"
	@echo "┌──────────────────────┐"
	@echo "│ agent.requests       │  (Kafka topic)"
	@echo "│ (Kafka topic)        │"
	@echo "└──────┬───────────────┘"
	@echo "       │ consume"
	@echo "       ▼"
	@echo "┌──────────────────────┐"
	@echo "│ JS Agent Service     │  (make run-agent)"
	@echo "│ - loads config       │"
	@echo "│ - builds prompt      │"
	@echo "│ - calls LLM (stub)   │"
	@echo "└──────┬───────────────┘"
	@echo "       │ produce"
	@echo "       ▼"
	@echo "┌──────────────────────┐"
	@echo "│ agent.responses      │  (Kafka topic)"
	@echo "│ (Kafka topic)        │"
	@echo "└──────┬───────────────┘"
	@echo "       │ consume"
	@echo "       ▼"
	@echo "┌──────────────┐"
	@echo "│ Human Panel  │  (make run-consumer)"
	@echo "│ shows result │"
	@echo "└──────────────┘"
	@echo ""

# Mixed client tests for debugging
test-producer:
	@echo "🧪 Testing KafkaJS Producer"
	@echo "=========================="
	@echo ""
	@echo "This will send a message using KafkaJS."
	@echo "To verify, run in another terminal:"
	@echo "  make kcat-consume"
	@echo ""
	@node scripts/test-producer-only.js

test-consumer:
	@echo "🧪 Testing KafkaJS Consumer"
	@echo "==========================="
	@echo ""
	@echo "This will consume messages using KafkaJS."
	@echo "To send a test message, run in another terminal:"
	@echo "  make kcat-produce"
	@echo "  (or see instructions in output)"
	@echo ""
	@node scripts/test-consumer-only.js

kcat-consume:
	@echo "📥 Starting kcat consumer"
	@echo "========================"
	@echo "Consuming from: agent_requests"
	@echo "Press Ctrl+C to stop"
	@echo ""
	@kcat -b 127.0.0.1:39092 -t agent_requests -C || \
		(echo "" && echo "❌ kcat not found. Install with: brew install kcat" && exit 1)

kcat-produce:
	@echo "📤 Starting kcat producer"
	@echo "========================"
	@echo "Producing to: agent_requests"
	@echo "Type your messages (Ctrl+D to finish):"
	@echo ""
	@kcat -b 127.0.0.1:39092 -t agent_requests -P || \
		(echo "" && echo "❌ kcat not found. Install with: brew install kcat" && exit 1)
