<!--
Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
This project is supported and financed by Scalytics, Inc. (www.scalytics.io).

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Quick Start Guide - Agent Kanban Board

## 🎯 What You'll See

A modern Kanban board interface where you can:
- Create tasks for your AI agent
- Drag tasks to trigger processing
- Watch the agent work in real-time
- See results appear automatically

## 🚀 Run It Now (2 Steps)

### 1. Install and Setup
```bash
cd examples/E50_JS-kafscale-demo
make install
make setup
```

### 2. Start Services (2 Terminals)

**Terminal 1:**
```bash
make run-agent
```

**Terminal 2:**
```bash
make run-web
```

### 3. Open Browser
Navigate to: **http://localhost:3000**

## 💡 Using the Kanban Board

### Create a Task
1. Click **"+ New Task"** button
2. Enter:
   - **Task**: "Write a blog post about Kafka"
   - **Spec**: "800 words, markdown format"
   - **Context**: "For developers learning Kafka"
3. Click **"Create Task"**

### Process the Task
1. Your task appears in the **📥 Backlog** lane
2. **Drag** the card to the **⚙️ Agent Processing** lane
3. The card is sent to Kafka immediately
4. Watch the right panel:
   - **Current Task** shows what the agent is working on
   - **Prompt Built** displays the LLM prompt
   - **LLM Processing** indicator shows activity

### See the Result
1. When complete, the card automatically moves to **✅ Completed**
2. Click the card to see the full response
3. The task appears in the **Task History** on the right

## 🎨 UI Features

### Left Panel: Task Board
- **Backlog**: New tasks waiting
- **Agent Processing**: Tasks being processed (drag here!)
- **Completed**: Finished tasks with results

### Right Panel: Agent Monitor
- **Current Task**: What's being worked on now
- **Prompt Built**: The actual LLM prompt
- **LLM Processing**: Real-time status
- **Task History**: Last 10 completed tasks

## 🔧 Configuration

Edit `config/agent-config.json` to change:
- Kafka broker address (currently: `127.0.0.1:39092`)
- Topic names
- Agent system prompt
- LLM settings

## 🛠️ Replace the LLM Stub

The demo uses a stubbed LLM. To use real AI:

1. Install provider SDK:
```bash
npm install @anthropic-ai/sdk
# or
npm install openai
```

2. Edit `src/llm.js` and replace the `callLLM` function

3. Restart the agent:
```bash
make run-agent
```

## 📊 What's Happening Behind the Scenes

1. **Task Creation** → Message sent to Kafka `agent_requests` topic
2. **Agent Service** → Consumes from `agent_requests`
3. **LLM Call** → Processes with stubbed/real LLM
4. **Response** → Published to `agent_responses` topic
5. **Web UI** → Receives via WebSocket and updates card

## 🎓 Next Steps

Try these experiments:
- [ ] Create multiple tasks and drag them all to processing
- [ ] Edit `config/agent-config.json` to change the system prompt
- [ ] Replace the LLM stub with a real API
- [ ] Open multiple browser tabs (all sync in real-time!)
- [ ] Add custom CSS to change the theme

## 🐛 Troubleshooting

**Can't connect to Kafka?**
```bash
# Check your config
cat config/agent-config.json

# Make sure Kafka/KafScale is running on the configured port
```

**Agent not processing?**
```bash
# Verify agent is running
# Terminal 1 should show: "🎧 Agent started, waiting for tasks..."
```

**Web UI not loading?**
```bash
# Check web server is running
# Terminal 2 should show: "🚀 Web Server Started"
# Try: http://localhost:3000
```

## 📚 Learn More

- Full documentation: [README.md](README.md)
- Architecture details: [SPEC-and-SD.md](SPEC-and-SD.md)
- Makefile commands: `make help`
