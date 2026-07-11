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

# Testing Quick Reference

## 🎯 Mixed Client Tests (Recommended for Debugging)

These tests help isolate whether issues are in producer, consumer, or broker.

### Test 1: KafkaJS Producer → kcat Consumer

**Terminal 1:**
```bash
make kcat-consume
# or: kcat -b 127.0.0.1:39092 -t agent_requests -C
```

**Terminal 2:**
```bash
make test-producer
# or: npm run test:producer
```

**What it tests:** Can KafkaJS write messages that native tools can read?

---

### Test 2: kcat Producer → KafkaJS Consumer

**Terminal 1:**
```bash
make test-consumer
# or: npm run test:consumer
```

**Terminal 2:**
```bash
echo '{"test":"hello"}' | kcat -b 127.0.0.1:39092 -t agent_requests -P
# or: make kcat-produce (then type message)
```

**What it tests:** Can KafkaJS read messages from native producers?

---

### Test 3: Full E2E (KafkaJS → Agent → KafkaJS)

**Terminal 1:**
```bash
make run-agent
```

**Terminal 2:**
```bash
make test-e2e
# or: npm test
```

**What it tests:** Complete workflow with KafkaJS only.

---

## 📊 Interpretation Matrix

| Test 1 | Test 2 | Test 3 | Diagnosis |
|--------|--------|--------|-----------|
| ✅ | ✅ | ✅ | **Everything works!** |
| ❌ | ✅ | ❌ | KafkaJS **producer** broken |
| ✅ | ❌ | ❌ | KafkaJS **consumer** broken |
| ❌ | ❌ | ❌ | KafkaJS incompatible with broker |
| ✅ | ✅ | ❌ | Agent logic issue (not client) |

---

## 🔧 Quick Commands

### Run Tests
```bash
make test-producer          # Test KafkaJS producer
make test-consumer          # Test KafkaJS consumer
make test-e2e               # Full E2E test
npm run debug:metadata      # Check broker metadata
```

### Native Tools
```bash
make kcat-consume           # Start kcat consumer
make kcat-produce           # Start kcat producer
make topics                 # List topics
```

### Check Connectivity
```bash
lsof -i :39092              # Is broker listening?
nc -zv 127.0.0.1 39092      # Can we connect?
```

---

## 🐛 Common Issues

### "ECONNREFUSED"
- Broker not running on expected port
- Check: `lsof -i :39092`

### "BrokerNotFound"
- Metadata issue (broker ID mismatch)
- Check: `npm run debug:metadata`

### Message sent but not received
- Run Test 1 & 2 to isolate which direction fails
- Check topic exists: `make topics`

---

## 📚 Full Documentation

- [MIXED-TEST-GUIDE.md](MIXED-TEST-GUIDE.md) - Complete testing guide
- [KAFSCALE-COMPATIBILITY.md](KAFSCALE-COMPATIBILITY.md) - Technical details
- [CURRENT-STATUS.md](CURRENT-STATUS.md) - Current implementation status
