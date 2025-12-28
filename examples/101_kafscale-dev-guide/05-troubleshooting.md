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

# Troubleshooting

This section covers common issues you might encounter when using the local demo or the platform demo.

**Most common issues** (start here):
1. [Connection refused](#problem-connection-refused-when-connecting-to-broker) - Broker not running or wrong port
2. [Topic not found](#problem-topic-not-found) - Topic needs manual creation
3. [Messages sent but not consumed](#problem-messages-sent-but-not-consumed) - Topic name mismatch

**Advanced issues** (for stream processing demos):
4. [Flink Kafka sink fails](#problem-flink-kafka-sink-fails-with-init_producer_id--transactional-errors)
5. [Flink offset commit fails](#problem-flink-offset-commit-fails-with-unknown_member_id)

---

## Connection Issues

### Problem: "Connection refused" when connecting to broker

**Symptoms**:
```
org.apache.kafka.common.errors.TimeoutException: Failed to update metadata after 60000 ms.
```

**Possible Causes**:
1. KafScale broker is not running.
2. Wrong bootstrap server address (should be `localhost:39092` for local and platform demos).
3. Port 39092 is not accessible.

**Solutions**:

1. **Verify port is listening**:
```bash
lsof -i :39092
```

2. **Local demo logs**: Check the terminal running `make demo`.

3. **Platform demo logs**:
```bash
kubectl -n kafscale-demo get pods
kubectl -n kafscale-demo logs deployment/kafscale-broker
```

4. **Test connection**:
```bash
telnet localhost 39092
```

### Problem: Broker starts but immediately crashes

**Local demo**: Check the terminal output from `make demo`.

**Platform demo**:
```bash
kubectl -n kafscale-demo logs deployment/kafscale-broker
kubectl -n kafscale-demo logs deployment/kafscale-operator
```

**Check Dependencies**:

1. **Verify etcd is healthy**:
```bash
docker logs kafscale-etcd
curl http://localhost:2379/health
```

2. **Verify MinIO is healthy**:
```bash
docker logs kafscale-minio
curl http://localhost:9000/minio/health/live
```

## Topic Issues

### Problem: Topic not found

**Symptoms**:
```
org.apache.kafka.common.errors.UnknownTopicOrPartitionException: This server does not host this topic-partition.
```

**Solutions**:

1. **Create the topic manually**:
```bash
# Note: Using localhost:39092 for external connection
kafka-topics --bootstrap-server localhost:39092 \
  --create \
  --topic your-topic \
  --partitions 3
```

2. **List existing topics**:
```bash
kafka-topics --bootstrap-server localhost:39092 --list
```

3. **Enable auto-topic creation** (not recommended for production):
Add to broker environment in `docker-compose.yml`:
```yaml
- KAFSCALE_AUTO_CREATE_TOPICS=true
```

## Consumer Group Issues

### Problem: Consumer not receiving messages

**Solutions**:

1. **Check consumer group status**:
```bash
kafka-consumer-groups --bootstrap-server localhost:39092 \
  --describe \
  --group your-group-id
```

2. **Reset offsets to beginning**:
```bash
kafka-consumer-groups --bootstrap-server localhost:39092 \
  --group your-group-id \
  --reset-offsets \
  --to-earliest \
  --topic your-topic \
  --execute
```

3. **Use a new consumer group**:
```properties
spring.kafka.consumer.group-id=new-group-name
```

4. **Set auto-offset-reset**:
```properties
spring.kafka.consumer.auto-offset-reset=earliest
```

### Problem: Consumer lag increasing

**Check**:

1. **Consumer processing speed**:
```bash
kafka-consumer-groups --bootstrap-server localhost:39092 \
  --describe \
  --group your-group-id
```

2. **Increase consumer concurrency**:
```properties
spring.kafka.listener.concurrency=5
```

3. **Check for errors in consumer logs**

## Serialization Issues

### Problem: Deserialization errors

**Symptoms**:
```
org.springframework.kafka.support.serializer.DeserializationException:
failed to deserialize; nested exception is com.fasterxml.jackson.databind.exc.InvalidDefinitionException
```

**Solutions**:

1. **Verify serializer/deserializer match**:
```properties
# Producer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer

# Consumer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
```

2. **Trust all packages** (for JSON):
```properties
spring.kafka.consumer.properties.spring.json.trusted.packages=*
```

3. **Check message format**:
```bash
kafka-console-consumer --bootstrap-server localhost:39092 \
  --topic your-topic \
  --from-beginning \
  --property print.key=true \
  --property print.value=true
```

## Docker Issues

### Problem: Port already in use

**Symptoms**:
```
Error starting userland proxy: listen tcp4 0.0.0.0:39092: bind: address already in use
```

**Solutions**:

1. **Find process using the port**:
```bash
lsof -i :39092
```

2. **Stop conflicting service**:
```bash
docker stop <container-id>
```

### Problem: Out of disk space

**Symptoms**:
```
Error response from daemon: no space left on device
```

**Solutions**:

1. **Clean up Docker**:
```bash
docker system prune -a --volumes
```

2. **Remove old images**:
```bash
docker image prune -a
```

3. **Increase Docker disk limit** (Docker Desktop -> Settings -> Resources)

## Performance Issues

### Problem: Slow message production

**Check**:

1. **Batch size too small**:
```properties
spring.kafka.producer.properties.batch.size=32768
spring.kafka.producer.properties.linger.ms=100
```

2. **Compression disabled**:
```properties
spring.kafka.producer.compression-type=snappy
```

3. **MinIO performance**:
```bash
docker stats kafscale-minio
```

### Problem: High latency

**Expected Behavior**:
- KafScale adds 10-50ms latency due to S3 storage
- This is normal and expected

**If latency is higher**:

1. **Check MinIO is running locally** (not remote S3)
2. **Verify network connectivity**:
```bash
ping localhost
```

3. **Check Docker resource limits**:
```bash
docker stats
```

## Application Issues

### Problem: Flink Kafka sink fails with INIT_PRODUCER_ID / transactional errors

**Symptoms**:
```
UnsupportedVersionException: The node does not support INIT_PRODUCER_ID ...
```

**Cause**:
Flink Kafka sink uses idempotent/transactional producer features for stronger guarantees. The current KafScale broker
does not support `INIT_PRODUCER_ID`, so the producer fails.

**Solutions**:

1. **Use delivery guarantee `none` and disable idempotence** (recommended for demos):
   - Set `KAFSCALE_SINK_DELIVERY_GUARANTEE=none` and `KAFSCALE_SINK_ENABLE_IDEMPOTENCE=false`.

2. **Disable the sink entirely**:
   - Set `KAFSCALE_SINK_ENABLED=false` and rely on stdout output only.

3. **Reduce delivery guarantees**:
   - Keep `DeliveryGuarantee.AT_LEAST_ONCE` and avoid transactional settings.

### Problem: Spring Boot application won't start

**Check**:
1. **Bootstrap Servers**: Ensure `application.properties` uses `localhost:39092`.
2. **Port Conflicts**: The app runs on `8083`. Ensure it's free.

5. **Bootstrap Servers**: Ensure `application.properties` uses `localhost:39092`.
6. **Port Conflicts**: The app runs on `8093`. Ensure it's free.

### Problem: Messages sent but not consumed

**Check**:
1. **Topic name matches**:
```properties
# Producer
app.kafka.topic=orders
# Consumer
@KafkaListener(topics = "${app.kafka.topic}")
```

2. **Consumer group is active**:
```bash
kafka-consumer-groups --bootstrap-server localhost:39092 --list
```

### Problem: Flink offset commit fails with UNKNOWN_MEMBER_ID

**Symptoms**:
```
The coordinator is not aware of this member
CommitFailedException: group has already rebalanced
```

**Cause**:
The consumer spent too long between polls or a rebalance occurred, so the coordinator rejected the offset commit.

**Solutions**:
1. **Use minimal delivery guarantees**:
   - Set `KAFSCALE_COMMIT_ON_CHECKPOINT=false`.
2. **Tune consumer settings**:
   - Increase `KAFSCALE_CONSUMER_MAX_POLL_INTERVAL_MS`.
   - Reduce `KAFSCALE_CONSUMER_MAX_POLL_RECORDS`.
   - Adjust `KAFSCALE_CONSUMER_SESSION_TIMEOUT_MS` and `KAFSCALE_CONSUMER_HEARTBEAT_INTERVAL_MS`.

## Debugging Tips

### Enable Debug Logging

**For Spring Kafka**:
```properties
logging.level.org.springframework.kafka=DEBUG
logging.level.org.apache.kafka=DEBUG
```

**For KafScale Broker**:
```yaml
# In docker-compose.yml
environment:
  - KAFSCALE_LOG_LEVEL=debug
```

### View Broker Logs

```bash
# Follow logs in real-time
docker-compose logs -f kafscale-broker

# View last 100 lines
docker-compose logs --tail=100 kafscale-broker
```

### Test with Kafka Console Tools

```bash
# Produce test message
echo "test message" | kafka-console-producer \
  --bootstrap-server localhost:39092 \
  --topic test-topic

# Consume and verify
kafka-console-consumer --bootstrap-server localhost:39092 \
  --topic test-topic \
  --from-beginning \
  --max-messages 1
```

### Check etcd Data

```bash
docker exec kafscale-etcd etcdctl get "" --prefix --keys-only
```

### Inspect MinIO Bucket

1. Open [http://localhost:9001](http://localhost:9001)
2. Login with `minioadmin` / `minioadmin`
3. Browse `kafscale` bucket

## What You Should Know Now

After reviewing this troubleshooting guide, you should be able to:

- [ ] Diagnose connection refused errors (broker not running, wrong port)
- [ ] Create topics manually when auto-creation fails
- [ ] Debug topic name mismatches between producer and consumer
- [ ] Handle Flink-specific transaction and offset commit errors
- [ ] Use MinIO console to inspect stored segments

**Checkpoint**: Keep this page bookmarked for quick reference when issues arise!

## Getting Help

If you're still stuck:

1. **Check KafScale logs** for error messages
2. **Review the [KafScale specification](../../kafscale-spec.md)** for technical details
3. **Search GitHub issues**: [github.com/novatechflow/kafscale/issues](https://github.com/novatechflow/kafscale/issues)
4. **Ask in discussions**: [github.com/novatechflow/kafscale/discussions](https://github.com/novatechflow/kafscale/discussions)

## Common Error Messages

| Error | Likely Cause | Solution |
|-------|-------------|----------|
| `Connection refused` | Broker not running | Start broker with `make demo` or `docker-compose up` |
| `Unknown topic` | Topic doesn't exist | Create topic or enable auto-creation |
| `Offset out of range` | Consumer offset invalid | Reset offsets to earliest |
| `Serialization failed` | Mismatched serializers | Verify serializer configuration |
| `Group coordinator not available` | etcd not accessible | Check etcd is running |
| `Timeout waiting for metadata` | Network issue | Check broker connectivity |

**Next**: [Next Steps](06-next-steps.md) ->
