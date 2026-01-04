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
2. **Port Conflicts**: The app runs on `8093`. Ensure it's free.

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

### Inspect MinIO Bucket

1. Open [http://localhost:9001](http://localhost:9001)
2. Login with `minioadmin` / `minioadmin`
3. Browse `kafscale` bucket

## Getting Help

If you're still stuck, please open an issue on [GitHub](https://github.com/novatechflow/kafscale/issues).
