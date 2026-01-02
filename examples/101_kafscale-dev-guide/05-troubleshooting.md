# Troubleshooting

This section covers common issues you might encounter when using the local demo or the platform demo.

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

### Problem: Spring Boot application won't start

**Check**:
1. **Bootstrap Servers**: Ensure `application.properties` uses `localhost:39092`.
2. **Port Conflicts**: The app runs on `8083`. Ensure it's free.

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
