# Troubleshooting

This section covers common issues you might encounter when using KafScale.

## Connection Issues

### Problem: "Connection refused" when connecting to broker

**Symptoms**:
```
org.apache.kafka.common.errors.TimeoutException: Failed to update metadata after 60000 ms.
```

**Possible Causes**:
1. KafScale broker is not running.
2. Wrong bootstrap server address (should be `localhost:39092` for local clients).
3. Port 39092 is not accessible.

**Solutions**:

1. **Check if broker is running**:
```bash
docker ps | grep kafscale-broker
```

2. **Check broker logs**:
```bash
docker logs kafscale-broker
```

3. **Verify port is listening**:
```bash
lsof -i :39092
```

4. **Test connection**:
```bash
telnet localhost 39092
```

### Problem: Broker starts but immediately crashes

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

### Check etcd Data

```bash
docker exec kafscale-etcd etcdctl get "" --prefix --keys-only
```

### Inspect MinIO Bucket

1. Open [http://localhost:9001](http://localhost:9001)
2. Login with `minioadmin` / `minioadmin`
3. Browse `kafscale` bucket

## Getting Help

If you're still stuck, please open an issue on [GitHub](https://github.com/novatechflow/kafscale/issues).
