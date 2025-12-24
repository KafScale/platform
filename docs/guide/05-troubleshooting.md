# Troubleshooting

This section covers common issues you might encounter when using KafScale and how to resolve them.

## Connection Issues

### Problem: "Connection refused" when connecting to broker

**Symptoms**:
```
org.apache.kafka.common.errors.TimeoutException: Failed to update metadata after 60000 ms.
```

**Possible Causes**:
1. KafScale broker is not running
2. Wrong bootstrap server address
3. Port 9092 is not accessible

**Solutions**:

1. **Check if broker is running**:
```bash
docker-compose ps kafscale-broker
```

2. **Check broker logs**:
```bash
docker-compose logs kafscale-broker
```

3. **Verify port is listening**:
```bash
lsof -i :9092
# or
netstat -an | grep 9092
```

4. **Test connection**:
```bash
telnet localhost 9092
```

### Problem: Broker starts but immediately crashes

**Check Dependencies**:

1. **Verify etcd is healthy**:
```bash
docker-compose logs etcd
curl http://localhost:2379/health
```

2. **Verify MinIO is healthy**:
```bash
docker-compose logs minio
curl http://localhost:9000/minio/health/live
```

3. **Check bucket was created**:
```bash
docker-compose logs minio-setup
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
kafka-topics --bootstrap-server localhost:9092 \
  --create \
  --topic your-topic \
  --partitions 3
```

2. **List existing topics**:
```bash
kafka-topics --bootstrap-server localhost:9092 --list
```

3. **Enable auto-topic creation** (not recommended for production):
Add to broker environment in `docker-compose.yml`:
```yaml
- KAFSCALE_AUTO_CREATE_TOPICS=true
```

## Consumer Group Issues

### Problem: Consumer not receiving messages

**Possible Causes**:
1. Offset already committed past available messages
2. Wrong consumer group ID
3. Consumer started after messages were produced

**Solutions**:

1. **Check consumer group status**:
```bash
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe \
  --group your-group-id
```

2. **Reset offsets to beginning**:
```bash
kafka-consumer-groups --bootstrap-server localhost:9092 \
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
kafka-consumer-groups --bootstrap-server localhost:9092 \
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
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic your-topic \
  --from-beginning \
  --property print.key=true \
  --property print.value=true
```

## Docker Issues

### Problem: Port already in use

**Symptoms**:
```
Error starting userland proxy: listen tcp4 0.0.0.0:9092: bind: address already in use
```

**Solutions**:

1. **Find process using the port**:
```bash
lsof -i :9092
```

2. **Stop conflicting service**:
```bash
# If it's another Kafka
brew services stop kafka

# If it's another Docker container
docker ps | grep 9092
docker stop <container-id>
```

3. **Change port in docker-compose.yml**:
```yaml
ports:
  - "19092:9092"  # Use different host port
```

Then update your application:
```properties
spring.kafka.bootstrap-servers=localhost:19092
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

3. **Increase Docker disk limit** (Docker Desktop → Settings → Resources)

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

### Problem: Spring Boot application won't start

**Check**:

1. **Java version**:
```bash
java -version  # Should be 17+
```

2. **Maven build**:
```bash
mvn clean package
```

3. **Dependencies**:
```bash
mvn dependency:tree
```

4. **Application logs**:
Look for stack traces in console output

### Problem: Messages sent but not consumed

**Check**:

1. **Consumer is running**:
Look for `@KafkaListener` startup logs

2. **Topic name matches**:
```properties
# Producer
app.kafka.topic=orders

# Consumer
@KafkaListener(topics = "${app.kafka.topic}")
```

3. **Consumer group is active**:
```bash
kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

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
  --bootstrap-server localhost:9092 \
  --topic test-topic

# Consume and verify
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic test-topic \
  --from-beginning \
  --max-messages 1
```

### Check etcd Data

```bash
# List all keys
docker exec kafscale-etcd etcdctl get "" --prefix --keys-only

# Get specific key
docker exec kafscale-etcd etcdctl get /kafscale/topics/orders
```

### Inspect MinIO Bucket

1. Open [http://localhost:9001](http://localhost:9001)
2. Login with `minioadmin` / `minioadmin`
3. Browse `kafscale` bucket
4. Check for segment files under `default/your-topic/`

## Getting Help

If you're still stuck:

1. **Check KafScale logs** for error messages
2. **Review the [KafScale specification](../../kafscale-spec.md)** for technical details
3. **Search GitHub issues**: [github.com/novatechflow/kafscale/issues](https://github.com/novatechflow/kafscale/issues)
4. **Ask in discussions**: [github.com/novatechflow/kafscale/discussions](https://github.com/novatechflow/kafscale/discussions)

## Common Error Messages

| Error | Likely Cause | Solution |
|-------|-------------|----------|
| `Connection refused` | Broker not running | Start broker with `docker-compose up` |
| `Unknown topic` | Topic doesn't exist | Create topic or enable auto-creation |
| `Offset out of range` | Consumer offset invalid | Reset offsets to earliest |
| `Serialization failed` | Mismatched serializers | Verify serializer configuration |
| `Group coordinator not available` | etcd not accessible | Check etcd is running |
| `Timeout waiting for metadata` | Network issue | Check broker connectivity |

**Next**: [Next Steps](06-next-steps.md) →
