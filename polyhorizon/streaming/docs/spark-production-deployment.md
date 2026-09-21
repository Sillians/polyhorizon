# Spark Streaming Job - Production Deployment Guide

## Overview

This guide covers three production deployment patterns for your Spark streaming jobs.

## Option 1: Prefect Orchestration (Recommended)

**Best for:** Your current stack, scheduled jobs, complex workflows

### Setup

1. **Deploy the Prefect flow:**
```bash
# Register the flow with Prefect
python -m src.flows.spark_streaming_flow

# Create a deployment
prefect deployment build src/flows/spark_streaming_flow.py:spark_streaming_flow \
  -n "spark-streaming-production" \
  -q "spark-jobs" \
  --cron "0 0 * * *"  # Or your schedule

prefect deployment apply spark_streaming_flow-deployment.yaml
```

2. **Start a Prefect agent:**
```bash
docker compose exec prefect-server prefect agent start -q spark-jobs
```

3. **Monitor via Prefect UI:**
- http://localhost:4200

### Advantages
- ✅ Automatic retries and error handling
- ✅ Job scheduling and dependencies
- ✅ Rich monitoring UI
- ✅ Integration with your existing Prefect setup
- ✅ Easy to trigger manually or via API

---

## Option 2: Long-Running Service

**Best for:** Continuous streaming jobs, real-time processing

### Setup

1. **Start the infrastructure:**
```bash
docker compose up -d spark-master spark-worker kafka-broker-1 kafka-broker-2 kafka-broker-3
```

2. **Deploy the streaming service:**
```bash
docker compose -f docker-compose.yaml -f docker-compose.prod.yaml up -d spark-streaming-service
```

3. **Monitor logs:**
```bash
docker compose logs -f spark-streaming-service
```

4. **Check Spark UI:**
- http://localhost:18080

### Key Features
- `restart: unless-stopped` - Auto-restart on failures
- `--supervise` flag - Spark-level driver restart
- Health checks for container monitoring
- Log rotation (100MB, 5 files)

### Scaling Workers
```bash
docker compose up -d --scale spark-worker=3
```

---

## Option 3: Kubernetes with Spark Operator

**Best for:** Cloud production, auto-scaling, multi-tenancy

### Prerequisites

1. **Install Spark Operator:**
```bash
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm install spark-operator spark-operator/spark-operator --namespace spark-operator --create-namespace
```

2. **Create namespace:**
```bash
kubectl create namespace spark-jobs
```

3. **Create secrets:**
```bash
kubectl create secret generic aws-credentials \
  --namespace=spark-jobs \
  --from-literal=access-key-id=$AWS_ACCESS_KEY_ID \
  --from-literal=secret-access-key=$AWS_SECRET_ACCESS_KEY
```

### Deployment

1. **Build and push image:**
```bash
docker build -t your-registry/spark-custom:3.5.7 docker/spark/
docker push your-registry/spark-custom:3.5.7
```

2. **Deploy the SparkApplication:**
```bash
kubectl apply -f k8s/spark-streaming-job.yaml
```

3. **Monitor:**
```bash
kubectl get sparkapplications -n spark-jobs
kubectl logs -n spark-jobs polyhorizon-streaming-job-driver -f
```

### Advantages
- ✅ Native Kubernetes scheduling
- ✅ Auto-scaling executors
- ✅ Resource isolation
- ✅ Declarative configuration
- ✅ Built-in monitoring with Prometheus

---

## Production Best Practices

### 1. Deploy Mode
- **Client mode**: Driver runs in submission container (simpler, good for Docker Compose)
- **Cluster mode**: Driver runs on worker nodes (more resilient, required for K8s)

### 2. Checkpointing
- Always use S3/distributed storage for checkpoints
- Set unique checkpoint paths per job:
  ```python
  checkpointLocation = f"s3a://polyhorizon-streamingdata/checkpoints/{job_name}"
  ```

### 3. Resource Configuration
```python
# Production recommendations
spark.driver.memory = 2-4g
spark.executor.memory = 4-8g
spark.executor.cores = 2-4
spark.sql.shuffle.partitions = 200  # Adjust based on data volume
```

### 4. Monitoring

#### Metrics to track:
- Input rate (records/sec)
- Processing time
- Scheduling delay
- Total delay
- Checkpoint duration

#### Integration with your Grafana:
```yaml
# Already configured in your docker-compose.yaml
# Spark metrics exposed on:
# - Master: spark-master:8090
# - Worker: spark-worker:8091
```

### 5. Error Handling

Update your streaming job:
```python
query = df.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime='30 seconds') \
    .foreachBatch(process_batch_with_error_handling) \
    .start()

def process_batch_with_error_handling(batch_df, batch_id):
    try:
        batch_df.write.format("delta").mode("append").save(output_path)
    except Exception as e:
        # Log to monitoring system
        logger.error(f"Batch {batch_id} failed: {e}")
        # Write to DLQ
        batch_df.write.mode("append").save(f"{dlq_path}/batch_{batch_id}")
```

### 6. Secrets Management

For production, use your Vault service:
```python
# docker/spark/jobs/streaming_job.py
import hvac

client = hvac.Client(url='http://vault:8200', token=os.getenv('VAULT_TOKEN'))
secrets = client.secrets.kv.v2.read_secret_version(path='spark/credentials')

spark.conf.set("spark.hadoop.fs.s3a.access.key", secrets['data']['data']['aws_access_key'])
spark.conf.set("spark.hadoop.fs.s3a.secret.key", secrets['data']['data']['aws_secret_key'])
```

---

## Comparison Matrix

| Feature | Prefect | Docker Service | Kubernetes |
|---------|---------|----------------|------------|
| **Complexity** | Medium | Low | High |
| **Scheduling** | ✅ Built-in | Manual | CronJob |
| **Monitoring** | ✅ Rich UI | Logs + Grafana | Prometheus + Grafana |
| **Scaling** | Manual | Manual | ✅ Auto-scale |
| **Cost** | Low | Low | Higher |
| **Best for** | Batch + Streaming | Simple streaming | Cloud production |

---

## Recommendations by Environment

### Development
- Use current `docker compose up spark-submit`
- Quick iteration, no restart policy needed

### Staging
- **Option 2** (Docker Service) with restart policy
- Tests production-like behavior
- Easy to debug

### Production
- **Small scale**: Option 2 (Docker Service)
- **With existing Prefect**: Option 1 (Prefect orchestration)
- **Cloud/Large scale**: Option 3 (Kubernetes)

---

## Migration Path

1. **Now (Dev)**: `docker compose up spark-submit`
2. **Next**: Add Prefect flow for scheduling
3. **Future**: Move to K8s when scaling requirements grow

Your infrastructure already has Prefect, Prometheus, and Grafana, so **Option 1** is the natural next step.
