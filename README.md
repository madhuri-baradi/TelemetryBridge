# TelemetryBridge -- A Real-Time Log Analytics Platform
(Kafka , Java , Python , gRPC , GraphQL)

A production-style, multi-language log analytics pipeline:

- **Kafka** as the backbone for decoupled, resilient streaming.
- **Java pre-processor (multithreaded)** parses, enriches, filters logs, and routes malformed events to a **Dead-Letter Queue**.
- **Python async data pipeline** consumes structured logs and **serves live state via gRPC**.
- **Node/Apollo GraphQL** exposes a **typed API** for metrics/log lookups and integrations.
- **Prometheus/Grafana (optional)** for dashboards and alerting.

---

## ✨ Features

- **Concurrent ingestion** with bounded-queue backpressure & worker thread pool.
- **Parsing & Enrichment**: UUID event IDs, ISO timestamps, worker/thread id, original timestamp passthrough.
- **Filtering & Sampling**: Level allow-list; configurable sampling for INFO (WARN/ERROR always 100%).
- **Dead-Letter Queue (DLQ)**: Structured reasons + raw payload for safe inspection/replay.
- **Typed API**: Python gRPC + Node GraphQL for metrics/log queries.
- **Observability-ready**: Prometheus metrics & Grafana dashboards (optional).

---

## 🧱 Architecture
Producers → Kafka (raw-logs)
│
▼
Java Enhanced Processor (threads)
- regex parse
- enrichment (UUID, ts, worker)
- filtering + INFO sampling
- DLQ on failure → Kafka (dead-logs)
│
▼
Kafka (parsed-logs)
│
▼
Python Pipeline (async) + gRPC server
- in-memory live state
- gRPC: GetMetrics / GetAlerts / GetLogs
│
▼
Node Apollo GraphQL (typed API for clients)

---

## 🧰 Tech Stack

- **Kafka** (Confluent images via Docker)
- **Java 17** (`kafka-clients`, `slf4j-simple`, `jackson-databind`)
- **Python 3.10+** (`aiokafka`, `grpcio`, `grpcio-tools`, `prometheus_client`)
- **Node 18+/20+** (`@apollo/server`, `graphql`, `@grpc/grpc-js`, `@grpc/proto-loader`)

---

## 🚦 Getting Started

### 1️⃣ Start Kafka + Zookeeper + Kafka UI
```bash
docker-compose up -d
```
- Kafka → `localhost:9092`  
- Kafka UI → `http://localhost:8080`  

Create topics (if not already created):
```bash
kafka-topics --bootstrap-server localhost:9092 \
  --create --topic raw-logs --partitions 3 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 \
  --create --topic parsed-logs --partitions 3 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 \
  --create --topic dead-logs --partitions 3 --replication-factor 1
```

---

### 2️⃣ Run Log Producer (Python)
```bash
cd producer
pip install -r requirements.txt   # contains aiokafka
python log_producer.py
```
This publishes synthetic logs (INFO/ERROR) into `raw-logs`.

---

### 3️⃣ Run Java Pre-Processor
```bash
cd processor
mvn clean compile exec:java
```
The **multithreaded processor**:
- Parses raw logs with regex.
- Enriches payloads with UUID, timestamp, worker thread id.
- Filters via `ALLOW_LEVELS` and `INFO_SAMPLE_N` env vars.
- Routes bad logs into `dead-logs`.

Example console output:
```
[Processor] recv=1000 fwd=900 drop=90 dlq=10 q=0
```

---

### 4️⃣ Run Python Data Pipeline + gRPC
```bash
cd pipeline
pip install -r requirements.txt   # aiokafka, grpcio, protobuf
python main.py
```
- Consumes `parsed-logs`.  
- Updates in-memory metrics + recent logs.  
- Exposes gRPC service on port **50051**.  

---

### 5️⃣ Run Node GraphQL API
```bash
cd graphql-api
npm install
node index.js
```
GraphQL API available at → **http://localhost:4000/**  

Try a query:
```graphql
query {
  metrics(endpoint: "/api/order") {
    endpoint
    totalRequests
    errorCount
    errorRate
  }
}
```

