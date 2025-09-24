import asyncio
import json
import time
from aiokafka import AIOKafkaConsumer
import grpc
from concurrent import futures

from pipeline_state import add_log, add_alert, get_metrics, get_alerts, get_logs
import metrics_pb2, metrics_pb2_grpc

ERROR_RATE_THRESHOLD = 0.2
TRAFFIC_SPIKE_THRESHOLD = 10

async def process_log(log):
    data = json.loads(log)
    endpoint = data["endpoint"]
    level = data["level"]
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    log_entry = {
        "timestamp": timestamp,
        "level": level,
        "endpoint": endpoint,
        "message": data.get("message", "")
    }

    print(f"[Pipeline] Received log for {endpoint}, level={level}")
    add_log(log_entry)

    await check_alerts(endpoint)

async def check_alerts(endpoint):
    metrics = get_metrics(endpoint)
    error_rate = metrics["errorRate"]

    if error_rate > ERROR_RATE_THRESHOLD:
        print(f"[Pipeline] ALERT: High error rate for {endpoint}")
        add_alert(endpoint, "HIGH_ERROR_RATE", error_rate)

async def consume():
    consumer = AIOKafkaConsumer(
        'parsed-logs',
        bootstrap_servers='localhost:9092',
        group_id="python-pipeline"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            await process_log(msg.value.decode())
    finally:
        await consumer.stop()

# ---------- gRPC Service ----------
class LogPipelineService(metrics_pb2_grpc.LogPipelineServiceServicer):
    def GetMetrics(self, request, context):
        m = get_metrics(request.endpoint)
        return metrics_pb2.MetricsResponse(
            endpoint=m["endpoint"],
            totalRequests=m["totalRequests"],
            errorCount=m["errorCount"],
            errorRate=m["errorRate"]
        )

    def GetAlerts(self, request, context):
        return metrics_pb2.AlertsResponse(alerts=get_alerts())

    def GetLogs(self, request, context):
        logs = get_logs(limit=request.limit, level=request.level)
        return metrics_pb2.LogsResponse(logs=[
            metrics_pb2.LogEntry(
                timestamp=l["timestamp"],
                level=l["level"],
                endpoint=l["endpoint"],
                message=l["message"]
            ) for l in logs
        ])

def serve_grpc():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    metrics_pb2_grpc.add_LogPipelineServiceServicer_to_server(LogPipelineService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("[gRPC] Server running on port 50051")
    return server

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    # Start gRPC server
    grpc_server = serve_grpc()

    # Start Kafka consumer
    try:
        loop.run_until_complete(consume())
    except KeyboardInterrupt:
        pass
    finally:
        grpc_server.stop(0)
