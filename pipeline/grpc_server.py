import grpc
from concurrent import futures
import time
import metrics_pb2, metrics_pb2_grpc

# Import pipeline state (from pipeline.py Phase 2)
from pipeline_state import endpoint_counts, error_counts, alerts, recent_logs

class LogPipelineService(metrics_pb2_grpc.LogPipelineServiceServicer):
    def GetMetrics(self, request, context):
        endpoint = request.endpoint
        total = endpoint_counts.get(endpoint, 0)
        errors = error_counts.get(endpoint, 0)
        error_rate = errors / total if total else 0

        print(f"[gRPC] Metrics for {endpoint} → total={total}, errors={errors}, rate={error_rate}")
        
        return metrics_pb2.MetricsResponse(
            endpoint=endpoint,
            totalRequests=total,
            errorCount=errors,
            errorRate=error_rate
        )
    
    def GetAlerts(self, request, context):
        return metrics_pb2.AlertsResponse(alerts=alerts)

    def GetLogs(self, request, context):
        logs = []
        for ep, entries in recent_logs.items():
            for e in list(entries)[-request.limit:]:
                if not request.level or e["level"] == request.level:
                    logs.append(metrics_pb2.LogEntry(
                        timestamp=e["timestamp"],
                        level=e["level"],
                        endpoint=e["endpoint"],
                        message=e["message"]
                    ))
        return metrics_pb2.LogsResponse(logs=logs)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    metrics_pb2_grpc.add_LogPipelineServiceServicer_to_server(LogPipelineService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("gRPC server running on port 50051")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
