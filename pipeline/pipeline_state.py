from collections import defaultdict, deque
import threading

# Thread-safe in-memory state
lock = threading.Lock()

# Metrics
endpoint_counts = defaultdict(int) 
error_counts = defaultdict(int)

# Alerts (list of dicts)
alerts = []

# Recent logs (per endpoint, fixed window)
recent_logs = defaultdict(lambda: deque(maxlen=200))


def add_log(log_entry):
    #Add a log entry and update metrics.
    with lock:
        endpoint = log_entry["endpoint"]
        level = log_entry["level"]

        # Update metrics
        endpoint_counts[endpoint] += 1
        if level == "ERROR":
            error_counts[endpoint] += 1

        # Store recent log
        recent_logs[endpoint].append(log_entry)


def add_alert(endpoint, severity, error_rate):
    # Add an alert for an endpoint.
    with lock:
        alerts.append({
            "endpoint": endpoint,
            "severity": severity,
            "errorRate": error_rate,
            "triggeredAt": time_now()
        })


def get_metrics(endpoint):
    # Return metrics dict for given endpoint.
    with lock:
        total = endpoint_counts.get(endpoint, 0)
        errors = error_counts.get(endpoint, 0)
        error_rate = errors / total if total else 0
        return {
            "endpoint": endpoint,
            "totalRequests": total,
            "errorCount": errors,
            "errorRate": error_rate
        }


def get_alerts():
    # Return current list of alerts.
    with lock:
        return list(alerts)


def get_logs(limit=10, level=None):
    # Return recent logs across all endpoints.
    with lock:
        all_logs = []
        for ep, logs in recent_logs.items():
            for entry in logs:
                if not level or entry["level"] == level:
                    all_logs.append(entry)
        return all_logs[-limit:]


def time_now():
    import datetime
    return datetime.datetime.utcnow().isoformat() + "Z"
