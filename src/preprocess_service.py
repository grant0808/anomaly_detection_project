import os
import time
from fastapi import FastAPI
from prometheus_client import Counter, Histogram, make_asgi_app

app = FastAPI(title="DeepLog Real-time Preprocessing & Inference Service", version="1.0.0")

# Prometheus Metrics
LOGS_PROCESSED = Counter("deeplog_logs_processed_total", "Total logs processed by the service", ["status"])
INFERENCE_LATENCY = Histogram("deeplog_inference_latency_seconds", "Latency of inference requests to KServe")
ANOMALIES_DETECTED = Counter("deeplog_anomalies_detected_total", "Total anomalies detected")

@app.get("/healthz")
def healthz():
    return {"status": "healthy"}

@app.get("/api/v1/status")
def get_status():
    return {
        "kafka_bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "kserve_endpoint": os.getenv("KSERVE_ENDPOINT", "http://deeplog-serving.kserve-test.svc.cluster.local/v1/models/deeplog:predict"),
        "window_size": int(os.getenv("WINDOW_SIZE", "10"))
    }

# Expose Prometheus Metrics at /metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)
