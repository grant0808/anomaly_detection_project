import os
import time
import threading
from collections import defaultdict, deque

import requests
from fastapi import FastAPI
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import Counter, Histogram, make_asgi_app
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

from parser import clean_log_message, extract_block_id
from preprocess import EventIndexer

app = FastAPI(title="DeepLog Real-time Preprocessing & Inference Service", version="1.0.0")

# Prometheus Metrics
LOGS_PROCESSED = Counter("deeplog_logs_processed_total", "Total logs processed by the service", ["status"])
INFERENCE_LATENCY = Histogram("deeplog_inference_latency_seconds", "Latency of inference requests to KServe")
ANOMALIES_DETECTED = Counter("deeplog_anomalies_detected_total", "Total anomalies detected")
KAFKA_ERRORS = Counter("deeplog_kafka_errors_total", "Total Kafka processing errors")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_LOG_TOPIC = os.getenv("RAW_LOG_TOPIC", "hdfs-raw-logs")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "anomaly-alerts")
KSERVE_ENDPOINT = os.getenv(
    "KSERVE_ENDPOINT",
    "http://deeplog-serving-predictor.deeplog.svc.cluster.local/v1/models/deeplog:predict",
)
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", "10"))
TOP_G = int(os.getenv("TOP_G", "9"))
VOCAB_PATH = os.getenv("VOCAB_PATH", os.path.join(os.path.dirname(__file__), "vocab.json"))
DRAIN_CONFIG_PATH = os.getenv("DRAIN_CONFIG_PATH", os.path.join(os.path.dirname(__file__), "drain3.ini"))
ENABLE_KAFKA_WORKER = os.getenv("ENABLE_KAFKA_WORKER", "true").lower() == "true"

trace_buffers = defaultdict(lambda: deque(maxlen=WINDOW_SIZE + 1))
worker_started = False


def create_template_miner():
    config = TemplateMinerConfig()
    config.load(DRAIN_CONFIG_PATH)
    return TemplateMiner(config=config)


def create_consumer():
    return KafkaConsumer(
        RAW_LOG_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=os.getenv("KAFKA_GROUP_ID", "deeplog-preprocessing-service"),
        auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest"),
        enable_auto_commit=True,
        value_deserializer=lambda value: value.decode("utf-8"),
    )


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: value.encode("utf-8"),
    )


def build_window(sequence):
    input_seq = list(sequence)[-WINDOW_SIZE - 1 : -1]
    pad_len = WINDOW_SIZE - len(input_seq)
    return [0] * pad_len + input_seq


def process_log_line(raw_log, template_miner, indexer, producer):
    block_id = extract_block_id(raw_log)
    if not block_id:
        LOGS_PROCESSED.labels(status="skipped_no_block_id").inc()
        return

    cleaned_msg = clean_log_message(raw_log)
    result = template_miner.add_log_message(cleaned_msg)
    event_id = f"E{result['cluster_id']}"
    event_index = indexer.event2idx.get(event_id, 0)

    trace_buffers[block_id].append(event_index)
    if len(trace_buffers[block_id]) <= 1:
        LOGS_PROCESSED.labels(status="buffering").inc()
        return

    input_window = build_window(trace_buffers[block_id])
    actual_event = trace_buffers[block_id][-1]

    started_at = time.time()
    response = requests.post(
        KSERVE_ENDPOINT,
        json={"instances": [input_window], "top_g": TOP_G},
        timeout=float(os.getenv("KSERVE_TIMEOUT_SECONDS", "3")),
    )
    INFERENCE_LATENCY.observe(time.time() - started_at)
    response.raise_for_status()

    prediction = response.json()["predictions"][0]
    top_indices = prediction["top_indices"]
    is_anomaly = actual_event not in top_indices

    if is_anomaly:
        ANOMALIES_DETECTED.inc()
        producer.send(
            ALERT_TOPIC,
            value=(
                f'{{"block_id":"{block_id}",'
                f'"event_id":"{event_id}",'
                f'"event_index":{event_index},'
                f'"top_indices":{top_indices},'
                f'"timestamp":{int(time.time())}}}'
            ),
        )
        producer.flush()

    LOGS_PROCESSED.labels(status="processed").inc()


def kafka_worker():
    indexer = EventIndexer.load(VOCAB_PATH)
    template_miner = create_template_miner()
    consumer = create_consumer()
    producer = create_producer()

    for message in consumer:
        try:
            process_log_line(message.value, template_miner, indexer, producer)
        except Exception:
            KAFKA_ERRORS.inc()
            LOGS_PROCESSED.labels(status="error").inc()


@app.on_event("startup")
def startup_event():
    global worker_started
    if ENABLE_KAFKA_WORKER and not worker_started:
        thread = threading.Thread(target=kafka_worker, daemon=True)
        thread.start()
        worker_started = True

@app.get("/healthz")
def healthz():
    return {"status": "healthy"}

@app.get("/api/v1/status")
def get_status():
    return {
        "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "raw_log_topic": RAW_LOG_TOPIC,
        "alert_topic": ALERT_TOPIC,
        "kserve_endpoint": KSERVE_ENDPOINT,
        "window_size": WINDOW_SIZE,
        "top_g": TOP_G,
        "kafka_worker_enabled": ENABLE_KAFKA_WORKER,
        "kafka_worker_started": worker_started,
    }

# Expose Prometheus Metrics at /metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)
