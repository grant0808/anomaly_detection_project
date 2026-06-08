import os
import re
import json
import time
import threading
from typing import Dict, List, Optional
from fastapi import FastAPI
import requests
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import Counter, Histogram, Gauge, make_asgi_app
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from drain3.file_persistence_handler import FilePersistenceHandler

app = FastAPI(title="DeepLog Real-time Preprocessing & Inference Service", version="1.0.0")

# ==========================================
# Prometheus Metrics Configuration
# ==========================================
LOGS_PROCESSED = Counter(
    "deeplog_logs_processed_total", 
    "Total logs processed by the service", 
    ["status"]
)
INFERENCE_LATENCY = Histogram(
    "deeplog_inference_latency_seconds", 
    "Latency of inference requests to KServe"
)
ANOMALIES_DETECTED = Counter(
    "deeplog_anomalies_detected_total", 
    "Total anomalies detected"
)
ACTIVE_TRACES = Gauge(
    "deeplog_active_traces_count", 
    "Number of active block traces tracked in memory"
)

# ==========================================
# Environment Variables & Config
# ==========================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_RAW_TOPIC = os.getenv("KAFKA_RAW_TOPIC", "hdfs-raw-logs")
KAFKA_ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "anomaly-alerts")

# Default KServe model endpoint (standard KServe v1/v2 or MLflow predictor URL)
KSERVE_ENDPOINT = os.getenv(
    "KSERVE_ENDPOINT", 
    "http://deeplog-serving.deeplog.svc.cluster.local/v1/models/deeplog-serving:predict"
)
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", "10"))
TOP_G = int(os.getenv("TOP_G", "9"))

# ==========================================
# Globals & State
# ==========================================
event2idx: Dict[str, int] = {}
idx2event: Dict[int, str] = {}
block_histories: Dict[str, List[int]] = {}
consumer_thread: Optional[threading.Thread] = None
running = False

# Helper functions for log cleaning (must match parser.py)
def extract_block_id(log_line):
    match = re.search(r'(blk_-?\d+)', log_line)
    return match.group(1) if match else None

def clean_log_message(log_line):
    match = re.match(r'^\d{6}\s+\d{6}\s+\d+\s+[A-Z]+\s+[\w$.]+:\s+(.*)$', log_line)
    if match:
        msg = match.group(1)
    else:
        parts = log_line.split(':', 5)
        msg = ':'.join(parts[1:]).strip() if len(parts) > 1 else log_line.strip()
    msg = re.sub(r'blk_-?\d+', '[block_id]', msg)
    msg = re.sub(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', '[ip]', msg)
    msg = re.sub(r'\b\d+\b', '[num]', msg)
    return msg

# ==========================================
# Real-time Kafka Consumer Loop
# ==========================================
def kafka_consumer_worker():
    global running, event2idx, block_histories
    
    print("Initializing Real-time Kafka Consumer...")
    
    # 1. Load Vocab Mapping
    base_dir = os.path.dirname(os.path.abspath(__file__))
    vocab_path = os.path.join(base_dir, "vocab.json")
    if not os.path.exists(vocab_path):
        print(f"ERROR: Vocabulary file not found at {vocab_path}. Run training first!")
        running = False
        return
        
    with open(vocab_path, 'r') as f:
        vocab_data = json.load(f)
        event2idx = vocab_data['event2idx']
        print(f"Loaded vocabulary mapping containing {len(event2idx)} events.")

    # 2. Load Drain3 state
    drain_config_path = os.path.join(base_dir, "drain3.ini")
    drain_state_path = os.path.join(base_dir, "drain_state.bin")
    
    if not os.path.exists(drain_state_path):
        print(f"ERROR: Drain3 state file not found at {drain_state_path}. Run parser.py first!")
        running = False
        return
        
    config = TemplateMinerConfig()
    config.load(drain_config_path)
    
    # Load parser in read-only mode by leveraging persistence handler
    persistence_handler = FilePersistenceHandler(drain_state_path)
    template_miner = TemplateMiner(persistence_handler=persistence_handler, config=config)
    print(f"Loaded Drain3 TemplateMiner with {len(template_miner.drain.clusters)} templates.")

    # 3. Connect to Kafka
    bootstrap_list = KAFKA_BOOTSTRAP_SERVERS.split(",")
    try:
        consumer = KafkaConsumer(
            KAFKA_RAW_TOPIC,
            bootstrap_servers=bootstrap_list,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: x.decode('utf-8')
        )
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_list,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"Connected to Kafka. Consuming from '{KAFKA_RAW_TOPIC}'...")
    except Exception as e:
        print(f"ERROR: Failed to connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}: {e}")
        running = False
        return

    # 4. Stream consumption
    while running:
        # Poll messages
        msg_pack = consumer.poll(timeout_ms=500)
        for tp, messages in msg_pack.items():
            for message in messages:
                raw_log = message.value
                block_id = extract_block_id(raw_log)
                if not block_id:
                    LOGS_PROCESSED.labels(status="skipped").inc()
                    continue
                    
                # Parse log line using matching logic
                cleaned_msg = clean_log_message(raw_log)
                match_res = template_miner.match(cleaned_msg)
                
                if match_res:
                    event_id = f"E{match_res.cluster_id}"
                else:
                    # If it's a completely new unseen event, treat it as unknown
                    event_id = "<UNK>"
                    
                # Map EventID to Index (0 for <PAD> or unknown)
                event_idx = event2idx.get(event_id, 0)
                
                # Fetch history for block ID
                if block_id not in block_histories:
                    block_histories[block_id] = []
                history = block_histories[block_id]
                
                # Form input sequence (length W)
                pad_len = WINDOW_SIZE - len(history)
                padded_input = [0] * pad_len + history
                
                # Prepare payload for KServe Model
                # Standard KServe V1/V2 or MLflow format:
                # KServe MLflow accepts: {"instances": [[0, 0, 0, ...]]}
                payload = {
                    "instances": [padded_input]
                }
                
                # Request inference from KServe
                start_time = time.time()
                is_anomaly = False
                try:
                    res = requests.post(KSERVE_ENDPOINT, json=payload, timeout=2.0)
                    elapsed = time.time() - start_time
                    INFERENCE_LATENCY.observe(elapsed)
                    
                    if res.status_code == 200:
                        predictions = res.json().get("predictions", [])
                        if predictions:
                            # Logits or probability vector
                            probs = predictions[0]
                            # Get indices of top-g largest values
                            top_g_indices = np_top_k(probs, TOP_G)
                            
                            # Check if the actual current event_idx is in the predicted candidates
                            if event_idx not in top_g_indices:
                                is_anomaly = True
                    else:
                        print(f"KServe error: HTTP {res.status_code} - {res.text}")
                except Exception as ex:
                    print(f"Failed to query KServe at {KSERVE_ENDPOINT}: {ex}")
                    
                # Send alert to Kafka if anomaly is detected
                if is_anomaly:
                    ANOMALIES_DETECTED.inc()
                    alert_payload = {
                        "timestamp": time.time(),
                        "block_id": block_id,
                        "raw_log": raw_log,
                        "parsed_event": event_id,
                        "window_history": padded_input,
                        "prediction_status": "anomaly"
                    }
                    try:
                        producer.send(KAFKA_ALERT_TOPIC, value=alert_payload)
                    except Exception as pe:
                        print(f"Failed to send alert to Kafka: {pe}")
                        
                # Update history (keep last W events)
                history.append(event_idx)
                if len(history) > WINDOW_SIZE:
                    history.pop(0)
                    
                block_histories[block_id] = history
                ACTIVE_TRACES.set(len(block_histories))
                LOGS_PROCESSED.labels(status="success").inc()

    consumer.close()
    producer.close()
    print("Kafka consumer/producer closed.")

def np_top_k(probs: List[float], k: int) -> List[int]:
    """Helper to find indices of top k elements in a list"""
    indexed = [(val, idx) for idx, val in enumerate(probs)]
    indexed.sort(key=lambda x: x[0], reverse=True)
    return [idx for _, idx in indexed[:k]]

# ==========================================
# FastAPI Lifecycle Hooks
# ==========================================
@app.on_event("startup")
def startup_event():
    global consumer_thread, running
    running = True
    consumer_thread = threading.Thread(target=kafka_consumer_worker, daemon=True)
    consumer_thread.start()
    print("FastAPI Service started background worker.")

@app.on_event("shutdown")
def shutdown_event():
    global running, consumer_thread
    running = False
    if consumer_thread:
        consumer_thread.join(timeout=3)
    print("FastAPI Service shutdown completed.")

@app.get("/healthz")
def healthz():
    return {"status": "healthy", "worker_running": running}

@app.get("/api/v1/status")
def get_status():
    return {
        "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "kserve_endpoint": KSERVE_ENDPOINT,
        "window_size": WINDOW_SIZE,
        "top_g": TOP_G,
        "tracked_active_blocks": len(block_histories)
    }

# Expose Prometheus Metrics at /metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)
