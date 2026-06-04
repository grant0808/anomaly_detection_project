import os
import re
import time
import threading
from typing import Dict, Optional
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer

app = FastAPI(title="HDFS Mock Log Generator API", version="1.0.0")

# Global variables to manage generator threads and state
generators: Dict[str, Dict] = {
    "namesystem": {
        "pattern": r"dfs\.FSNamesystem",
        "thread": None,
        "running": False,
        "sent_count": 0,
        "delay": 0.05,
    },
    "blockreceiver": {
        "pattern": r"dfs\.DataNode\$BlockReceiver",
        "thread": None,
        "running": False,
        "sent_count": 0,
        "delay": 0.05,
    },
    "packetresponder": {
        "pattern": r"dfs\.DataNode\$PacketResponder",
        "thread": None,
        "running": False,
        "sent_count": 0,
        "delay": 0.05,
    }
}

# Shared Kafka Producer config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "hdfs-raw-logs")
LOG_FILE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../HDFS_v1/HDFS.log"))

producer: Optional[KafkaProducer] = None

def get_kafka_producer() -> KafkaProducer:
    global producer
    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: v.encode('utf-8'),
                acks=1
            )
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            print(f"Failed to connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}: {e}")
            raise e
    return producer

def generator_worker(name: str):
    info = generators[name]
    pattern = re.compile(info["pattern"])
    delay = info["delay"]
    
    print(f"Generator [{name}] started streaming...")
    
    try:
        kafka_prod = get_kafka_producer()
    except Exception:
        info["running"] = False
        return

    while info["running"]:
        if not os.path.exists(LOG_FILE_PATH):
            print(f"Log file not found at {LOG_FILE_PATH}")
            info["running"] = False
            break
            
        with open(LOG_FILE_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                if not info["running"]:
                    break
                    
                # Filter logs matching the component's pattern
                if pattern.search(line):
                    stripped_line = line.strip()
                    try:
                        # Produce to Kafka
                        kafka_prod.send(KAFKA_TOPIC, value=stripped_line)
                        info["sent_count"] += 1
                    except Exception as e:
                        print(f"[{name}] Kafka send error: {e}")
                        
                    # Configurable throughput delay
                    time.sleep(delay)
                    
            # If we reach EOF and are still running, loop back (indefinite stream)
            if info["running"]:
                print(f"[{name}] Reached EOF, restarting log stream loop...")
                
    print(f"Generator [{name}] stopped.")

class StartRequest(BaseModel):
    delay: float = 0.05

@app.post("/api/v1/generator/{name}/start")
def start_generator(name: str, req: StartRequest):
    if name not in generators:
        raise HTTPException(status_code=404, detail="Generator not found")
        
    info = generators[name]
    if info["running"]:
        return {"status": "already running", "name": name}
        
    info["running"] = True
    info["delay"] = req.delay
    
    thread = threading.Thread(target=generator_worker, args=(name,), daemon=True)
    info["thread"] = thread
    thread.start()
    
    return {"status": "started", "name": name, "delay": req.delay}

@app.post("/api/v1/generator/{name}/stop")
def stop_generator(name: str):
    if name not in generators:
        raise HTTPException(status_code=404, detail="Generator not found")
        
    info = generators[name]
    if not info["running"]:
        return {"status": "already stopped", "name": name}
        
    info["running"] = False
    if info["thread"]:
        info["thread"].join(timeout=2)
        info["thread"] = None
        
    return {"status": "stopped", "name": name}

@app.get("/api/v1/generator/status")
def get_status():
    status_data = {}
    for name, info in generators.items():
        status_data[name] = {
            "running": info["running"],
            "sent_count": info["sent_count"],
            "delay": info["delay"],
            "pattern": info["pattern"]
        }
    return {
        "kafka_connected": producer is not None,
        "kafka_topic": KAFKA_TOPIC,
        "generators": status_data
    }

@app.on_event("shutdown")
def shutdown_event():
    global producer
    print("Shutting down generators...")
    for name, info in generators.items():
        info["running"] = False
        if info["thread"]:
            info["thread"].join(timeout=1)
    if producer:
        producer.close(timeout=2)
        print("Kafka producer closed.")
