import subprocess
import time
import requests
from kafka import KafkaConsumer

def main():
    print("=== Phase 2 Verification: Starting Ingestion Test ===")
    
    # 1. Start the FastAPI log generator API in a background process
    print("Launching Log Generator API on port 8000...")
    api_process = subprocess.Popen(
        [".venv/bin/uvicorn", "src.log_generator:app", "--host", "127.0.0.1", "--port", "8000"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Wait for the API to boot
    time.sleep(3)
    
    try:
        # Check API status
        print("Checking API Status...")
        resp = requests.get("http://127.0.0.1:8000/api/v1/generator/status")
        print("API Status Response:", resp.json())
        
        # 2. Start the 3 Mock Log Generators
        print("\nTriggering the 3 Mock Log Generators via API...")
        components = ["namesystem", "blockreceiver", "packetresponder"]
        for component in components:
            r = requests.post(f"http://127.0.0.1:8000/api/v1/generator/{component}/start", json={"delay": 0.01})
            print(f"  Started {component}: {r.json()}")
            
        # Wait a moment for logs to be pushed to Kafka
        time.sleep(2)
        
        # 3. Connect to Kafka and Consume raw log streams
        print("\nConnecting Kafka Consumer to 'hdfs-raw-logs' topic...")
        consumer = KafkaConsumer(
            'hdfs-raw-logs',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: x.decode('utf-8'),
            consumer_timeout_ms=5000  # Exit if no logs received for 5 seconds
        )
        
        print("Waiting for streaming log messages to arrive from Kafka...")
        received_logs = []
        for message in consumer:
            received_logs.append(message.value)
            if len(received_logs) >= 5:
                break
                
        # 4. Print received logs for visual verification
        print(f"\nSuccessfully consumed {len(received_logs)} logs from Kafka:")
        for idx, log in enumerate(received_logs):
            print(f"  [{idx+1}] {log[:120]}...")
            
        assert len(received_logs) >= 5, "Failed to ingest logs into Kafka!"
        print("\nVerification SUCCESS: Logs are successfully flowing to Kafka!")

    except Exception as e:
        print(f"\nVerification FAILED: An error occurred: {e}")
        
    finally:
        # 5. Shut down log generators
        print("\nStopping generators...")
        for component in components:
            try:
                requests.post(f"http://127.0.0.1:8000/api/v1/generator/{component}/stop")
            except Exception:
                pass
                
        # Terminate API process
        print("Terminating API process...")
        api_process.terminate()
        api_process.wait()
        print("API process terminated.")
        print("=== Test Complete ===")

if __name__ == '__main__':
    main()
