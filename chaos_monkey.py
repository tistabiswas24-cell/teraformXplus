import time
import random
import requests

API_ENDPOINT = "http://127.0.0.1:8000/api/v1/telemetry"

def send_metric(server_id: str, region: str, cpu: float, mem: float, latency: float, status: str):
    payload = {
        "server_id": server_id,
        "region": region,
        "cpu_usage": round(cpu, 2),
        "memory_usage": round(mem, 2),
        "latency_ms": round(latency, 2),
        "status": status,
        "timestamp": time.time()
    }
    try:
        res = requests.post(API_ENDPOINT, json=payload, timeout=2)
        print(f"[{status}] {server_id}: CPU={cpu}% Latency={latency}ms -> HTTP {res.status_code}")
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    print("Streaming telemetry to FastAPI gateway. Press Ctrl+C to stop.\n")
    tick = 0
    while True:
        tick += 1
        # Every 6 ticks, simulate a severe outage
        if tick % 6 == 0:
            print("\n🚨 INJECTING SIMULATED OUTAGE...")
            send_metric("aws-prod-db-01", "ap-south-1", 99.1, 95.4, 1850.0, "CRITICAL")
        else:
            cpu = random.uniform(15.0, 40.0)
            mem = random.uniform(30.0, 50.0)
            lat = random.uniform(12.0, 30.0)
            send_metric("aws-prod-db-01", "ap-south-1", cpu, mem, lat, "HEALTHY")
        time.sleep(1.5)