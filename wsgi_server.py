import json
import random
import asyncio
import time
import requests  # Added requests to talk to WSGI
import psutil

WSGI_URL ="http://127.0.0.1:8000/telemetry"

WJS_SERVERS = [
    "wjs-us-east-db-01",
    "wjs-ap-south-core-02",
    "wjs-eu-west-cache-01",
    "wjs-ap-southeast-api-03"
]

def burn_cpu(duration_seconds=4):
    end_time = time.time() + duration_seconds
    while time.time() < end_time:
        _ = [i * i for i in range(3000000)]

async def generate_telemetry():
    print("🚀 [WJS WSGI PRODUCER] Streaming telemetry to WSGI server...")

    try:
        while True:
            server_id = random.choice(WJS_SERVERS)
            
            if random.random() < 0.25:
                print(f"💥 [REAL OVERLOAD] Burning physical CPU for {server_id}...")
                burn_cpu(duration_seconds=4)

            actual_cpu = psutil.cpu_percent(interval=1.0)
            latency = round(actual_cpu * 15.5 + random.uniform(20.0, 100.0), 1)
            status = "CRITICAL_ANOMALY" if (actual_cpu > 60.0 or latency > 1000.0) else "HEALTHY"

            payload = {
                "server_id": server_id,
                "cpu_usage": actual_cpu,
                "latency_ms": latency,
                "status": status
            }

            # Send data via HTTP POST to your WSGI server
            try:
                response = requests.post(WSGI_URL, json=payload)
                if status == "CRITICAL_ANOMALY":
                    print(f"🔴 [WSGI SENT SPIKE] Node: {server_id} | CPU: {actual_cpu}% | Response: {response.status_code}")
                else:
                    print(f"[>] [WSGI SENT NORMAL] Node: {server_id} | CPU: {actual_cpu}%")
            except Exception as e:
                print(f"[-] Failed to reach WSGI server: {e}")

            await asyncio.sleep(2)
            
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    asyncio.run(generate_telemetry())