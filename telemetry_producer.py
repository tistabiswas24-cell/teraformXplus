import json
import random
import asyncio
import time
from aiokafka import AIOKafkaProducer

KAFKA_BROKER = "127.0.0.1:9092"
TOPIC = "infrastructure_telemetry"

# WJS (Web Journal Services) Cloud Server Fleet
WJS_SERVERS = [
    "wjs-us-east-db-01",
    "wjs-ap-south-core-02",
    "wjs-eu-west-cache-01",
    "wjs-ap-southeast-api-03"
]

def burn_cpu(duration_seconds=4):
    """Heavier loop designed to actually force a visible jump in Windows Task Manager."""
    end_time = time.time() + duration_seconds
    while time.time() < end_time:
        # Much heavier computation over multiple iterations
        _ = [i * i for i in range(2000000)]
        # Add a tiny bit of string processing to keep multiple CPU pipelines active
        _ = str([x for x in range(50000)])

async def generate_telemetry():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()
    print("🚀 [WJS CLOUD SPAM ENGINE] Active & streaming telemetry to Kafka...")

    try:
        while True:
            server_id = random.choice(WJS_SERVERS)
            
            # Normal baseline metrics
            cpu = round(random.uniform(15.0, 40.0), 1)
            latency = round(random.uniform(40.0, 150.0), 1)

            # 30% chance of a real heavy incident matching judge criteria (>60% CPU or >1000ms latency)
            if random.random() < 0.30:
                print(f"💥 [WJS OVERLOAD] Simulating traffic spike on {server_id}...")
                
                # HEAVY OS SPIKE FOR TASK MANAGER!
                burn_cpu(duration_seconds=4)
                
                cpu = round(random.uniform(70.0, 99.1), 1)
                latency = round(random.uniform(1200.0, 3200.0), 1)

            status = "CRITICAL_ANOMALY" if (cpu > 60.0 or latency > 1000.0) else "HEALTHY"

            payload = {
                "server_id": server_id,
                "cpu_usage": cpu,
                "latency_ms": latency,
                "status": status
            }

            if status == "CRITICAL_ANOMALY":
                print(f"🔴 [SPAM ALERT] WJS Node: {server_id} | CPU: {cpu}% (Task Manager Spiked!) | Latency: {latency}ms")
            else:
                print(f"[>] [WJS NORMAL] Node: {server_id} | CPU: {cpu}% | Latency: {latency}ms")

            await producer.send_and_wait(TOPIC, payload)
            await asyncio.sleep(2)
            
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(generate_telemetry())