import json
import asyncio
from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer

app = FastAPI(title="AI Disaster Recovery API")

KAFKA_BROKER = "localhost:9092"
TOPIC = "infrastructure_telemetry"

# Temporary storage for the latest telemetry
latest_telemetry = []

async def consume_telemetry():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    await consumer.start()
    try:
        async for msg in consumer:
            data = msg.value
            latest_telemetry.append(data)
            # Keep only the last 20 records in memory for testing
            if len(latest_telemetry) > 20:
                latest_telemetry.pop(0)
            print(f"📥 Received from Kafka: {data}")
    finally:
        await consumer.stop()

@app.on_event("startup")
async def startup_event():
    # Start the Kafka consumer in the background when FastAPI boots
    asyncio.create_task(consume_telemetry())

@app.get("/api/telemetry")
async def get_telemetry():
    """Endpoint to check the latest telemetry data."""
    return {"status": "success", "data": latest_telemetry}

@app.get("/")
async def root():
    return {"message": "Backend is running. Go to /docs to test the API."}