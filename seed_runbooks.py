import json
import asyncio
import os
from aiokafka import AIOKafkaConsumer
from google import genai  # <-- NEW SDK IMPORT
from supabase import create_client, Client
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'backend', '.env'))

# 1. Initialize Gemini (Using the new Client structure)
ai_client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

# 2. Initialize Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 3. Load Open-Source Embedding Model
print("[*] Loading Sentence Transformers (all-MiniLM-L6-v2)...")
embedding_model = SentenceTransformer('all-MiniLM-L6-v2')

# 4. Kafka Settings
KAFKA_BROKER = "127.0.0.1:9092"
TOPIC = "infrastructure_telemetry"

async def consume_telemetry():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    
    await consumer.start()
    print("[*] AI Worker online. Listening to Kafka for anomalies...")
    
    try:
        async for msg in consumer:
            data = msg.value
            cpu = data.get("cpu_usage", 0)
            latency = data.get("latency_ms", 0)
            server_id = data.get("server_id", "unknown")
            
            print(f"[>] Received: {server_id} | CPU: {cpu}% | Latency: {latency}ms")
            
            # Trigger on Anomaly
            if cpu > 85 or latency > 300:
                print(f"[!] ANOMALY DETECTED on {server_id}. Generating vector embedding...")
                
                incident_query = f"High CPU {cpu}% and latency {latency}ms on {server_id}"
                query_embedding = embedding_model.encode(incident_query).tolist()
                
                try:
                    # Query Supabase pgvector
                    response = supabase.rpc(
                        'match_runbooks', 
                        {'query_embedding': query_embedding, 'match_threshold': 0.5, 'match_count': 1}
                    ).execute()
                    
                    if response.data:
                        matched_runbook = response.data[0]['remediation_steps']
                    else:
                        matched_runbook = "No specific runbook found. Apply standard DR protocols."
                except Exception as e:
                    print(f"[-] Vector search failed: {e}")
                    matched_runbook = "Fallback: Restart service and monitor logs."
                
                ai_prompt = (
                    f"You are a strict Enterprise Disaster Recovery AI. Do not suggest destructive actions.\n"
                    f"Incident: {incident_query}\n"
                    f"Retrieved Runbook: {matched_runbook}\n"
                    f"Provide a 1-sentence root cause and a 1-sentence safe remediation."
                )
                
                try:
                    # <-- NEW SDK INFERENCE CALL
                    llm_response = ai_client.models.generate_content(
                        model='gemini-1.5-flash',
                        contents=ai_prompt
                    )
                    remediation_plan = llm_response.text.strip()
                except Exception as e:
                    print(f"[-] AI Generation failed: {e}")
                    remediation_plan = matched_runbook

                incident_record = {
                    "server_id": server_id,
                    "cpu_usage": cpu,
                    "latency_ms": latency,
                    "root_cause": f"Vector AI Analysis for {server_id}",
                    "remediation": remediation_plan
                }
                
                try:
                    supabase.table("incident_reports").insert(incident_record).execute()
                    print(f"[+] Successfully saved AI incident report to Supabase for {server_id}\n")
                except Exception as e:
                    print(f"[-] Supabase insert error: {e}")
                    
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume_telemetry())