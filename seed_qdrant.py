from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct
from sentence_transformers import SentenceTransformer

client = QdrantClient(url="http://localhost:6333")
model = SentenceTransformer("all-MiniLM-L6-v2")

collection_name = "dr_runbooks"

if client.collection_exists(collection_name):
    client.delete_collection(collection_name)

client.create_collection(
    collection_name=collection_name,
    vectors_config=VectorParams(size=384, distance=Distance.COSINE),
)

runbooks = [
    {
        "id": 1, 
        "text": "If aws-prod-db-01 CPU exceeds 90% and latency spikes over 1000ms, the root cause is typically a runaway analytical query locking the primary tables. Remediation: 1. Instantly route read-traffic to aws-prod-db-02. 2. Terminate all transactions running longer than 60 seconds on the primary."
    },
    {
        "id": 2, 
        "text": "If API Gateway returns 502 Bad Gateway with high CPU, the backend FastAPI pods are saturated. Remediation: 1. Trigger Kubernetes auto-scaler to add 3 more replicas. 2. Enable aggressive Redis caching for read-heavy endpoints."
    },
    {
        "id": 3, 
        "text": "If Kafka broker latency exceeds 500ms, consumer groups are lagging. Remediation: 1. Increase the number of Kafka partitions for the telemetry topic. 2. Spin up additional AI worker nodes to process the backlog."
    }
]

print("🧠 Loading local embedding model...")
points = []
for doc in runbooks:
    vector = model.encode(doc["text"]).tolist()
    points.append(
        PointStruct(id=doc["id"], vector=vector, payload={"content": doc["text"]})
    )

print("💾 Uploading vectorized runbooks to Qdrant...")
client.upsert(collection_name=collection_name, points=points)
print("✅ Qdrant memory banks seeded successfully!")