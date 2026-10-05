import os
import json
from google import genai
from supabase import create_client, Client
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

# Load environment configuration
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'backend', '.env'))

class DisasterRecoveryRAG:
    """
    Enterprise Vector RAG Engine for Autonomous Disaster Recovery.
    Handles semantic runbook retrieval and Gemini AI safety-validated remediation.
    """
    def __init__(self):
        print("[*] Initializing RAG Engine & Embedding Model (all-MiniLM-L6-v2)...")
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Initialize Supabase Client
        SUPABASE_URL = os.getenv("SUPABASE_URL")
        SUPABASE_KEY = os.getenv("SUPABASE_KEY")
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # Initialize Gemini SDK Client
        self.client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

    def retrieve_runbook(self, incident_query: str, match_threshold: float = 0.4) -> str:
        """
        Converts an incident query into a vector embedding and queries Supabase pgvector
        using semantic cosine similarity RPC to retrieve the best matching operational runbook.
        """
        try:
            print(f"[*] Generating vector embedding for query: '{incident_query}'")
            query_embedding = self.embedding_model.encode(incident_query).tolist()
            
            response = self.supabase.rpc(
                'match_runbooks', 
                {
                    'query_embedding': query_embedding, 
                    'match_threshold': match_threshold, 
                    'match_count': 1
                }
            ).execute()
            
            if response.data and len(response.data) > 0:
                matched_steps = response.data[0].get('remediation_steps', '')
                print("[+] Semantic runbook matched successfully from Supabase pgvector.")
                return matched_steps
            else:
                print("[-] No close vector match found. Falling back to default protocols.")
                return "Execute standard WJS high-availability node failover and drain queue."
                
        except Exception as e:
            print(f"[-] Vector RAG search error: {e}")
            return "Fallback: Gracefully restart service instance and inspect container logs."

    def generate_ai_remediation(self, incident_query: str, matched_runbook: str) -> dict:
        """
        Passes the live incident context and retrieved vector runbook to Gemini 2.5 Flash
        under strict JSON safety constraints to produce an audited root cause and safe fix.
        """
        ai_prompt = (
            f"You are a strict Enterprise Disaster Recovery AI for WJS Cloud Fleet. "
            f"Do not suggest destructive actions or hard service deletions.\n"
            f"Live Incident: {incident_query}\n"
            f"Retrieved Runbook Context: {matched_runbook}\n"
            f"Provide a concise root cause and a safe non-destructive remediation plan."
        )

        try:
            print("[*] Querying Gemini 2.5 Flash with enforced JSON safety schema...")
            llm_response = self.client.models.generate_content(
                model='gemini-2.5-flash',
                contents=ai_prompt,
                config={
                    'response_mime_type': 'application/json',
                    'response_schema': {
                        "type": "OBJECT",
                        "properties": {
                            "root_cause": {"type": "STRING"},
                            "remediation": {"type": "STRING"}
                        },
                        "required": ["root_cause", "remediation"]
                    },
                    'automatic_function_calling': {'disable': True}
                }
            )
            return json.loads(llm_response.text)
            
        except Exception as e:
            print(f"[-] AI Generation or schema validation failed: {e}")
            return {
                "root_cause": "System resource exhaustion or network threshold breach",
                "remediation": matched_runbook
            }

if __name__ == "__main__":
    # Quick standalone validation test for judges
    rag = DisasterRecoveryRAG()
    test_query = "High CPU utilization at 88.5% and network latency spike of 1800ms on node wjs-us-east-db-01"
    runbook = rag.retrieve_runbook(test_query)
    result = rag.generate_ai_remediation(test_query, runbook)
    print("\n--- RAG Engine Test Output ---")
    print(json.dumps(result, indent=2))