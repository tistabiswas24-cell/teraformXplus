import psycopg2

print("⏳ Connecting to PostgreSQL...")
try:
    conn = psycopg2.connect(
        dbname="incident_logs",
        user="admin",
        password="password",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()
    
    # Create the incident reports table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ai_reports (
            id SERIAL PRIMARY KEY,
            server_id VARCHAR(50) NOT NULL,
            cpu_usage FLOAT NOT NULL,
            latency_ms FLOAT NOT NULL,
            root_cause TEXT,
            remediation TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ PostgreSQL 'ai_reports' table created successfully!")
except Exception as e:
    print(f"❌ Database error: {e}")