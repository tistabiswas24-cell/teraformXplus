import requests

def fetch_telemetry_from_wsgi():
    try:
        response = requests.get("http://127.0.0.1:8000/telemetry")
        if response.status_code == 200:
            telemetry_data = response.json()
            for entry in telemetry_data:
                print(f"Fetched -> Node: {entry.get('server_id')} | CPU: {entry.get('cpu_usage')}%")
    except Exception as e:
        print(f"Error fetching from WSGI: {e}")

if __name__ == "__main__":
    fetch_telemetry_from_wsgi()