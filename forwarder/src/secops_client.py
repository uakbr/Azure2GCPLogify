import json
import requests
import google.auth
from google.auth.transport.requests import Request as GoogleRequest
from typing import List, Dict, Any

class SecOpsClient:
    def __init__(self, ingestion_endpoint: str, customer_id: str):
        self.ingestion_endpoint = ingestion_endpoint
        self.customer_id = customer_id
        self.credentials, self.project = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self.max_payload_size_bytes = 10 * 1024 * 1024  # 10MB limit

    def _get_token(self) -> str:
        if not self.credentials.valid:
            self.credentials.refresh(GoogleRequest())
        return self.credentials.token

    def send_logs(self, logs: List[Dict[str, Any]], log_type: str):
        if not logs:
            return

        token = self._get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # Chunking logic
        current_batch = []
        current_batch_size = 0
        
        # Base overhead for JSON structure: {"customer_id": "...", "log_type": "...", "entries": []}
        # Approximate overhead
        base_overhead = len(self.customer_id) + len(log_type) + 50 
        
        for log in logs:
            log_str = json.dumps(log)
            log_size = len(log_str.encode('utf-8'))
            
            # If adding this log exceeds limit, send current batch
            if current_batch_size + log_size + base_overhead > self.max_payload_size_bytes:
                self._send_batch(current_batch, log_type, headers)
                current_batch = []
                current_batch_size = 0
            
            current_batch.append(log)
            current_batch_size += log_size + 2 # +2 for comma and space
            
        # Send remaining
        if current_batch:
            self._send_batch(current_batch, log_type, headers)

    def _send_batch(self, entries: List[Dict[str, Any]], log_type: str, headers: Dict[str, str]):
        payload = {
            "customer_id": self.customer_id,
            "log_type": log_type,
            "entries": entries
        }
        
        try:
            response = requests.post(self.ingestion_endpoint, headers=headers, json=payload)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"Error sending batch: {e.response.text}")
            raise
