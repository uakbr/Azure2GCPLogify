import json
import time
import requests
import google.auth
from google.auth.transport.requests import Request as GoogleRequest
from typing import List, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .metrics import (
    SECOPS_BATCHES_SENT, SECOPS_BATCHES_FAILED, LOG_ENTRIES_PROCESSED, BATCH_SIZE_BYTES
)

class SecOpsClient:
    def __init__(self, ingestion_endpoint: str, customer_id: str, max_payload_size_bytes: int = 10 * 1024 * 1024):
        self.ingestion_endpoint = ingestion_endpoint
        self.customer_id = customer_id
        self.credentials, self.project = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self.max_payload_size_bytes = max_payload_size_bytes
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session = requests.Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

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
        # We calculate this exactly to be safe
        base_payload = {
            "customer_id": self.customer_id,
            "log_type": log_type,
            "entries": []
        }
        base_overhead = len(json.dumps(base_payload).encode('utf-8'))
        
        # Current batch size starts with base overhead
        current_batch_size = base_overhead
        
        for log in logs:
            log_str = json.dumps(log)
            # Each entry adds the log size + 1 (comma) roughly, but let's be conservative
            # In the list [a, b], adding b adds ", b"
            log_size = len(log_str.encode('utf-8')) + 2 
            
            # If adding this log exceeds limit, send current batch
            if current_batch_size + log_size > self.max_payload_size_bytes:
                self._send_batch(current_batch, log_type, headers)
                current_batch = []
                current_batch_size = base_overhead
            
            current_batch.append(log)
            current_batch_size += log_size
            
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
            payload_json = json.dumps(payload)
            payload_size = len(payload_json.encode('utf-8'))
            
            response = self.session.post(self.ingestion_endpoint, headers=headers, data=payload_json, timeout=30)
            response.raise_for_status()
            
            SECOPS_BATCHES_SENT.labels(log_type=log_type).inc()
            LOG_ENTRIES_PROCESSED.labels(log_type=log_type).inc(len(entries))
            BATCH_SIZE_BYTES.labels(log_type=log_type).observe(payload_size)
            
        except requests.exceptions.RequestException as e:
            # The retry adapter handles retries for 5xx/429. 
            # If we are here, it's a permanent failure or retries exhausted.
            print(f"Error sending batch: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response content: {e.response.text}")
            
            SECOPS_BATCHES_FAILED.labels(log_type=log_type).inc()
            raise
