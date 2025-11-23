import os
from typing import Any, Optional
from azure.data.tables import TableServiceClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

class StateManager:
    def __init__(self, connection_string: str = None, table_name: str = "forwarderstate", credential: Any = None, account_url: str = None):
        if connection_string:
            self.service_client = TableServiceClient.from_connection_string(conn_str=connection_string)
        elif account_url and credential:
            self.service_client = TableServiceClient(endpoint=account_url, credential=credential)
        else:
            # Fallback to DefaultAzureCredential if account_url is provided but no credential
            if account_url:
                from azure.identity import DefaultAzureCredential
                self.service_client = TableServiceClient(endpoint=account_url, credential=DefaultAzureCredential())
            else:
                 raise ValueError("Either connection_string or account_url must be provided for StateManager.")

        self.table_name = table_name
        self.table_client = self.service_client.get_table_client(table_name=self.table_name)
        
        try:
            self.table_client.create_table()
        except ResourceExistsError:
            pass

    def is_processed(self, container_name: str, blob_name: str, etag: str, size: int) -> bool:
        # PartitionKey: container_name
        # RowKey: blob_name (encoded)
        # We store etag and size to detect changes
        
        row_key = self._encode_row_key(blob_name)
        
        try:
            entity = self.table_client.get_entity(partition_key=container_name, row_key=row_key)
            # Check if ETag matches. If size changed, we might want to reprocess too.
            # For strict exactly-once of immutable logs, ETag check is sufficient.
            return entity.get("etag") == etag and entity.get("size") == size
        except ResourceNotFoundError:
            return False

    def mark_processed(self, container_name: str, blob_name: str, etag: str, size: int, last_modified: str):
        row_key = self._encode_row_key(blob_name)
        
        entity = {
            "PartitionKey": container_name,
            "RowKey": row_key,
            "etag": etag,
            "size": size,
            "last_modified": last_modified
        }
        
        self.table_client.upsert_entity(entity=entity)

    def _encode_row_key(self, key: str) -> str:
        # Azure Table RowKey cannot contain certain characters: / \ # ?
        # We use base64 encoding to be safe
        import base64
        return base64.urlsafe_b64encode(key.encode('utf-8')).decode('utf-8')
