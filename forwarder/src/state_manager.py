import os
from typing import Any
from azure.data.tables import TableServiceClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

class StateManager:
    def __init__(self, connection_string: str, table_name: str = "forwarderstate"):
        self.service_client = TableServiceClient.from_connection_string(conn_str=connection_string)
        self.table_name = table_name
        self.table_client = self.service_client.get_table_client(table_name=self.table_name)
        
        try:
            self.table_client.create_table()
        except ResourceExistsError:
            pass

    def is_processed(self, container_name: str, blob_name: str, last_modified: str) -> bool:
        # PartitionKey: container_name
        # RowKey: blob_name (encoded to be safe for RowKey)
        # We store the last_modified timestamp as a property
        
        row_key = self._encode_row_key(blob_name)
        
        try:
            entity = self.table_client.get_entity(partition_key=container_name, row_key=row_key)
            return entity.get("last_modified") == last_modified
        except ResourceNotFoundError:
            return False

    def mark_processed(self, container_name: str, blob_name: str, last_modified: str):
        row_key = self._encode_row_key(blob_name)
        
        entity = {
            "PartitionKey": container_name,
            "RowKey": row_key,
            "last_modified": last_modified
        }
        
        self.table_client.upsert_entity(entity=entity)

    def _encode_row_key(self, key: str) -> str:
        # Azure Table RowKey cannot contain certain characters: / \ # ?
        # Simple encoding: replace / with | (assuming | is not used or we accept collision risk for simplicity)
        # Better: base64 encode
        import base64
        return base64.urlsafe_b64encode(key.encode('utf-8')).decode('utf-8')
