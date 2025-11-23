import os
from typing import List, Generator, Any
from azure.storage.blob import BlobServiceClient, BlobProperties
from azure.core.exceptions import ResourceNotFoundError

class AzureClient:
    def __init__(self, account_url: str, credential: Any = None):
        # If credential is not provided, it tries to use DefaultAzureCredential or env vars
        if not credential:
            # For simplicity in this implementation, we expect AZURE_STORAGE_CONNECTION_STRING or similar
            # But typically we'd use DefaultAzureCredential() for production
            from azure.identity import DefaultAzureCredential
            credential = DefaultAzureCredential()
            
        self.service_client = BlobServiceClient(account_url=account_url, credential=credential)

    def list_blobs(self, container_name: str, prefix: str = None) -> Generator[BlobProperties, None, None]:
        container_client = self.service_client.get_container_client(container_name)
        return container_client.list_blobs(name_starts_with=prefix)

    def stream_blob(self, container_name: str, blob_name: str, chunk_size: int = 4 * 1024 * 1024) -> Generator[bytes, None, None]:
        blob_client = self.service_client.get_blob_client(container=container_name, blob=blob_name)
        stream = blob_client.download_blob()
        for chunk in stream.chunks():
            yield chunk

    def blob_exists(self, container_name: str, blob_name: str) -> bool:
        blob_client = self.service_client.get_blob_client(container=container_name, blob=blob_name)
        return blob_client.exists()

    def upload_blob(self, container_name: str, blob_name: str, data: bytes, overwrite: bool = True):
        blob_client = self.service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.upload_blob(data, overwrite=overwrite)
