import time
import json
import logging
import os
from .config import load_config
from .azure_client import AzureClient
from .secops_client import SecOpsClient
from .state_manager import StateManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting SecOps Forwarder...")
    config = load_config()
    
    secops_client = SecOpsClient(
        ingestion_endpoint=config.gsecops.ingestion_endpoint,
        customer_id=config.gsecops.customer_id
    )

    # Connection string for Table Storage (State)
    # In production, this should be separate or derived from a specific account
    # For this implementation, we'll assume the first storage account's connection string 
    # or a dedicated env var is used for state.
    # SIMPLIFICATION: We'll use an env var AZURE_STATE_CONNECTION_STRING or fall back to the first account's URL (which won't work directly without a key, so we assume env var).
    state_connection_string = os.getenv("AZURE_STATE_CONNECTION_STRING")
    if not state_connection_string:
        logger.warning("AZURE_STATE_CONNECTION_STRING not set. State persistence might fail if not using Managed Identity.")
        # Fallback logic or error would go here.
    
    state_manager = StateManager(connection_string=state_connection_string, table_name=config.forwarder.state_container)

    while True:
        logger.info("Polling for new logs...")
        
        for tenant in config.azure.tenants:
            for sa_config in tenant.storage_accounts:
                try:
                    # Note: AzureClient now needs a way to get credentials. 
                    # We assume DefaultAzureCredential is functional in the environment.
                    azure_client = AzureClient(account_url=sa_config.account_url)
                    
                    for container_config in sa_config.containers:
                        logger.info(f"Checking container: {container_config.name}")
                        
                        prefixes = container_config.prefixes if container_config.prefixes else [None]
                        
                        for prefix in prefixes:
                            blobs = azure_client.list_blobs(container_config.name, prefix=prefix)
                            
                            for blob in blobs:
                                last_modified = blob.last_modified.isoformat()
                                
                                if state_manager.is_processed(container_config.name, blob.name, last_modified):
                                    continue
                                
                                logger.info(f"Processing new blob: {blob.name}")
                                
                                try:
                                    # Stream blob content
                                    log_entries = []
                                    buffer = ""
                                    
                                    # Stream chunks and process line-by-line
                                    for chunk in azure_client.stream_blob(container_config.name, blob.name):
                                        text_chunk = chunk.decode('utf-8', errors='ignore')
                                        buffer += text_chunk
                                        
                                        while '\n' in buffer:
                                            line, buffer = buffer.split('\n', 1)
                                            if line.strip():
                                                try:
                                                    log_entries.append(json.loads(line))
                                                except json.JSONDecodeError:
                                                    pass # Skip malformed lines
                                    
                                    # Process remaining buffer
                                    if buffer.strip():
                                        try:
                                            log_entries.append(json.loads(buffer))
                                        except json.JSONDecodeError:
                                            pass

                                    # Send to SecOps (now handles batching internally)
                                    if log_entries:
                                        secops_client.send_logs(log_entries, container_config.log_type)
                                    
                                    # CRITICAL FIX: Mark processed ONLY after success
                                    state_manager.mark_processed(container_config.name, blob.name, last_modified)
                                    
                                except Exception as e:
                                    logger.error(f"Failed to process blob {blob.name}: {e}")
                                    # Do NOT mark as processed, so it retries
                                    
                except Exception as e:
                    logger.error(f"Error processing storage account {sa_config.name}: {e}")

        logger.info(f"Sleeping for {config.forwarder.poll_interval_seconds} seconds...")
        time.sleep(config.forwarder.poll_interval_seconds)

if __name__ == "__main__":
    main()
