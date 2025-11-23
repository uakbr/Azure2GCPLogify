import time
import json
import logging
import os
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from .config import load_config
from .azure_client import AzureClient
from .secops_client import SecOpsClient
from .state_manager import StateManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global flag for shutdown
shutdown_event = threading.Event()

def signal_handler(signum, frame):
    logger.info("Shutdown signal received. Exiting...")
    shutdown_event.set()

def process_container(container_config, sa_config, azure_client, state_manager, secops_client):
    """
    Process a single container.
    """
    logger.info(f"Checking container: {container_config.name}")
    prefixes = container_config.prefixes if container_config.prefixes else [None]
    
    for prefix in prefixes:
        if shutdown_event.is_set():
            break
            
        blobs = azure_client.list_blobs(container_config.name, prefix=prefix)
        
        for blob in blobs:
            if shutdown_event.is_set():
                break

            last_modified = blob.last_modified.isoformat()
            etag = blob.etag
            size = blob.size
            
            if state_manager.is_processed(container_config.name, blob.name, etag, size):
                continue
            
            logger.info(f"Processing new blob: {blob.name} (Size: {size})")
            
            try:
                # Stream blob content and batch send immediately
                # Do NOT accumulate all logs in memory
                current_batch = []
                # We reuse SecOpsClient's internal batching logic by calling send_logs with chunks
                # But SecOpsClient.send_logs expects a list and handles batching. 
                # To be memory efficient, we should accumulate a reasonable chunk here (e.g. 1000 lines or 5MB)
                # and send it. 
                
                buffer = ""
                batch_accumulator = []
                batch_size_estimate = 0
                MAX_BATCH_SIZE = 5 * 1024 * 1024 # 5MB intermediate buffer
                
                for chunk in azure_client.stream_blob(container_config.name, blob.name):
                    text_chunk = chunk.decode('utf-8', errors='ignore')
                    buffer += text_chunk
                    
                    while '\n' in buffer:
                        line, buffer = buffer.split('\n', 1)
                        if line.strip():
                            try:
                                log_entry = json.loads(line)
                                batch_accumulator.append(log_entry)
                                batch_size_estimate += len(line)
                                
                                if batch_size_estimate >= MAX_BATCH_SIZE:
                                    secops_client.send_logs(batch_accumulator, container_config.log_type)
                                    batch_accumulator = []
                                    batch_size_estimate = 0
                                    
                            except json.JSONDecodeError:
                                logger.warning(f"Skipping malformed JSON line in {blob.name}")
                
                # Process remaining buffer
                if buffer.strip():
                    try:
                        log_entry = json.loads(buffer)
                        batch_accumulator.append(log_entry)
                    except json.JSONDecodeError:
                        logger.warning(f"Skipping malformed JSON in buffer for {blob.name}")

                # Send remaining logs
                if batch_accumulator:
                    secops_client.send_logs(batch_accumulator, container_config.log_type)
                
                # Mark processed ONLY after success
                state_manager.mark_processed(container_config.name, blob.name, etag, size, last_modified)
                
            except Exception as e:
                logger.error(f"Failed to process blob {blob.name}: {e}")
                # Do NOT mark as processed, so it retries

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting SecOps Forwarder...")
    
    try:
        config = load_config()
    except Exception as e:
        logger.critical(f"Failed to load config: {e}")
        return

    secops_client = SecOpsClient(
        ingestion_endpoint=config.gsecops.ingestion_endpoint,
        customer_id=config.gsecops.customer_id
    )

    # State Manager Init
    state_connection_string = os.getenv("AZURE_STATE_CONNECTION_STRING")
    # Fallback to first storage account if not set (assuming it has table endpoint and creds work)
    # But code review said this crashes if missing. 
    # We will prioritize env var, then try to find a usable credential from config if possible.
    
    state_manager = None
    try:
        if state_connection_string:
            state_manager = StateManager(connection_string=state_connection_string, table_name=config.forwarder.state_container)
        else:
            # Try to use the first storage account's URL + DefaultAzureCredential
            if config.azure.tenants and config.azure.tenants[0].storage_accounts:
                first_sa = config.azure.tenants[0].storage_accounts[0]
                # Construct table endpoint from blob endpoint (usually replace blob with table)
                table_endpoint = first_sa.account_url.replace(".blob.", ".table.")
                logger.info(f"AZURE_STATE_CONNECTION_STRING not set. Attempting to use Table endpoint: {table_endpoint}")
                state_manager = StateManager(account_url=table_endpoint, table_name=config.forwarder.state_container)
            else:
                 logger.critical("No state connection string and no storage accounts configured.")
                 return
    except Exception as e:
        logger.critical(f"Failed to initialize StateManager: {e}")
        return

    # Main Loop
    while not shutdown_event.is_set():
        logger.info("Polling for new logs...")
        
        # Use ThreadPoolExecutor for concurrency across storage accounts/containers
        # We flatten the work items first
        work_items = []
        for tenant in config.azure.tenants:
            for sa_config in tenant.storage_accounts:
                try:
                    # Determine credential/connection for this SA
                    # In this simple config, we only have account_url. 
                    # We assume DefaultAzureCredential works or AZURE_STORAGE_CONNECTION_STRING is set (but that's global).
                    # If we want per-SA connection strings, we'd need them in config or env vars mapped by name.
                    # For now, we assume DefaultAzureCredential or global env var.
                    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
                    azure_client = AzureClient(account_url=sa_config.account_url, connection_string=connection_string)
                    
                    for container_config in sa_config.containers:
                        work_items.append((container_config, sa_config, azure_client))
                except Exception as e:
                    logger.error(f"Failed to create client for SA {sa_config.name}: {e}")

        # Execute in parallel
        with ThreadPoolExecutor(max_workers=config.forwarder.max_parallel_containers or 4) as executor:
            futures = [
                executor.submit(process_container, cc, sac, ac, state_manager, secops_client) 
                for cc, sac, ac in work_items
            ]
            
            # Wait for all to complete or shutdown
            for future in as_completed(futures):
                if shutdown_event.is_set():
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Container processing task failed: {e}")

        if not shutdown_event.is_set():
            logger.info(f"Sleeping for {config.forwarder.poll_interval_seconds} seconds...")
            shutdown_event.wait(config.forwarder.poll_interval_seconds)

    logger.info("Forwarder stopped.")

if __name__ == "__main__":
    main()
