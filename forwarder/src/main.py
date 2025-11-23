import time
import json
import logging
import os
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from prometheus_client import start_http_server
from .config import load_config
from .azure_client import AzureClient
from .secops_client import SecOpsClient
from .state_manager import StateManager
from .metrics import (
    BLOBS_FOUND, BLOBS_PROCESSED, BLOBS_FAILED, LOG_ENTRIES_SKIPPED,
    BLOB_SIZE_BYTES, PROCESSING_TIME_SECONDS, FORWARDER_UP
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global flag for shutdown
shutdown_event = threading.Event()

def signal_handler(signum, frame):
    logger.info("Shutdown signal received. Exiting...")
    shutdown_event.set()

def process_container(container_config, sa_config, azure_client, state_manager, secops_client, batch_size_limit):
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

            BLOBS_FOUND.labels(container=container_config.name, storage_account=sa_config.name).inc()
            
            last_modified = blob.last_modified.isoformat()
            etag = blob.etag
            size = blob.size
            
            if state_manager.is_processed(container_config.name, blob.name, etag, size):
                continue
            
            logger.info(f"Processing new blob: {blob.name} (Size: {size})")
            start_time = time.time()
            
            try:
                # Stream blob content and batch send immediately
                current_batch = []
                
                buffer = ""
                batch_accumulator = []
                batch_size_estimate = 0
                # Use configured batch size limit (e.g. 5MB) for intermediate flushes
                MAX_BATCH_SIZE = batch_size_limit 
                
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
                                LOG_ENTRIES_SKIPPED.labels(container=container_config.name).inc()
                
                # Process remaining buffer
                if buffer.strip():
                    try:
                        log_entry = json.loads(buffer)
                        batch_accumulator.append(log_entry)
                    except json.JSONDecodeError:
                        logger.warning(f"Skipping malformed JSON in buffer for {blob.name}")
                        LOG_ENTRIES_SKIPPED.labels(container=container_config.name).inc()

                # Send remaining logs
                if batch_accumulator:
                    secops_client.send_logs(batch_accumulator, container_config.log_type)
                
                # Mark processed ONLY after success
                state_manager.mark_processed(container_config.name, blob.name, etag, size, last_modified)
                
                BLOBS_PROCESSED.labels(container=container_config.name, storage_account=sa_config.name).inc()
                BLOB_SIZE_BYTES.labels(container=container_config.name).observe(size)
                PROCESSING_TIME_SECONDS.labels(container=container_config.name).observe(time.time() - start_time)
                
            except Exception as e:
                logger.error(f"Failed to process blob {blob.name}: {e}")
                BLOBS_FAILED.labels(container=container_config.name, storage_account=sa_config.name).inc()
                # Do NOT mark as processed, so it retries

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting SecOps Forwarder...")
    
    # Start Prometheus Metrics Server
    start_http_server(8000)
    FORWARDER_UP.set(1)
    
    try:
        config = load_config()
    except Exception as e:
        logger.critical(f"Failed to load config: {e}")
        return

    secops_client = SecOpsClient(
        ingestion_endpoint=config.gsecops.ingestion_endpoint,
        customer_id=config.gsecops.customer_id,
        max_payload_size_bytes=config.forwarder.max_bytes_per_batch
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
                    # Check for per-account connection string env var
                    connection_string = None
                    if sa_config.connection_string_env_var:
                        connection_string = os.getenv(sa_config.connection_string_env_var)
                        if not connection_string:
                            logger.warning(f"Env var {sa_config.connection_string_env_var} not set for SA {sa_config.name}. Falling back to global/default.")
                    
                    # Fallback to global
                    if not connection_string:
                        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
                        
                    azure_client = AzureClient(account_url=sa_config.account_url, connection_string=connection_string)
                    
                    for container_config in sa_config.containers:
                        work_items.append((container_config, sa_config, azure_client))
                except Exception as e:
                    logger.error(f"Failed to create client for SA {sa_config.name}: {e}")

        # Execute in parallel
        with ThreadPoolExecutor(max_workers=config.forwarder.max_parallel_containers or 4) as executor:
            # We use half of the max payload size for intermediate flushing to be safe and allow SecOpsClient to batch efficiently
            intermediate_batch_limit = int(config.forwarder.max_bytes_per_batch / 2)
            futures = [
                executor.submit(process_container, cc, sac, ac, state_manager, secops_client, intermediate_batch_limit) 
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
