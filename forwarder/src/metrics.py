from prometheus_client import Counter, Histogram, Gauge

# Counters
BLOBS_FOUND = Counter('secops_forwarder_blobs_found_total', 'Total number of blobs found', ['container', 'storage_account'])
BLOBS_PROCESSED = Counter('secops_forwarder_blobs_processed_total', 'Total number of blobs successfully processed', ['container', 'storage_account'])
BLOBS_FAILED = Counter('secops_forwarder_blobs_failed_total', 'Total number of blobs failed processing', ['container', 'storage_account'])

LOG_ENTRIES_PROCESSED = Counter('secops_forwarder_log_entries_processed_total', 'Total number of log entries processed', ['log_type'])
LOG_ENTRIES_SKIPPED = Counter('secops_forwarder_log_entries_skipped_total', 'Total number of log entries skipped (malformed)', ['container'])

SECOPS_BATCHES_SENT = Counter('secops_forwarder_batches_sent_total', 'Total number of batches sent to SecOps', ['log_type'])
SECOPS_BATCHES_FAILED = Counter('secops_forwarder_batches_failed_total', 'Total number of batches failed to send to SecOps', ['log_type'])

# Histograms
BLOB_SIZE_BYTES = Histogram('secops_forwarder_blob_size_bytes', 'Size of processed blobs in bytes', ['container'])
BATCH_SIZE_BYTES = Histogram('secops_forwarder_batch_size_bytes', 'Size of batches sent to SecOps in bytes', ['log_type'])
PROCESSING_TIME_SECONDS = Histogram('secops_forwarder_processing_time_seconds', 'Time taken to process a blob', ['container'])

# Gauges
FORWARDER_UP = Gauge('secops_forwarder_up', 'Forwarder is running')
