# Azure to Google SecOps Log Ingestion

![Status](https://img.shields.io/badge/Status-Production%20Ready-success)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Terraform](https://img.shields.io/badge/IaC-Terraform-purple)
![Docker](https://img.shields.io/badge/Container-Docker-blue)

A robust, scalable, and stateless forwarder service designed to ingest custom logs from **Microsoft Azure Blob Storage** into **Google Security Operations (SecOps)**.

---

## 📖 Overview

This solution bridges the gap between Azure's storage-centric logging and Google SecOps' ingestion API. While Google SecOps provides native feeds for standard Azure services (Activity Logs, Entra ID), custom applications and specific Azure services often export logs to Blob Storage in formats that require custom handling.

This forwarder provides:
- **Stateless Design**: Uses Azure Table Storage for distributed state tracking.
- **Memory Safety**: Streams large log files to prevent OOM errors.
- **Resilience**: At-least-once delivery guarantee with automatic retries and backoff.
- **Efficiency**: Smart batching to respect SecOps API limits (10MB payloads).
- **Concurrency**: Parallel processing of multiple containers.

---

## 🏗 Architecture

The system follows a poll-process-push model, designed for horizontal scalability.

```mermaid
graph LR
    subgraph Azure Cloud
        A[("Blob Storage\n(Logs)")]
        B[("Table Storage\n(State)")]
    end
    
    subgraph Forwarder Service
        C[("Azure Client\n(Streaming)")]
        D{("Logic Core")}
        E[("SecOps Client\n(Batching)")]
    end
    
    subgraph Google Cloud
        F[("SecOps Ingestion API")]
    end

    A -->|Stream Chunks| C
    C --> D
    D -->|Check/Update| B
    D --> E
    E -->|HTTPS POST| F
```

---

## 🚀 Getting Started

### Prerequisites

- **Azure Subscription**: With Storage Accounts containing logs.
- **Google Cloud Project**: With SecOps API enabled and a Service Account key.
- **Docker**: For building and running the forwarder.
- **Terraform**: For deploying the infrastructure.

### 1. Infrastructure Deployment

We use Terraform to provision the necessary Azure Storage resources, including the Table Storage for state.

```bash
cd infra/terraform
terraform init
terraform apply -var="storage_account_name=stsecopslogs001"
```

This will create:
- Storage Account for logs.
- Blob Containers (`custom-logs`, `activity-logs`, etc.).
- **Table Storage** (`forwarderstate`) for forwarder state tracking.
- **Output**: The connection string for the storage account (sensitive).

### 2. Configuration

Create a `config.yaml` file based on the example:

```yaml
env: "prod"
azure:
  tenants:
    - name: "primary-tenant"
      storage_accounts:
        - name: "stsecopslogs001"
          account_url: "https://stsecopslogs001.blob.core.windows.net"
          containers:
            - name: "custom-logs"
              prefixes: ["firewall/", "waf/"]
              log_type: "AZURE_CUSTOM_FIREWALL"
gsecops:
  ingestion_endpoint: "https://malachiteingestion-pa.googleapis.com/v1/ingestion"
  customer_id: "YOUR_CUSTOMER_ID"
forwarder:
  poll_interval_seconds: 60
  state_container: "forwarderstate" # Table Name
  max_parallel_containers: 4
```

### 3. Running the Forwarder

The forwarder requires credentials for both Azure and Google Cloud.

**Environment Variables:**
- `AZURE_STORAGE_CONNECTION_STRING`: Connection string for the log storage account.
- `AZURE_STATE_CONNECTION_STRING`: Connection string for the state storage account (can be the same as above).
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to your GCP Service Account JSON key.
- `FORWARDER_POLL_INTERVAL_SECONDS`: (Optional) Override poll interval.

**Docker Run:**

```bash
# Build the image
docker build -t secops-forwarder ./forwarder

# Run the container
docker run -d \
  --name secops-forwarder \
  -e AZURE_STORAGE_CONNECTION_STRING="..." \
  -e AZURE_STATE_CONNECTION_STRING="..." \
  -e GOOGLE_APPLICATION_CREDENTIALS="/app/key.json" \
  -v $(pwd)/config.yaml:/app/config.yaml \
  -v $(pwd)/gcp-key.json:/app/key.json \
  secops-forwarder
```

---

## 🛠 Development & Customization

### Adding New Log Sources
1.  **Export**: Configure your Azure resource to export logs to a Blob Container in the SecOps Storage Account.
2.  **Config**: Add the container and prefix to `config.yaml`.
3.  **Restart**: Restart the forwarder to pick up the new configuration (or send SIGTERM to trigger graceful shutdown).

### Monitoring
The forwarder outputs structured logs to `stdout`. Monitor these logs for:
- `INFO`: Normal processing (blobs found, batches sent).
- `WARNING`: Configuration issues or transient errors.
- `ERROR`: Failed ingestion or connectivity issues.

---

## 🔒 Security

- **Least Privilege**: The Azure SAS token or Identity should only have `Storage Blob Data Reader` on log containers and `Storage Table Data Contributor` on the state table.
- **Encryption**: All data in transit is encrypted via TLS 1.2+.
- **Secrets**: Never commit keys to repo. Use Azure Key Vault or GCP Secret Manager in production.

---

## 📄 License

This project is licensed under the Apache 2.0 License.
