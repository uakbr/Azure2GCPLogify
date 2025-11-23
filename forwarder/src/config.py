import os
import yaml
from typing import List, Optional
from pydantic import BaseModel, Field

class ContainerConfig(BaseModel):
    name: str
    prefixes: List[str] = Field(default_factory=list)
    log_type: str
    parser_hint: Optional[str] = None

class StorageAccountConfig(BaseModel):
    name: str
    account_url: str
    containers: List[ContainerConfig]

class TenantConfig(BaseModel):
    name: str
    tenant_id: str
    storage_accounts: List[StorageAccountConfig]

class AzureConfig(BaseModel):
    tenants: List[TenantConfig]

class GSecOpsConfig(BaseModel):
    ingestion_endpoint: str
    customer_id: str
    service_account_key_path: Optional[str] = None

class ForwarderConfig(BaseModel):
    batch_size: int = 500
    max_bytes_per_batch: int = 1_000_000
    poll_interval_seconds: int = 60
    state_container: str = "forwarderstate"

class AppConfig(BaseModel):
    env: str
    azure: AzureConfig
    gsecops: GSecOpsConfig
    forwarder: ForwarderConfig

def load_config(config_path: str = "config.yaml") -> AppConfig:
    with open(config_path, "r") as f:
        raw_config = yaml.safe_load(f)
    
    # Override with env vars
    if os.getenv("GSECOPS_CUSTOMER_ID"):
        raw_config["gsecops"]["customer_id"] = os.getenv("GSECOPS_CUSTOMER_ID")
        
    if os.getenv("FORWARDER_POLL_INTERVAL_SECONDS"):
        try:
            raw_config["forwarder"]["poll_interval_seconds"] = int(os.getenv("FORWARDER_POLL_INTERVAL_SECONDS"))
        except ValueError:
            pass

    if os.getenv("FORWARDER_STATE_CONTAINER"):
        raw_config["forwarder"]["state_container"] = os.getenv("FORWARDER_STATE_CONTAINER")

    return AppConfig(**raw_config)
