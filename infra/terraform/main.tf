resource "azurerm_resource_group" "secops_rg" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_storage_account" "secops_sa" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.secops_rg.name
  location                 = azurerm_resource_group.secops_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"

  tags = {
    environment = var.environment
    project     = "secops-ingestion"
  }
}

# Containers for native SecOps feeds
resource "azurerm_storage_container" "activity_logs" {
  name                  = "activity-logs"
  storage_account_name  = azurerm_storage_account.secops_sa.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "appservice_logs" {
  name                  = "appservice-logs"
  storage_account_name  = azurerm_storage_account.secops_sa.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "loganalytics_export" {
  name                  = "loganalytics-export"
  storage_account_name  = azurerm_storage_account.secops_sa.name
  container_access_type = "private"
}

# Container for custom logs (to be picked up by forwarder)
resource "azurerm_storage_container" "custom_logs" {
  name                  = "custom-logs"
  storage_account_name  = azurerm_storage_account.secops_sa.name
  container_access_type = "private"
}

# Table for forwarder state checkpoints
resource "azurerm_storage_table" "forwarder_state" {
  name                 = "forwarderstate"
  storage_account_name = azurerm_storage_account.secops_sa.name
}

output "storage_account_connection_string" {
  value     = azurerm_storage_account.secops_sa.primary_connection_string
  sensitive = true
}
