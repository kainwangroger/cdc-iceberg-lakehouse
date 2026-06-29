terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

locals {
  prefix = "cdc-${random_string.suffix.result}"
}

resource "azurerm_resource_group" "main" {
  name     = "${local.prefix}-rg"
  location = var.location
}

resource "azurerm_postgresql_flexible_server" "source" {
  name                         = "${local.prefix}-psql"
  resource_group_name          = azurerm_resource_group.main.name
  location                     = azurerm_resource_group.main.location
  version                      = "16"
  administrator_login          = var.postgres_admin_user
  administrator_password       = var.postgres_admin_password
  sku_name                     = "B_Standard_B1ms"
  storage_mb                   = 32768
  geo_redundant_backup_enabled = false
  zone                         = null

  depends_on = [azurerm_resource_group.main]
}

resource "azurerm_postgresql_flexible_server_database" "source_db" {
  name      = "source_db"
  server_id = azurerm_postgresql_flexible_server.source.id
  charset   = "UTF8"
}

resource "azurerm_postgresql_flexible_server_firewall_rule" "allow_azure" {
  name             = "AllowAzureServices"
  server_id        = azurerm_postgresql_flexible_server.source.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

resource "azurerm_storage_account" "minio_alt" {
  name                     = replace("${local.prefix}minio", "-", "")
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "warehouse" {
  name                  = "iceberg-warehouse"
  storage_account_name  = azurerm_storage_account.minio_alt.name
  container_access_type = "private"
}

resource "azurerm_container_group" "pipeline" {
  name                = "${local.prefix}-cg"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  os_type             = "Linux"
  ip_address_type     = "Public"
  dns_name_label      = local.prefix

  container {
    name   = "kafka"
    image  = "apache/kafka:3.9.0"
    cpu    = 1.0
    memory = 2.0

    environment_variables = {
      CLUSTER_ID                        = "cdc-cluster-001"
      KAFKA_NODE_ID                     = "1"
      KAFKA_PROCESS_ROLES               = "broker,controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS    = "1@localhost:9093"
      KAFKA_LISTENERS                   = "PLAINTEXT://:9092,CONTROLLER://:9093"
      KAFKA_ADVERTISED_LISTENERS        = "PLAINTEXT://kafka:9092"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR = "1"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR = "1"
    }

    ports {
      port     = 9092
      protocol = "TCP"
    }
  }

  container {
    name   = "nessie"
    image  = "projectnessie/nessie:latest"
    cpu    = 0.5
    memory = 1.0

    environment_variables = {
      QUARKUS_HTTP_PORT          = "19120"
      NESSIE_VERSION_STORE_TYPE  = "ROCKSDB"
    }

    ports {
      port     = 19120
      protocol = "TCP"
    }
  }

  container {
    name   = "trino"
    image  = "trinodb/trino:470"
    cpu    = 1.0
    memory = 2.0

    ports {
      port     = 8080
      protocol = "TCP"
    }
  }
}
