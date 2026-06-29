output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "postgres_server_fqdn" {
  value = azurerm_postgresql_flexible_server.source.fqdn
}

output "postgres_database" {
  value = azurerm_postgresql_flexible_server_database.source_db.name
}

output "storage_account" {
  value = azurerm_storage_account.minio_alt.name
}

output "container_group_fqdn" {
  value = "${local.prefix}.${azurerm_resource_group.main.location}.azurecontainer.io"
}

output "nessie_endpoint" {
  value = "http://${azurerm_container_group.pipeline.fqdn}:19120/api/v2"
}

output "trino_endpoint" {
  value = "http://${azurerm_container_group.pipeline.fqdn}:8080"
}
