# Terraform — CDC Iceberg Lakehouse (Azure)

## Prérequis

- Azure CLI installé et authentifié (`az login`)
- Terraform >= 1.5

## Utilisation

```bash
export TF_VAR_postgres_admin_password="TonMotDePasseSécurisé"

terraform init
terraform plan
terraform apply -auto-approve
```

## Destruction

```bash
terraform destroy -auto-approve
```

## Ressources créées

| Service | Azure Resource |
|---|---|
| PostgreSQL 16 Flexible Server | Base source pour le CDC |
| Storage Account (Blob) | Warehouse Iceberg (alternative MinIO) |
| Container Group | Kafka, Nessie, Trino |
