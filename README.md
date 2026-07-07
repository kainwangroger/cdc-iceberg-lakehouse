# Pipeline CDC - Debezium + Kafka + Spark + Iceberg
**Stack :** Debezium, Kafka Connect, Apache Kafka 3.9, Apache Spark 3.5, Apache Iceberg 1.6, MinIO/S3, Nessie, Trino
**Volume :** 10M+ changements/heure | **Latence :** < 1 minute

## Comprendre le projet
### Contexte
Une plateforme SaaS possede une base PostgreSQL de 5TB qui doit etre synchronisee en temps reel avec un lakehouse pour l'analytics, sans downtime. Chaque fois qu'un client est modifie, qu'une commande est passee ou qu'un stock est mis a jour, le changement doit arriver instantanement dans le lakehouse. Le CDC (Change Data Capture) capture chaque INSERT/UPDATE/DELETE au fil de l'eau via Debezium, les achemine dans Kafka, les ecrit dans Iceberg via Spark, et permet le versionnement Git-like des donnees avec Nessie et le time travel avec Trino.

## 1. Presentation & Specifications Metier

Le pipeline repose sur 5 composants interconnectes :

- **Source PostgreSQL (16)** : WAL level logical active, publication `cdc_pub` sur 4 tables (customers, orders, order_items, inventory)
- **Debezium PostgreSQL Connector** : Snapshot initial + CDC continu, topics Kafka prefixes `cdc.postgres.sales.*`
- **Kafka 3.9 (KRaft)** : Topics avec compression Snappy, retention 24h pour re-streaming
- **Spark Structured Streaming** : Merge CDC (UPSERT + DELETE) sur Iceberg, checkpoint sur MinIO, fenetre de 10 secondes
- **Iceberg + Nessie + MinIO** : Format de table avec snapshot isolation et schema evolution, catalogue Nessie avec branches Git-like
- **Trino** : SQL direct avec time travel et zero-copy branching

Tables source : `customers` (id, name, email, loyalty_tier), `orders` (id, customer_id, status, total_amount), `order_items` (id, order_id, product_id, quantity, unit_price), `inventory` (product_id, product_name, quantity, reorder_level).

## 2. Architecture Technique

```
  PostgreSQL 16 (source)
  Tables: sales.customers, orders, order_items, inventory
  WAL level: logical | Publication: cdc_pub
          |
  Debezium PostgreSQL Connector (Kafka Connect)
  Capture chaque INSERT / UPDATE / DELETE
  Topics: cdc.postgres.sales.{customers,orders,inventory}
          |
  Kafka 3.9 (KRaft mode)
  Topics avec compression Snappy
  Retention 24h pour re-streaming
          |
  Spark Structured Streaming
  Merge CDC : UPSERT + DELETE sur Iceberg
  Checkpoint sur MinIO
          |
  Apache Iceberg sur MinIO/S3
  Format de table : snapshot isolation, schema evolution
  Catalogue : Nessie (Git-like branching)
          |
     +----+----+
     |         |
  Trino     Nessie
  SQL       Branching
  Time      Data Science
  travel    Zero-copy fork
```

## 3. Structure du Projet

```
.
  docker-compose.yml
  .env.example

  src/
    debezium/
      register-postgres-connector.sh
    spark-streaming/
      cdc_to_iceberg.py
      submit_job.sh
    scripts/
      init-source.sql
      generate_changes.py
    trino/
      catalog/
        iceberg.properties
        postgresql.properties
      queries_demo.sql
      queries_nessie_branching.sql
    monitoring/
      grafana/
        datasources.yml
        dashboards.yml
      dashboards/
        cdc_pipeline.json
```

## 4. Guide de Demarrage Rapide

```bash
# 1. Lancer l'infrastructure
docker compose up -d

# 2. Verifier que tout est pret
docker compose ps

# 3. Enregistrer le connector Debezium
bash src/debezium/register-postgres-connector.sh

# 4. Generer des changements sur PostgreSQL
pip install psycopg2-binary
python src/scripts/generate_changes.py

# 5. Voir les topics Kafka
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic cdc.postgres.sales.customers \
  --from-beginning
```

**Ports :**

| Service | Port | Acces |
|---------|------|-------|
| PostgreSQL source | 5432 | |
| Kafka | 9092 | |
| Kafka Connect | 8083 | http://localhost:8083 |
| MinIO API | 9002 | |
| MinIO Console | 9003 | http://localhost:9003 |
| Nessie | 19120 | http://localhost:19120 |
| Spark UI | 4040 | http://localhost:4040 |
| Trino | 8081 | `trino --server localhost:8081` |
| Grafana | 3001 | http://localhost:3001 (admin/admin) |

**Demonstration Nessie (Zero-Copy Branching) :**

```sql
-- Sur Trino (port 8081)
-- Creer une branche pour un data scientist
CALL iceberg.system.create_branch('iceberg_warehouse', 'ds_experiment', 'main');

-- Basculer sur la branche
SET SESSION iceberg.nessie_catalog_ref = 'ds_experiment';

-- Modifier sans risque
UPDATE iceberg.iceberg_warehouse.customers
SET loyalty_tier = 'platinum' WHERE customer_id = 1;

-- Revenir a main (inchange)
SET SESSION iceberg.nessie_catalog_ref = 'main';
```

## 5. Validation, Metriques & Observabilite

Le dashboard Grafana auto-provisionne couvre : Kafka Consumer Lag, CDC Events per table, Iceberg Snapshot Size, Connect Connector Status, Trino Queries/min, Spark Streaming Progress.

Flux des donnees :
1. `generate_changes.py` -> INSERT/UPDATE/DELETE sur PostgreSQL
2. PostgreSQL WAL -> Debezium connector capture le changement
3. Debezium -> evenement JSON dans Kafka topic `cdc.postgres.sales.{table}`
4. Spark Streaming -> lit Kafka, parse CDC events
5. Spark -> MERGE INTO Iceberg table (UPSERT ou DELETE)
6. Trino -> requetes SQL avec time travel + branching

Le generateur de changements simule en continu : nouveaux clients (INSERT), changements de loyalty tier (UPDATE), nouvelles commandes (INSERT), mises a jour de statut (UPDATE), ajustements de stock (UPDATE).

## Skills Demonstrated

- Change Data Capture (CDC) avec Debezium et Kafka Connect
- Streaming temps reel avec Apache Kafka (KRaft mode)
- Apache Iceberg pour le lakehouse transactionnel (ACID, schema evolution, snapshot isolation)
- Spark Structured Streaming avec exactly-once semantics
- Nessie catalog pour le versionnement Git-like des donnees
- Trino pour le time travel et le zero-copy branching
- Gestion des schemas evolutifs (schema change events Debezium)
- Merge CDC (UPSERT + DELETE) a haute velocite
- Retention des topics Kafka pour re-streaming
- Gestion des tombstones (deletes) dans Iceberg
- Monitoring du lag Kafka Consumer et de la sante du pipeline
