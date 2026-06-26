# Pipeline CDC — Debezium + Kafka + Spark + Iceberg

**Stack :** Debezium, Kafka Connect, Apache Kafka 3.9, Apache Spark 3.5, Apache Iceberg 1.6, MinIO/S3, Nessie, Trino  
**Volume :** 10M+ changements/heure | **Latence :** < 1 minute

---

## Comprendre le projet

### En langage simple (non-tech)

Imagine une base de données qui alimente en temps réel un entrepôt de données analytique. Chaque fois qu'un client est modifié, qu'une commande est passée ou qu'un stock est mis à jour, le changement doit arriver instantanément dans le lakehouse.

C'est le **CDC (Change Data Capture)** :
1. **PostgreSQL** est la base source (transactions, clients, stocks)
2. **Debezium** capture chaque INSERT/UPDATE/DELETE au fil de l'eau
3. **Kafka** transporte les événements
4. **Spark** lit Kafka et les écrit dans **Iceberg** (format de table moderne)
5. **Nessie** permet le versionnement Git-like des données
6. **Trino** permet de requêter avec time travel et zero-copy branching

> C'est comme un système de vidéosurveillance pour ta base de données : chaque changement est enregistré, stocké, et tu peux voyager dans le temps pour voir l'état des données à n'importe quel moment.

### En langage technique

```
┌─────────────────────────────────────────────────────────────┐
│  PostgreSQL 16 (source)                                     │
│  Tables: sales.customers, orders, order_items, inventory    │
│  WAL level: logical | Publication: cdc_pub                  │
└────────────────────┬────────────────────────────────────────┘
                     │ WAL streaming
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Debezium PostgreSQL Connector (Kafka Connect)              │
│  Capture chaque INSERT / UPDATE / DELETE                    │
│  Topics: cdc.postgres.sales.{customers,orders,inventory}    │
└────────────────────┬────────────────────────────────────────┘
                     │ Événements CDC (JSON)
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Kafka 3.9 (KRaft mode)                                     │
│  Topics avec compression Snappy                             │
│  Rétention 24h pour re-streaming                            │
└────────────────────┬────────────────────────────────────────┘
                     │ Streaming
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Spark Structured Streaming                                 │
│  Merge CDC : UPSERT + DELETE sur Iceberg                    │
│  Checkpoint sur MinIO                                       │
└────────────────────┬────────────────────────────────────────┘
                     │ Écriture Iceberg
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Apache Iceberg sur MinIO/S3                                │
│  Format de table : snapshot isolation, schema evolution     │
│  Catalogue : Nessie (Git-like branching)                    │
└────────────────────┬────────────────────────────────────────┘
                     │
         ┌───────────┴───────────┐
         ▼                       ▼
┌──────────────────┐   ┌──────────────────┐
│  Trino           │   │  Nessie          │
│  SQL direct      │   │  Branching       │
│  Time travel     │   │  Data Science    │
│  Cross-domain    │   │  Zero-copy fork  │
└──────────────────┘   └──────────────────┘
```

---

## Démarrage rapide

```bash
# 1. Lancer l'infrastructure
docker compose up -d

# 2. Vérifier que tout est prêt
docker compose ps

# 3. Enregistrer le connector Debezium
bash src/debezium/register-postgres-connector.sh

# 4. Générer des changements sur PostgreSQL
pip install psycopg2-binary
python src/scripts/generate_changes.py

# 5. Voir les topics Kafka
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic cdc.postgres.sales.customers \
  --from-beginning
```

---

## Guide des composants

### 1. PostgreSQL Source
- WAL level `logical` activé
- Publication `cdc_pub` sur les 4 tables
- Tables : `customers`, `orders`, `order_items`, `inventory`
- Données seed : 5 clients, 4 commandes, 5 produits

### 2. Debezium Connector
Configuration dans `src/debezium/register-postgres-connector.sh` :
- `plugin.name=pgoutput` — protocole natif PostgreSQL
- `snapshot.mode=initial` — snapshot + CDC continu
- `topic.prefix=cdc` — préfixe des topics Kafka
- Topics créés : `cdc.postgres.sales.{customers,orders,order_items,inventory}`

### 3. Spark Streaming Job (`src/spark-streaming/cdc_to_iceberg.py`)
- Lit les topics Kafka par pattern `cdc.postgres.sales.*`
- Parse les événements Debezium (before/after/op)
- UPSERT + DELETE via `MERGE INTO` Iceberg
- Checkpoint sur MinIO pour exactly-once
- Fenêtre de 10 secondes, 100 events max par trigger

### 4. Iceberg + Nessie + MinIO
- Catalogue Nessie avec support de branches
- Entrepôt Iceberg sur MinIO (`s3://iceberg-warehouse/`)
- Tables : `customers`, `orders`, `inventory`
- Support du time travel

### 5. Trino
- Port : 8081
- Catalog Iceberg : `iceberg.iceberg_warehouse.*`
- Catalog PostgreSQL : `postgresql.sales.*`
- Time travel : `SELECT ... FOR TIMESTAMP AS OF ...`
- Branching Nessie : `CALL iceberg.system.create_branch(...)`

### 6. Générateur de changements (`src/scripts/generate_changes.py`)
Simule en continue :
- Nouveaux clients (INSERT)
- Changements de loyalty tier (UPDATE)
- Nouvelles commandes (INSERT)
- Mises à jour de statut (UPDATE → shipped/delivered/cancelled)
- Ajustements de stock (UPDATE)

---

## Structure du projet

```
.
├── docker-compose.yml              # Infra : Postgres, Kafka, Connect, MinIO, Nessie, Spark, Trino, Grafana
├── .env.example                    # Variables d'environnement
│
├── src/
│   ├── debezium/
│   │   └── register-postgres-connector.sh   # Enregistrer le connector Debezium
│   │
│   ├── spark-streaming/
│   │   ├── cdc_to_iceberg.py                # Spark Streaming job (Kafka → Iceberg)
│   │   └── submit_job.sh                    # Script de soumission Spark
│   │
│   ├── scripts/
│   │   ├── init-source.sql                  # Init PostgreSQL (tables + seed + publication)
│   │   └── generate_changes.py             # Générateur de changements en continu
│   │
│   ├── trino/
│   │   ├── catalog/
│   │   │   ├── iceberg.properties           # Catalog Iceberg (Nessie + MinIO)
│   │   │   └── postgresql.properties        # Catalog PostgreSQL source
│   │   ├── queries_demo.sql                 # Requêtes de démonstration
│   │   └── queries_nessie_branching.sql     # Nessie branching demo
│   │
│   └── monitoring/
│       ├── grafana/
│       │   ├── datasources.yml
│       │   └── dashboards.yml
│       └── dashboards/
│           └── cdc_pipeline.json           # Dashboard Grafana
```

---

## Ports

| Service | Port | Accès |
|---------|------|-------|
| PostgreSQL source | 5432 | |
| Kafka | 9092 | |
| Kafka Connect | 8083 | `http://localhost:8083` |
| MinIO API | 9002 | |
| MinIO Console | 9003 | `http://localhost:9003` |
| Nessie | 19120 | `http://localhost:19120` |
| Spark UI | 4040 | `http://localhost:4040` |
| Trino | 8081 | `trino --server localhost:8081` |
| Grafana | 3001 | `http://localhost:3001` (admin/admin) |

---

## Flux des données

```
1. change_generator.py → INSERT/UPDATE/DELETE sur PostgreSQL
2. PostgreSQL WAL → Debezium connector capture le changement
3. Debezium → événement JSON dans Kafka topic cdc.postgres.sales.{table}
4. Spark Streaming → lit Kafka, parse CDC events
5. Spark → MERGE INTO Iceberg table (UPSERT ou DELETE)
6. Trino → requêtes SQL avec time travel + branching
```

---

## Démo Nessie (Zero-Copy Branching)

```sql
-- Sur Trino (port 8081)
-- Créer une branche pour un data scientist
CALL iceberg.system.create_branch('iceberg_warehouse', 'ds_experiment', 'main');

-- Basculer sur la branche
SET SESSION iceberg.nessie_catalog_ref = 'ds_experiment';

-- Modifier sans risque
UPDATE iceberg.iceberg_warehouse.customers
SET loyalty_tier = 'platinum' WHERE customer_id = 1;

-- Revenir à main (inchangé)
SET SESSION iceberg.nessie_catalog_ref = 'main';
```

---

## Monitoring

Dashboard Grafana auto-provisionné avec :
- Kafka Consumer Lag
- CDC Events per table
- Iceberg Snapshot Size
- Connect Connector Status
- Trino Queries / min
- Spark Streaming Progress

---

## Déploiement GitHub

```bash
git init && git add . && git commit -m "Initial commit - Pipeline CDC Iceberg Lakehouse"
gh repo create cdc-iceberg-lakehouse --public --source=. --push
```

---

## Licence

MIT
