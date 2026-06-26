-- ══════════════════════════════════════════
-- Trino Demo Queries — CDC Iceberg Lakehouse
-- ══════════════════════════════════════════

-- Connexion:
--   trino --server localhost:8080 --catalog iceberg --schema iceberg_warehouse

-- ── 1. Voir les tables Iceberg ────────────
SHOW TABLES FROM iceberg.iceberg_warehouse;

-- ── 2. Requête directe sur les données ────
SELECT * FROM iceberg.iceberg_warehouse.customers LIMIT 10;

SELECT * FROM iceberg.iceberg_warehouse.orders LIMIT 10;

-- ── 3. Jointure cross-domaine (Postgres source vs Iceberg) ──
SELECT
    c.name,
    c.loyalty_tier,
    o.total_amount,
    o.status
FROM iceberg.iceberg_warehouse.customers c
JOIN iceberg.iceberg_warehouse.orders o ON c.customer_id = o.customer_id
ORDER BY o.total_amount DESC;

-- ── 4. Source PostgreSQL directe ──────────
SELECT * FROM postgresql.sales.customers LIMIT 5;

-- ── 5. Time Travel (Iceberg) ──────────────
-- Voir l'état des données à un moment précis
SELECT * FROM iceberg.iceberg_warehouse.customers
FOR TIMESTAMP AS OF TIMESTAMP '2026-06-26 12:00:00';

-- ── 6. Nessie Branching (créé un fork pour Data Science) ──
-- Dans Trino:
--   CALL iceberg.system.create_branch('iceberg_warehouse', 'ds_experiment', 'main');
--   SET SESSION iceberg.nessie_catalog_ref = 'ds_experiment';
--   -- Modifications isolées sur la branche
--   INSERT INTO iceberg.iceberg_warehouse.customers VALUES (999, 'Test DS', 'ds@test.com', null, 'gold', null);
--   -- Switch back to main
--   SET SESSION iceberg.nessie_catalog_ref = 'main';

-- ── 7. Métadonnées Iceberg ────────────────
-- Historique des snapshots
SELECT * FROM iceberg.iceberg_warehouse."customers$history" ORDER BY made_current_at DESC;

-- Fichiers de données
SELECT * FROM iceberg.iceberg_warehouse."customers$files";

-- ── 8. Audit des changements ──────────────
SELECT
    table_name,
    COUNT(*) AS change_count,
    MIN(event_ts_ms) AS first_event,
    MAX(event_ts_ms) AS last_event
FROM iceberg.iceberg_warehouse.customers
GROUP BY table_name;
