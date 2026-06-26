-- ══════════════════════════════════════════
-- Nessie Branching Demo
-- ══════════════════════════════════════════
-- Nessie permet le versionnement Git-like des données
-- Data Scientists peuvent fork les données sans impacter la prod

-- ── 1. Créer une branche depuis main ──────
CALL iceberg.system.create_branch('iceberg_warehouse', 'ds_experiment', 'main');

-- ── 2. Basculer sur la branche ────────────
SET SESSION iceberg.nessie_catalog_ref = 'ds_experiment';

-- ── 3. Faire des modifications isolées ────
INSERT INTO iceberg.iceberg_warehouse.customers
VALUES (999, 'Test Data Science', 'ds@test.com', NULL, 'gold', NULL);

UPDATE iceberg.iceberg_warehouse.customers
SET loyalty_tier = 'platinum'
WHERE customer_id = 1;

-- ── 4. Voir les changements ───────────────
SELECT * FROM iceberg.iceberg_warehouse.customers WHERE customer_id IN (1, 999);

-- ── 5. Revenir à main (inchangé) ──────────
SET SESSION iceberg.nessie_catalog_ref = 'main';

-- Vérifier que main n'a PAS été modifié
SELECT * FROM iceberg.iceberg_warehouse.customers WHERE customer_id IN (1, 999);

-- ── 6. Lister les branches ────────────────
SELECT * FROM iceberg.iceberg_warehouse."$branches";
