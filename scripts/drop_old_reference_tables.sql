-- Drop old reference tables after verifying plant_crosswalk is loaded
-- and the dashboard is working correctly.
--
-- Run: psql $DATABASE_URL -f scripts/drop_old_reference_tables.sql
--
-- Wrapped in a transaction with a proof-of-life check that plant_crosswalk
-- has rows before dropping anything; the whole transaction rolls back if
-- the check fails.
--
-- NOTE: extraction_metadata is NOT a deprecated reference table — it is
-- actively written by every ETL run (see etl/power-generation-etl). Do not
-- add it to this drop list.

BEGIN;

-- Proof-of-life: refuse to run if plant_crosswalk hasn't been populated.
DO $$
DECLARE
    cnt bigint;
BEGIN
    SELECT COUNT(*) INTO cnt FROM plant_crosswalk;
    IF cnt = 0 THEN
        RAISE EXCEPTION
            'plant_crosswalk is empty (0 rows) — refusing to drop old reference tables. '
            'Run scripts/bootstrap_neon_db.py first.';
    END IF;
END $$;

DROP TABLE IF EXISTS plant_coordinates_eia CASCADE;
DROP TABLE IF EXISTS plant_coordinates_entsoe CASCADE;
DROP TABLE IF EXISTS plant_coordinates_npp CASCADE;
DROP TABLE IF EXISTS gppd_global CASCADE;
DROP TABLE IF EXISTS gcpt_global CASCADE;
DROP TABLE IF EXISTS npp_gipt_crosswalk CASCADE;

COMMIT;
