-- Drop old reference tables after verifying plant_crosswalk is loaded
-- and the dashboard is working correctly.
--
-- Run: psql $DATABASE_URL -f scripts/drop_old_reference_tables.sql

DROP TABLE IF EXISTS plant_coordinates_eia CASCADE;
DROP TABLE IF EXISTS plant_coordinates_entsoe CASCADE;
DROP TABLE IF EXISTS plant_coordinates_npp CASCADE;
DROP TABLE IF EXISTS gppd_global CASCADE;
DROP TABLE IF EXISTS gcpt_global CASCADE;
DROP TABLE IF EXISTS npp_gipt_crosswalk CASCADE;
DROP TABLE IF EXISTS extraction_metadata CASCADE;
