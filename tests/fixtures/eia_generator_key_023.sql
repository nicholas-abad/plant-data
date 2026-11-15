-- public.eia_generator_key(), copied verbatim from power-generation-etl
-- schema/migrations/023_eia_generator_key.sql at commit 9f8f8d0041fd22cf08310bd1106902a4e9bd2db4.
-- src/eia_generator_info.eia_generator_key() mirrors it; tests/test_eia_generator_key_sql.py
-- runs both over every Form 860 ID and fails if this copy drifts from the ETL repository.
CREATE OR REPLACE FUNCTION public.eia_generator_key(plant_code TEXT, generator_id TEXT)
RETURNS TEXT
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN generator_id IS NULL THEN NULL
        -- Rockport (IN): Form 923 alternates MB1/MB2 with 1/2 month by month.
        WHEN plant_code = '6166' AND upper(replace(generator_id, ' ', '')) IN ('MB1', 'MB2')
            THEN right(upper(replace(generator_id, ' ', '')), 1)
        -- Form 860 lists "0001" and "001" at this plant as two turbines.
        WHEN plant_code = '56032' THEN upper(replace(generator_id, ' ', ''))
        ELSE coalesce(nullif(ltrim(upper(replace(generator_id, ' ', '')), '0'), ''), '0')
    END
$$;
