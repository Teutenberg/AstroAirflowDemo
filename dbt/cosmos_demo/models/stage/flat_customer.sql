-- ============================================================================
-- flat_customer.sql
--
-- This model flattens JSON data from the raw.customer table.
--
-- Schema Evolution Notes:
-- - In Postgres, you must specify the JSON keys to extract as columns.
-- - To allow for schema evolution, keep the full JSON as a column (see 'full_json')
--   and periodically update this model to extract new fields as needed.
--
-- Using Python Models for Schema Evolution (Snowflake):
-- - dbt Python models (supported on Snowflake) can dynamically infer and flatten
--   all keys in semi-structured data (e.g., VARIANT columns).
-- - Example: Use pandas.json_normalize() in a dbt Python model to automatically
--   expand all fields, so new fields are included without code changes.
-- - This approach is not available in dbt SQL models on Postgres.
--
-- See dbt docs: https://docs.getdbt.com/docs/build/python-models
-- ============================================================================
{{ config(schema='stage', materialized='view') }}

with customer_json as (
    select
        data
    from {{ source('raw', 'customer') }}
)
, flattened as (
    select
        -- Replace these with the actual JSON keys you want to extract
        data->>'customer_id' as customer_id,
        data->>'first_name' as first_name,
        data->>'last_name' as last_name,
        data->>'email' as email,
        data->>'address' as address,
        data->>'phone_number' as phone_number,
        data->>'date_of_birth' as date_of_birth,
        data->>'created_at' as created_at,
        data as raw_json -- keep the raw JSON for schema evolution
    from customer_json
)
select * from flattened
