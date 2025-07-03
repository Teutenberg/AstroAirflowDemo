-- ============================================================================ 
-- flat_transaction.sql
--
-- This model flattens JSON data from the raw.transaction table.
--
-- Schema Evolution Notes:
-- - In Postgres, you must specify the JSON keys to extract as columns.
-- - To allow for schema evolution, keep the full JSON as a column (see 'raw_json')
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

with transaction_json as (
    select
        data
    from {{ source('raw', 'transaction') }}
)
, flattened as (
    select
        -- Replace these with the actual JSON keys you want to extract
        data->>'transaction_id' as transaction_id,
        data->>'account_id' as account_id,
        data->>'transaction_type' as transaction_type,
        (data->>'amount')::numeric as amount,
        data->>'currency' as currency,
        data->>'category' as category,
        data->>'merchant' as merchant,
        data->>'description' as description,
        data->>'transaction_date' as transaction_date,
        data as raw_json -- keep the raw JSON for schema evolution
    from transaction_json
)
select * from flattened
