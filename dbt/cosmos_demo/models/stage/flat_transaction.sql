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
        -- Use macro for agnostic JSON extraction
        {{ extract_json_field('data', 'transaction_id', 'text') }} as transaction_id,
        {{ extract_json_field('data', 'account_id', 'text') }} as account_id,
        {{ extract_json_field('data', 'transaction_type', 'text') }} as transaction_type,
        {{ extract_json_field('data', 'amount', 'numeric') }} as amount,
        {{ extract_json_field('data', 'currency', 'text') }} as currency,
        {{ extract_json_field('data', 'category', 'text') }} as category,
        {{ extract_json_field('data', 'merchant', 'text') }} as merchant,
        {{ extract_json_field('data', 'description', 'text') }} as description,
        {{ extract_json_field('data', 'transaction_date', 'text') }} as transaction_date,
        data as raw_json -- keep the raw JSON for schema evolution
    from transaction_json
)
select * from flattened
