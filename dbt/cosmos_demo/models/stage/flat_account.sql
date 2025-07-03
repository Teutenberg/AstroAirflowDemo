-- ============================================================================
-- flat_account.sql
--
-- This model flattens JSON data from the raw.account table.
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

with account_json as (
    select
        data
    from {{ source('raw', 'account') }}
)
, flattened as (
    select
        -- Replace these with the actual JSON keys you want to extract
        data->>'account_id' as account_id,
        data->>'customer_id' as customer_id,
        data->>'account_number' as account_number,
        data->>'account_type' as account_type,
        data->>'bank_name' as bank_name,
        data->>'branch' as branch,
        (data->>'balance')::numeric as balance,
        data->>'currency' as currency,
        (data->>'is_active')::boolean as is_active,
        data->>'opened_at' as opened_at,
        data as raw_json -- keep the raw JSON for schema evolution
    from account_json
)
select * from flattened
