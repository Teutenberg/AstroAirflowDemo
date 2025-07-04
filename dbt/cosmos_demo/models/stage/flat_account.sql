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
        -- Use macro for agnostic JSON extraction
        {{ extract_json_field('data', 'account_id', 'text') }} as account_id,
        {{ extract_json_field('data', 'customer_id', 'text') }} as customer_id,
        {{ extract_json_field('data', 'account_number', 'text') }} as account_number,
        {{ extract_json_field('data', 'account_type', 'text') }} as account_type,
        {{ extract_json_field('data', 'bank_name', 'text') }} as bank_name,
        {{ extract_json_field('data', 'branch', 'text') }} as branch,
        {{ extract_json_field('data', 'balance', 'numeric') }} as balance,
        {{ extract_json_field('data', 'currency', 'text') }} as currency,
        {{ extract_json_field('data', 'is_active', 'boolean') }} as is_active,
        {{ extract_json_field('data', 'opened_at', 'text') }} as opened_at,
        data as raw_json -- keep the raw JSON for schema evolution
    from account_json
)
select * from flattened
