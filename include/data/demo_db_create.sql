-- Create a new database named demo
CREATE DATABASE demo;
-- Connect to the demo database
-- Note: The \connect command is specific to psql, the PostgreSQL command-line interface
\connect demo
-- Create a schema and a table in the demo database
CREATE SCHEMA raw;

-- Create customer table in the raw schema and populate it with data from a JSONL file
CREATE TABLE raw.customer (data JSONB NOT NULL);
COPY raw.customer (data)
FROM '/docker-entrypoint-initdb.d/customers.jsonl';

-- Create account table in the raw schema and populate it with data from a JSONL file
CREATE TABLE raw.account (data JSONB NOT NULL);
COPY raw.account (data)
FROM '/docker-entrypoint-initdb.d/accounts.jsonl';

-- Create transaction table in the raw schema and populate it with data from a JSONL file
CREATE TABLE raw.transaction (data JSONB NOT NULL);
COPY raw.transaction (data)
FROM '/docker-entrypoint-initdb.d/transactions.jsonl';

