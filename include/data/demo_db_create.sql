-- Create a new database named demo
CREATE DATABASE demo;
-- Connect to the demo database
-- Note: The \connect command is specific to psql, the PostgreSQL command-line interface
\connect demo

-- Create a schema and a table in the demo database
CREATE SCHEMA raw;
CREATE TABLE raw.customer (data JSON NOT NULL);
CREATE TABLE raw.account (data JSON NOT NULL);
CREATE TABLE raw.transaction (data JSON NOT NULL);