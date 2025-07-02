-- Create a new database named demo
CREATE DATABASE DEMO;
-- Connect to the demo database
-- Note: The \connect command is specific to psql, the PostgreSQL command-line interface
\connect DEMO
-- Create raw schema and a table in the demo database
CREATE SCHEMA RAW;
CREATE TABLE RAW.CUSTOMER (DATA JSONB NOT NULL);
CREATE TABLE RAW.ACCOUNT (DATA JSONB NOT NULL);
CREATE TABLE RAW.TRANSACTION (DATA JSONB NOT NULL);

CREATE SCHEMA STAGE;